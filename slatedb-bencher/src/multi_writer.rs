//! # Multi-writer benchmarker
//!
//! This module benchmarks SlateDB under multiple concurrent writer instances.
//! SlateDB uses fencing to ensure only one writer is active at a time — each
//! new writer fences the previous one. This benchmark measures:
//!
//! - Aggregate write/read throughput across all writers
//! - Fencing frequency
//! - Operations potentially lost to fencing (fenced-before-flush)
//! - Reopen behavior after fencing
//!
//! ## Fenced-before-flush counter
//!
//! When `await_durable` is false, a successful put returns before the WAL is
//! flushed. If the writer is subsequently fenced, those unflushed ops are lost.
//! The "fenced-before-flush" counter tracks an upper bound on these lost ops:
//! it increments on each non-durable put and resets per writer session. When a
//! fence is detected, the counter value is recorded. With `await_durable: true`,
//! this counter is always 0.

use std::ops::Range;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use object_store::path::Path;
use object_store::ObjectStore;
use rand::{Rng, RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use slatedb::config::{PutOptions, Settings, WriteOptions};
use slatedb::db_cache::DbCache;
use slatedb::{CloseReason, Db, ErrorKind};
use tokio::time::Instant;
use tracing::{info, warn};

use crate::db::KeyGenerator;
use crate::stats::{StatsRecorder, WindowStats};

/// How frequently to dump stats to the console.
const STAT_DUMP_INTERVAL: Duration = Duration::from_secs(10);

/// How far back to look when dumping stats.
const STAT_DUMP_LOOKBACK: Duration = Duration::from_secs(60);

/// How frequently to update stats and check if we need to dump new stats.
const REPORT_INTERVAL: Duration = Duration::from_millis(100);

pub struct MultiWriterBench {
    path: Path,
    object_store: Arc<dyn ObjectStore>,
    settings: Settings,
    memory_cache: Option<Arc<dyn DbCache>>,
    key_gen_supplier: Box<dyn Fn() -> Box<dyn KeyGenerator>>,
    val_len: usize,
    write_options: WriteOptions,
    num_writers: u32,
    concurrency: u32,
    duration: Option<Duration>,
    put_percentage: u32,
    reopen_on_fence: bool,
    reopen_delay: Duration,
}

impl MultiWriterBench {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        path: Path,
        object_store: Arc<dyn ObjectStore>,
        settings: Settings,
        memory_cache: Option<Arc<dyn DbCache>>,
        key_gen_supplier: Box<dyn Fn() -> Box<dyn KeyGenerator>>,
        val_len: usize,
        write_options: WriteOptions,
        num_writers: u32,
        concurrency: u32,
        duration: Option<Duration>,
        put_percentage: u32,
        reopen_on_fence: bool,
        reopen_delay: Duration,
    ) -> Self {
        Self {
            path,
            object_store,
            settings,
            memory_cache,
            key_gen_supplier,
            val_len,
            write_options,
            num_writers,
            concurrency,
            duration,
            put_percentage,
            reopen_on_fence,
            reopen_delay,
        }
    }

    pub async fn run(&self) {
        let stats = Arc::new(MultiWriterStatsRecorder::new());
        let duration = self.duration.unwrap_or(Duration::MAX);
        let start = Instant::now();
        let mut writer_handles = Vec::new();

        for writer_id in 0..self.num_writers {
            let path = self.path.clone();
            let object_store = self.object_store.clone();
            let settings = self.settings.clone();
            let memory_cache = self.memory_cache.clone();
            let write_options = self.write_options.clone();
            let stats = stats.clone();
            let concurrency = self.concurrency;
            let put_percentage = self.put_percentage;
            let val_len = self.val_len;
            let reopen_on_fence = self.reopen_on_fence;
            let reopen_delay = self.reopen_delay;
            let await_durable = self.write_options.await_durable;

            // Pre-create key generators for all tasks of this writer
            let mut key_gens: Vec<Box<dyn KeyGenerator + Send>> = Vec::new();
            for _ in 0..concurrency {
                key_gens.push((self.key_gen_supplier)());
            }

            writer_handles.push(tokio::spawn(async move {
                writer_loop(
                    writer_id,
                    path,
                    object_store,
                    settings,
                    memory_cache,
                    write_options,
                    key_gens,
                    val_len,
                    concurrency,
                    put_percentage,
                    duration,
                    reopen_on_fence,
                    reopen_delay,
                    await_durable,
                    stats,
                )
                .await;
            }));
        }

        let stats_clone = stats.clone();
        tokio::spawn(async move { dump_stats(stats_clone).await });

        for handle in writer_handles {
            handle.await.unwrap();
        }

        let elapsed = start.elapsed();
        let secs = elapsed.as_secs_f64();
        let total_puts = stats.total_puts.load(Ordering::Relaxed);
        let total_gets = stats.total_gets.load(Ordering::Relaxed);
        let total_puts_bytes = stats.total_puts_bytes.load(Ordering::Relaxed);
        let total_gets_bytes = stats.total_gets_bytes.load(Ordering::Relaxed);
        let total_fences = stats.total_fences.load(Ordering::Relaxed);
        let total_fbf = stats.total_fenced_before_flush.load(Ordering::Relaxed);
        let effective_puts = total_puts.saturating_sub(total_fbf);
        let effective_puts_bytes =
            total_puts_bytes.saturating_sub(total_fbf * self.val_len as u64);

        info!(
            "multi-writer final [elapsed: {:.3}s, put/s: {:.3} ({:.3} MiB/s), effective put/s: {:.3} ({:.3} MiB/s), get/s: {:.3} ({:.3} MiB/s), total: puts={}, effective_puts={}, gets={}, fences={}, fenced-before-flush={}]",
            secs,
            total_puts as f64 / secs,
            total_puts_bytes as f64 / secs / 1_048_576.0,
            effective_puts as f64 / secs,
            effective_puts_bytes as f64 / secs / 1_048_576.0,
            total_gets as f64 / secs,
            total_gets_bytes as f64 / secs / 1_048_576.0,
            total_puts,
            effective_puts,
            total_gets,
            total_fences,
            total_fbf,
        );
    }
}

#[allow(clippy::too_many_arguments)]
async fn writer_loop(
    writer_id: u32,
    path: Path,
    object_store: Arc<dyn ObjectStore>,
    settings: Settings,
    memory_cache: Option<Arc<dyn DbCache>>,
    write_options: WriteOptions,
    mut key_gens: Vec<Box<dyn KeyGenerator + Send>>,
    val_len: usize,
    concurrency: u32,
    put_percentage: u32,
    duration: Duration,
    reopen_on_fence: bool,
    reopen_delay: Duration,
    await_durable: bool,
    stats: Arc<MultiWriterStatsRecorder>,
) {
    let start = Instant::now();

    loop {
        if start.elapsed() >= duration {
            break;
        }

        // Open a new Db instance
        let mut builder =
            Db::builder(path.clone(), object_store.clone()).with_settings(settings.clone());
        if let Some(ref cache) = memory_cache {
            builder = builder.with_memory_cache(cache.clone());
        }

        let db = match builder.build().await {
            Ok(db) => Arc::new(db),
            Err(e) => {
                warn!("writer {} failed to open db [error={}]", writer_id, e);
                tokio::time::sleep(reopen_delay).await;
                continue;
            }
        };

        info!("writer {} opened db", writer_id);

        // Track unflushed ops for this session (upper bound on ops lost to fencing)
        let session_unflushed = Arc::new(AtomicU64::new(0));
        let cancelled = Arc::new(AtomicBool::new(false));

        let remaining = duration.saturating_sub(start.elapsed());

        // Spawn concurrent tasks for this writer
        let mut task_handles = Vec::new();
        let mut remaining_key_gens = Vec::new();
        for _ in 0..concurrency {
            let key_gen = match key_gens.pop() {
                Some(kg) => kg,
                None => break,
            };

            let db = db.clone();
            let write_options = write_options.clone();
            let stats = stats.clone();
            let session_unflushed = session_unflushed.clone();
            let cancelled = cancelled.clone();

            task_handles.push(tokio::spawn(async move {
                writer_task(
                    key_gen,
                    val_len,
                    write_options,
                    put_percentage,
                    remaining,
                    await_durable,
                    db,
                    stats,
                    session_unflushed,
                    cancelled,
                )
                .await
            }));
        }

        // Wait for all tasks and collect results
        let mut any_fenced = false;
        for handle in task_handles {
            let (fenced, key_gen) = handle.await.unwrap();
            remaining_key_gens.push(key_gen);
            if fenced {
                any_fenced = true;
            }
        }

        // Restore key generators for next session
        key_gens = remaining_key_gens;

        // Best-effort close
        if let Err(e) = db.close().await {
            if !any_fenced {
                warn!("writer {} close failed [error={}]", writer_id, e);
            }
        }

        if any_fenced {
            let lost = session_unflushed.load(Ordering::Relaxed);
            stats.record_fence(Instant::now(), lost);
            info!(
                "writer {} was fenced [unflushed_ops={}]",
                writer_id, lost
            );

            if !reopen_on_fence {
                break;
            }
            tokio::time::sleep(reopen_delay).await;
        } else {
            break;
        }
    }
}

/// A single concurrent task within a writer instance.
/// Returns (was_fenced, key_generator) so the key generator can be reused.
async fn writer_task(
    mut key_gen: Box<dyn KeyGenerator + Send>,
    val_len: usize,
    write_options: WriteOptions,
    put_percentage: u32,
    duration: Duration,
    await_durable: bool,
    db: Arc<Db>,
    stats: Arc<MultiWriterStatsRecorder>,
    session_unflushed: Arc<AtomicU64>,
    cancelled: Arc<AtomicBool>,
) -> (bool, Box<dyn KeyGenerator + Send>) {
    let mut random = XorShiftRng::from_os_rng();
    let mut puts = 0u64;
    let mut puts_bytes = 0u64;
    let mut gets = 0u64;
    let mut gets_bytes = 0u64;
    let start = Instant::now();
    let mut last_report = start;

    let fenced = loop {
        if cancelled.load(Ordering::Relaxed) || start.elapsed() >= duration {
            break false;
        }

        if random.random_range(0..100) < put_percentage {
            let key = key_gen.next_key();
            let mut value = vec![0; val_len];
            random.fill_bytes(value.as_mut_slice());
            match db
                .put_with_options(key, value, &PutOptions::default(), &write_options)
                .await
            {
                Ok(_) => {
                    puts += 1;
                    puts_bytes += val_len as u64;
                    if !await_durable {
                        session_unflushed.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(e) if e.kind() == ErrorKind::Closed(CloseReason::Fenced) => {
                    let now = Instant::now();
                    stats.record_ops(now, puts, gets, puts_bytes, gets_bytes);
                    break true;
                }
                Err(e) => {
                    warn!("put failed [error={}]", e);
                }
            }
        } else {
            let key = key_gen.used_key();
            match db.get(&key).await {
                Ok(val) => {
                    gets += 1;
                    gets_bytes += key.len() as u64 + val.map(|v| v.len() as u64).unwrap_or(0);
                }
                Err(e) if e.kind() == ErrorKind::Closed(CloseReason::Fenced) => {
                    let now = Instant::now();
                    stats.record_ops(now, puts, gets, puts_bytes, gets_bytes);
                    break true;
                }
                Err(e) => {
                    warn!("get failed [error={}]", e);
                }
            }
        }

        if last_report.elapsed() >= REPORT_INTERVAL {
            last_report = Instant::now();
            stats.record_ops(last_report, puts, gets, puts_bytes, gets_bytes);
            puts = 0;
            gets = 0;
            puts_bytes = 0;
            gets_bytes = 0;
        }
    };

    // Flush any remaining stats
    if !fenced && (puts > 0 || gets > 0) {
        stats.record_ops(Instant::now(), puts, gets, puts_bytes, gets_bytes);
    }

    (fenced, key_gen)
}

// --- Stats ---

#[derive(Debug)]
struct MultiWriterWindow {
    range: Range<Instant>,
    puts: u64,
    gets: u64,
    puts_bytes: u64,
    gets_bytes: u64,
    fences: u64,
    fenced_before_flush: u64,
}

impl Default for MultiWriterWindow {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            range: now..now,
            puts: 0,
            gets: 0,
            puts_bytes: 0,
            gets_bytes: 0,
            fences: 0,
            fenced_before_flush: 0,
        }
    }
}

impl WindowStats for MultiWriterWindow {
    fn range(&self) -> Range<Instant> {
        self.range.clone()
    }

    fn set_range(&mut self, range: Range<Instant>) {
        self.range = range;
    }
}

struct MultiWriterStatsRecorder {
    recorder: StatsRecorder<MultiWriterWindow>,
    total_puts: AtomicU64,
    total_gets: AtomicU64,
    total_puts_bytes: AtomicU64,
    total_gets_bytes: AtomicU64,
    total_fences: AtomicU64,
    total_fenced_before_flush: AtomicU64,
}

impl MultiWriterStatsRecorder {
    fn new() -> Self {
        Self {
            recorder: StatsRecorder::new(),
            total_puts: AtomicU64::new(0),
            total_gets: AtomicU64::new(0),
            total_puts_bytes: AtomicU64::new(0),
            total_gets_bytes: AtomicU64::new(0),
            total_fences: AtomicU64::new(0),
            total_fenced_before_flush: AtomicU64::new(0),
        }
    }

    fn record_ops(&self, now: Instant, puts: u64, gets: u64, puts_bytes: u64, gets_bytes: u64) {
        self.total_puts.fetch_add(puts, Ordering::Relaxed);
        self.total_gets.fetch_add(gets, Ordering::Relaxed);
        self.total_puts_bytes.fetch_add(puts_bytes, Ordering::Relaxed);
        self.total_gets_bytes.fetch_add(gets_bytes, Ordering::Relaxed);

        self.recorder.record(now, |window| {
            window.puts += puts;
            window.gets += gets;
            window.puts_bytes += puts_bytes;
            window.gets_bytes += gets_bytes;
        });
    }

    fn record_fence(&self, now: Instant, unflushed_ops: u64) {
        self.total_fences.fetch_add(1, Ordering::Relaxed);
        self.total_fenced_before_flush
            .fetch_add(unflushed_ops, Ordering::Relaxed);

        self.recorder.record(now, |window| {
            window.fences += 1;
            window.fenced_before_flush += unflushed_ops;
        });
    }

    fn stats_since(
        &self,
        lookback: Duration,
    ) -> Option<(Range<Instant>, u64, u64, u64, u64, u64, u64)> {
        self.recorder.stats_since(lookback, |range, windows| {
            let puts: u64 = windows.iter().map(|w| w.puts).sum();
            let gets: u64 = windows.iter().map(|w| w.gets).sum();
            let puts_bytes: u64 = windows.iter().map(|w| w.puts_bytes).sum();
            let gets_bytes: u64 = windows.iter().map(|w| w.gets_bytes).sum();
            let fences: u64 = windows.iter().map(|w| w.fences).sum();
            let fenced_before_flush: u64 = windows.iter().map(|w| w.fenced_before_flush).sum();
            (range, puts, gets, puts_bytes, gets_bytes, fences, fenced_before_flush)
        })
    }
}

async fn dump_stats(stats: Arc<MultiWriterStatsRecorder>) {
    let mut last_stats_dump: Option<Instant> = None;
    let mut first_dump_start: Option<Instant> = None;
    loop {
        tokio::time::sleep(REPORT_INTERVAL).await;

        let stats_since = stats.stats_since(STAT_DUMP_LOOKBACK);
        if let Some((
            range,
            puts_since,
            gets_since,
            puts_bytes_since,
            gets_bytes_since,
            fences_since,
            fenced_before_flush_since,
        )) = stats_since
        {
            let interval = range.end - range.start;
            let should_print = match last_stats_dump {
                Some(last) => (range.end - last) >= STAT_DUMP_INTERVAL,
                None => interval >= STAT_DUMP_INTERVAL,
            };
            first_dump_start = first_dump_start.or(Some(range.start));
            if should_print {
                let secs = interval.as_secs() as f32;
                let put_rate = puts_since as f32 / secs;
                let put_bytes_rate = puts_bytes_since as f32 / secs;
                let get_rate = gets_since as f32 / secs;
                let get_bytes_rate = gets_bytes_since as f32 / secs;
                let fence_rate = fences_since as f32 / secs;

                let total_puts = stats.total_puts.load(Ordering::Relaxed);
                let total_gets = stats.total_gets.load(Ordering::Relaxed);
                let total_fences = stats.total_fences.load(Ordering::Relaxed);
                let total_fbf = stats.total_fenced_before_flush.load(Ordering::Relaxed);

                info!(
                    "multi-writer stats [elapsed {:?}, put/s: {:.3} ({:.3} MiB/s), get/s: {:.3} ({:.3} MiB/s), fence/s: {:.3}, fenced-before-flush (window): {}, window: {:?}, total: puts={}, gets={}, fences={}, fenced-before-flush={}]",
                    range.end.duration_since(first_dump_start.unwrap()).as_secs_f64(),
                    put_rate,
                    put_bytes_rate / 1_048_576.0,
                    get_rate,
                    get_bytes_rate / 1_048_576.0,
                    fence_rate,
                    fenced_before_flush_since,
                    range.end - range.start,
                    total_puts,
                    total_gets,
                    total_fences,
                    total_fbf,
                );
                last_stats_dump = Some(range.end);
            }
        }
    }
}
