//! # BlobDB benchmarker
//!
//! Benchmarks BlobDB (key-value separation) against an unmodified SlateDB
//! baseline.  At the end of the run the module prints a single JSON line to
//! **stdout** with all result fields; tracing/logging goes to stderr.
//!
//! ## Primary metrics
//! - **Write amplification**: bytes written on the WAL/L0/compaction paths ÷
//!   bytes written by the application.
//! - Write/read throughput (ops/s and MiB/s)
//! - p50/p99 put and get latency (µs)
//!
//! ## Workload patterns
//! - `WriteHeavy`  — 95% puts, 5% gets
//! - `ReadHeavy`   — 5% puts, 95% gets
//! - `UpdateHeavy` — 50% puts (always overwrites existing keys), 50% gets
//! - `RangeScan`   — 5% puts, 95% range scans of `scan_width` entries each

use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hdrhistogram::Histogram;
use rand::{Rng, RngCore, SeedableRng};
use rand_xorshift::XorShiftRng;
use serde::Serialize;
use slatedb::compactor::stats::BYTES_COMPACTED;
use slatedb::config::{PutOptions, WriteOptions};
use slatedb::db_stats::{BLOB_FLUSH_BYTES, L0_FLUSH_BYTES, USER_WRITE_BYTES, WAL_FLUSH_BYTES};
use slatedb::Db;
use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue};
use tokio::time::Instant;
use tracing::{info, warn};

use crate::db::{KeyGenerator, RangeKeyGenerator};
use crate::stats::{StatsRecorder, WindowStats};

const STAT_DUMP_INTERVAL: Duration = Duration::from_secs(10);
const STAT_DUMP_LOOKBACK: Duration = Duration::from_secs(60);
const REPORT_INTERVAL: Duration = Duration::from_millis(100);

pub const DEFAULT_SCAN_WIDTH: usize = 10;

const MAX_LATENCY_US: u64 = 10_000_000;

// ─────────────────────────────────────────────────────────────────────────────
// Public enums (also used as clap args)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub enum WorkloadPattern {
    /// 100% puts walking `[0, key_count)` once, partitioned across tasks.
    /// Used to populate a fresh DB; untimed.
    Insert,
    /// 100% puts overwriting random existing keys; timed.
    Overwrite,
    /// 50% puts (overwrites) + 50% point gets — "point lookup/write mix".
    UpdateHeavy,
    /// 5% puts (overwrites) + 95% range scans — "range scan/write mix".
    RangeMix,
    /// 100% point gets.
    PointLookup,
    /// 100% range scans.
    RangeScan,
}

impl WorkloadPattern {
    fn put_percentage(&self) -> u32 {
        match self {
            WorkloadPattern::Insert => 100,
            WorkloadPattern::Overwrite => 100,
            WorkloadPattern::UpdateHeavy => 50,
            WorkloadPattern::RangeMix => 5,
            WorkloadPattern::PointLookup => 0,
            WorkloadPattern::RangeScan => 0,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            WorkloadPattern::Insert => "insert",
            WorkloadPattern::Overwrite => "overwrite",
            WorkloadPattern::UpdateHeavy => "update_heavy",
            WorkloadPattern::RangeMix => "range_mix",
            WorkloadPattern::PointLookup => "point_lookup",
            WorkloadPattern::RangeScan => "range_scan",
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Result
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct BlobDbResult {
    pub workload: String,
    pub val_size_bytes: usize,
    pub blob_enabled: bool,
    pub blob_threshold: usize,
    pub elapsed_secs: f64,
    pub total_puts: u64,
    pub total_gets: u64,
    pub total_scans: u64,
    pub total_puts_bytes: u64,
    pub user_write_bytes: u64,
    pub wal_flush_bytes: u64,
    pub l0_flush_bytes: u64,
    pub blob_flush_bytes: u64,
    pub bytes_compacted: u64,
    pub write_amp: f64,
    pub write_ops_per_sec: f64,
    pub write_mibps: f64,
    pub read_ops_per_sec: f64,
    pub read_mibps: f64,
    pub put_p50_us: u64,
    pub put_p99_us: u64,
    pub get_p50_us: u64,
    pub get_p99_us: u64,
}

// ─────────────────────────────────────────────────────────────────────────────
// BlobDbBench
// ─────────────────────────────────────────────────────────────────────────────

pub struct BlobDbBench {
    workload: WorkloadPattern,
    val_size_bytes: usize,
    write_options: WriteOptions,
    concurrency: u32,
    key_len: usize,
    key_count: u64,
    num_rows: Option<u64>,
    duration: Option<Duration>,
    scan_width: usize,
    kv_sep: bool,
    blob_threshold: usize,
    db: Arc<Db>,
    metrics_recorder: Arc<DefaultMetricsRecorder>,
}

impl BlobDbBench {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        workload: WorkloadPattern,
        val_size_bytes: usize,
        write_options: WriteOptions,
        concurrency: u32,
        key_len: usize,
        key_count: u64,
        num_rows: Option<u64>,
        duration: Option<Duration>,
        scan_width: usize,
        kv_sep: bool,
        blob_threshold: usize,
        db: Arc<Db>,
        metrics_recorder: Arc<DefaultMetricsRecorder>,
    ) -> Self {
        Self {
            workload,
            val_size_bytes,
            write_options,
            concurrency,
            key_len,
            key_count,
            num_rows,
            duration,
            scan_width,
            kv_sep,
            blob_threshold,
            db,
            metrics_recorder,
        }
    }

    /// Compute disjoint, contiguous chunks of `[0, key_count)` for the load
    /// phase. Each of `concurrency` tasks gets one chunk; the last absorbs the
    /// remainder so the union is exact.
    fn insert_chunks(&self) -> Vec<Range<u64>> {
        let concurrency = self.concurrency.max(1) as u64;
        let base = self.key_count / concurrency;
        let rem = self.key_count % concurrency;
        let mut chunks = Vec::with_capacity(concurrency as usize);
        let mut cursor: u64 = 0;
        for i in 0..concurrency {
            let extra = if i < rem { 1 } else { 0 };
            let end = cursor + base + extra;
            chunks.push(cursor..end);
            cursor = end;
        }
        chunks
    }

    /// Build a per-task key generator. Insert mode partitions the keyspace
    /// across tasks; all other workloads sample uniformly from `[0, key_count)`.
    fn build_generator(&self, task_idx: usize, chunks: &[Range<u64>]) -> Box<dyn KeyGenerator> {
        match self.workload {
            WorkloadPattern::Insert => {
                Box::new(RangeKeyGenerator::insert(self.key_len, chunks[task_idx].clone()))
            }
            _ => Box::new(RangeKeyGenerator::sample(self.key_len, self.key_count)),
        }
    }

    pub async fn run(&self) -> BlobDbResult {
        let stats_recorder = Arc::new(BlobDbStatsRecorder::new());
        let bench_start = Instant::now();

        let chunks = self.insert_chunks();
        let mut tasks = Vec::new();

        for task_idx in 0..self.concurrency as usize {
            let key_generator = self.build_generator(task_idx, &chunks);
            let insert_target = match self.workload {
                WorkloadPattern::Insert => Some(chunks[task_idx].end - chunks[task_idx].start),
                _ => None,
            };
            let mut task = BlobDbTask::new(
                key_generator,
                self.workload.clone(),
                self.val_size_bytes,
                self.write_options.clone(),
                self.num_rows,
                self.duration,
                self.scan_width,
                insert_target,
                stats_recorder.clone(),
                self.db.clone(),
            );
            tasks.push(tokio::spawn(async move { task.run().await }));
        }

        let stats_for_dump = stats_recorder.clone();
        tokio::spawn(async move { dump_stats(stats_for_dump).await });

        for task in tasks {
            task.await.unwrap();
        }

        // Drain everything still buffered (WAL queue, memtables, pending blob
        // packs) to object storage before sampling counters — otherwise the
        // physical-byte counters lag behind `user_write_bytes` and write_amp
        // comes out artificially low.
        if let Err(e) = self
            .db
            .flush_with_options(slatedb::config::FlushOptions {
                flush_type: slatedb::config::FlushType::MemTable,
            })
            .await
        {
            warn!("final flush before metrics read failed [error={}]", e);
        }

        let elapsed_secs = bench_start.elapsed().as_secs_f64();
        let user_write_bytes = read_counter(&self.metrics_recorder, USER_WRITE_BYTES);
        let wal_flush_bytes = read_counter(&self.metrics_recorder, WAL_FLUSH_BYTES);
        let l0_flush_bytes = read_counter(&self.metrics_recorder, L0_FLUSH_BYTES);
        let blob_flush_bytes = read_counter(&self.metrics_recorder, BLOB_FLUSH_BYTES);
        let bytes_compacted = read_counter(&self.metrics_recorder, BYTES_COMPACTED);
        // Total write amplification: all bytes written to object storage on the
        // write path (WAL flushes + memtable→L0 flushes + blob pack uploads +
        // compaction outputs) ÷ user-supplied key+value bytes accepted by the
        // DB. The Python orchestrator may recompute this from the raw byte
        // fields.
        let physical_write_bytes =
            wal_flush_bytes + l0_flush_bytes + blob_flush_bytes + bytes_compacted;
        let write_amp = if user_write_bytes > 0 {
            physical_write_bytes as f64 / user_write_bytes as f64
        } else {
            0.0
        };

        let total_puts = stats_recorder.puts();
        let total_gets = stats_recorder.gets();
        let total_scans = stats_recorder.scans();
        let total_puts_bytes = stats_recorder.total_puts_bytes();

        let put_p50_us;
        let put_p99_us;
        let get_p50_us;
        let get_p99_us;
        {
            let put_hist = stats_recorder.total_put_hist.lock().unwrap();
            let get_hist = stats_recorder.total_get_hist.lock().unwrap();
            put_p50_us = put_hist.value_at_quantile(0.5);
            put_p99_us = put_hist.value_at_quantile(0.99);
            get_p50_us = get_hist.value_at_quantile(0.5);
            get_p99_us = get_hist.value_at_quantile(0.99);
        }

        let write_ops_per_sec = total_puts as f64 / elapsed_secs;
        let write_mibps = total_puts_bytes as f64 / (elapsed_secs * 1024.0 * 1024.0);
        let read_ops = total_gets + total_scans;
        let read_ops_per_sec = read_ops as f64 / elapsed_secs;
        let total_gets_bytes = stats_recorder.total_gets_bytes();
        let read_mibps = total_gets_bytes as f64 / (elapsed_secs * 1024.0 * 1024.0);

        let result = BlobDbResult {
            workload: self.workload.as_str().to_string(),
            val_size_bytes: self.val_size_bytes,
            blob_enabled: self.kv_sep,
            blob_threshold: self.blob_threshold,
            elapsed_secs,
            total_puts,
            total_gets,
            total_scans,
            total_puts_bytes,
            user_write_bytes,
            wal_flush_bytes,
            l0_flush_bytes,
            blob_flush_bytes,
            bytes_compacted,
            write_amp,
            write_ops_per_sec,
            write_mibps,
            read_ops_per_sec,
            read_mibps,
            put_p50_us,
            put_p99_us,
            get_p50_us,
            get_p99_us,
        };

        info!(
            "blob-db final [workload={}, val_size_bytes={}, blob_threshold={}, elapsed={:.1}s, write_amp={:.3}x, puts={}, write_ops_per_sec={:.1}, write_mibps={:.3}, put_p50={}µs, put_p99={}µs, get_p50={}µs, get_p99={}µs]",
            result.workload,
            result.val_size_bytes,
            result.blob_threshold,
            result.elapsed_secs,
            result.write_amp,
            result.total_puts,
            result.write_ops_per_sec,
            result.write_mibps,
            result.put_p50_us,
            result.put_p99_us,
            result.get_p50_us,
            result.get_p99_us,
        );

        result
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BlobDbTask
// ─────────────────────────────────────────────────────────────────────────────

struct BlobDbTask {
    key_generator: Box<dyn KeyGenerator>,
    workload: WorkloadPattern,
    val_size_bytes: usize,
    write_options: WriteOptions,
    num_rows: Option<u64>,
    duration: Option<Duration>,
    scan_width: usize,
    /// If `Some`, the task terminates after this many puts, ignoring the
    /// global `num_rows` and `duration` caps. Used for the load phase, where
    /// each task owns a disjoint chunk and must write every key in it.
    insert_target: Option<u64>,
    stats_recorder: Arc<BlobDbStatsRecorder>,
    db: Arc<Db>,
}

impl BlobDbTask {
    #[allow(clippy::too_many_arguments)]
    fn new(
        key_generator: Box<dyn KeyGenerator>,
        workload: WorkloadPattern,
        val_size_bytes: usize,
        write_options: WriteOptions,
        num_rows: Option<u64>,
        duration: Option<Duration>,
        scan_width: usize,
        insert_target: Option<u64>,
        stats_recorder: Arc<BlobDbStatsRecorder>,
        db: Arc<Db>,
    ) -> Self {
        Self {
            key_generator,
            workload,
            val_size_bytes,
            write_options,
            num_rows,
            duration,
            scan_width,
            insert_target,
            stats_recorder,
            db,
        }
    }

    async fn run(&mut self) {
        let mut rng = XorShiftRng::from_os_rng();
        let put_pct = self.workload.put_percentage();
        let duration = self.duration.unwrap_or(Duration::MAX);
        let num_rows = self.num_rows.unwrap_or(u64::MAX);
        let start = Instant::now();
        let mut last_report = start;

        let mut puts = 0u64;
        let mut gets = 0u64;
        let mut scans = 0u64;
        let mut puts_bytes = 0u64;
        let mut gets_bytes = 0u64;
        let mut get_latencies_us: Vec<u64> = Vec::new();
        let mut put_latencies_us: Vec<u64> = Vec::new();

        // Cumulative per-task put count for insert-mode termination. The
        // `puts` counter above resets every flush, so we can't reuse it here.
        let mut task_puts_done: u64 = 0;
        let insert_target = self.insert_target;

        loop {
            let should_continue = if let Some(target) = insert_target {
                task_puts_done < target
            } else {
                self.stats_recorder.puts() < num_rows && start.elapsed() < duration
            };
            if !should_continue {
                break;
            }
            let op_roll: u32 = rng.random_range(0..100);

            if op_roll < put_pct {
                let key = match &self.workload {
                    WorkloadPattern::Insert => self.key_generator.next_key(),
                    _ => self.key_generator.used_key(),
                };
                let val_len = self.val_size_bytes;
                let mut value = vec![0u8; val_len];
                rng.fill_bytes(value.as_mut_slice());

                let op_start = Instant::now();
                match self
                    .db
                    .put_with_options(key, value, &PutOptions::default(), &self.write_options)
                    .await
                {
                    Ok(_) => {
                        let elapsed_us = op_start.elapsed().as_micros() as u64;
                        puts += 1;
                        task_puts_done += 1;
                        puts_bytes += val_len as u64;
                        put_latencies_us.push(elapsed_us);
                    }
                    Err(e) => warn!("put failed [error={}]", e),
                }
            } else if matches!(
                self.workload,
                WorkloadPattern::RangeMix | WorkloadPattern::RangeScan
            ) {
                let start_key = self.key_generator.used_key();
                let scan_width = self.scan_width;

                let op_start = Instant::now();
                match self.db.scan(start_key..).await {
                    Ok(mut iter) => {
                        let mut scanned = 0usize;
                        let mut scan_bytes = 0u64;
                        loop {
                            if scanned >= scan_width {
                                break;
                            }
                            match iter.next().await {
                                Ok(Some(kv)) => {
                                    scan_bytes +=
                                        kv.key.len() as u64 + kv.value.len() as u64;
                                    scanned += 1;
                                }
                                Ok(None) => break,
                                Err(e) => {
                                    warn!("scan next failed [error={}]", e);
                                    break;
                                }
                            }
                        }
                        let elapsed_us = op_start.elapsed().as_micros() as u64;
                        scans += 1;
                        gets_bytes += scan_bytes;
                        get_latencies_us.push(elapsed_us);
                    }
                    Err(e) => warn!("scan failed [error={}]", e),
                }
            } else {
                let key = self.key_generator.used_key();
                let op_start = Instant::now();
                match self.db.get(&key).await {
                    Ok(val) => {
                        let elapsed_us = op_start.elapsed().as_micros() as u64;
                        gets += 1;
                        gets_bytes +=
                            key.len() as u64 + val.map(|v| v.len() as u64).unwrap_or(0);
                        get_latencies_us.push(elapsed_us);
                    }
                    Err(e) => warn!("get failed [error={}]", e),
                }
            }

            if last_report.elapsed() >= REPORT_INTERVAL {
                last_report = Instant::now();
                self.stats_recorder.record(
                    last_report,
                    puts,
                    gets,
                    scans,
                    puts_bytes,
                    gets_bytes,
                    &get_latencies_us,
                    &put_latencies_us,
                );
                puts = 0;
                gets = 0;
                scans = 0;
                puts_bytes = 0;
                gets_bytes = 0;
                get_latencies_us.clear();
                put_latencies_us.clear();
            }
        }

        // Flush any remaining unreported counters so totals don't undercount
        // the final partial window when the loop exits.
        if puts > 0 || gets > 0 || scans > 0 {
            self.stats_recorder.record(
                Instant::now(),
                puts,
                gets,
                scans,
                puts_bytes,
                gets_bytes,
                &get_latencies_us,
                &put_latencies_us,
            );
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Stats window
// ─────────────────────────────────────────────────────────────────────────────

struct BlobDbWindow {
    range: Range<Instant>,
    puts: u64,
    gets: u64,
    scans: u64,
    puts_bytes: u64,
    gets_bytes: u64,
    get_latency_us: Histogram<u64>,
    put_latency_us: Histogram<u64>,
}

impl Default for BlobDbWindow {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            range: now..now,
            puts: 0,
            gets: 0,
            scans: 0,
            puts_bytes: 0,
            gets_bytes: 0,
            get_latency_us: Histogram::new_with_max(MAX_LATENCY_US, 3).expect("get hist"),
            put_latency_us: Histogram::new_with_max(MAX_LATENCY_US, 3).expect("put hist"),
        }
    }
}

impl WindowStats for BlobDbWindow {
    fn range(&self) -> Range<Instant> {
        self.range.clone()
    }

    fn set_range(&mut self, range: Range<Instant>) {
        self.range = range;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Stats recorder
// ─────────────────────────────────────────────────────────────────────────────

pub struct BlobDbStatsRecorder {
    recorder: StatsRecorder<BlobDbWindow>,
    total_puts: AtomicU64,
    total_gets: AtomicU64,
    total_scans: AtomicU64,
    total_puts_bytes: AtomicU64,
    total_gets_bytes: AtomicU64,
    // Global histograms accumulate across all windows for final P50/P99.
    pub total_put_hist: Mutex<Histogram<u64>>,
    pub total_get_hist: Mutex<Histogram<u64>>,
}

impl BlobDbStatsRecorder {
    pub fn new() -> Self {
        Self {
            recorder: StatsRecorder::new(),
            total_puts: AtomicU64::new(0),
            total_gets: AtomicU64::new(0),
            total_scans: AtomicU64::new(0),
            total_puts_bytes: AtomicU64::new(0),
            total_gets_bytes: AtomicU64::new(0),
            total_put_hist: Mutex::new(
                Histogram::new_with_max(MAX_LATENCY_US, 3).expect("global put hist"),
            ),
            total_get_hist: Mutex::new(
                Histogram::new_with_max(MAX_LATENCY_US, 3).expect("global get hist"),
            ),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record(
        &self,
        now: Instant,
        puts: u64,
        gets: u64,
        scans: u64,
        puts_bytes: u64,
        gets_bytes: u64,
        get_latencies_us: &[u64],
        put_latencies_us: &[u64],
    ) {
        self.total_puts.fetch_add(puts, Ordering::Relaxed);
        self.total_gets.fetch_add(gets, Ordering::Relaxed);
        self.total_scans.fetch_add(scans, Ordering::Relaxed);
        self.total_puts_bytes.fetch_add(puts_bytes, Ordering::Relaxed);
        self.total_gets_bytes.fetch_add(gets_bytes, Ordering::Relaxed);

        {
            let mut ph = self.total_put_hist.lock().unwrap();
            for &us in put_latencies_us {
                ph.record(us.min(MAX_LATENCY_US)).ok();
            }
        }
        {
            let mut gh = self.total_get_hist.lock().unwrap();
            for &us in get_latencies_us {
                gh.record(us.min(MAX_LATENCY_US)).ok();
            }
        }

        self.recorder.record(now, |w| {
            w.puts += puts;
            w.gets += gets;
            w.scans += scans;
            w.puts_bytes += puts_bytes;
            w.gets_bytes += gets_bytes;
            for &us in get_latencies_us {
                w.get_latency_us.record(us.min(MAX_LATENCY_US)).ok();
            }
            for &us in put_latencies_us {
                w.put_latency_us.record(us.min(MAX_LATENCY_US)).ok();
            }
        });
    }

    pub fn puts(&self) -> u64 {
        self.total_puts.load(Ordering::Relaxed)
    }

    pub fn gets(&self) -> u64 {
        self.total_gets.load(Ordering::Relaxed)
    }

    pub fn scans(&self) -> u64 {
        self.total_scans.load(Ordering::Relaxed)
    }

    pub fn total_puts_bytes(&self) -> u64 {
        self.total_puts_bytes.load(Ordering::Relaxed)
    }

    pub fn total_gets_bytes(&self) -> u64 {
        self.total_gets_bytes.load(Ordering::Relaxed)
    }

    fn stats_since(&self, lookback: Duration) -> Option<BlobDbPeriodStats> {
        self.recorder.stats_since(lookback, |range, windows| {
            let puts: u64 = windows.iter().map(|w| w.puts).sum();
            let gets: u64 = windows.iter().map(|w| w.gets).sum();
            let scans: u64 = windows.iter().map(|w| w.scans).sum();
            let puts_bytes: u64 = windows.iter().map(|w| w.puts_bytes).sum();
            let gets_bytes: u64 = windows.iter().map(|w| w.gets_bytes).sum();

            let mut get_hist =
                Histogram::<u64>::new_with_max(MAX_LATENCY_US, 3).expect("get histogram");
            let mut put_hist =
                Histogram::<u64>::new_with_max(MAX_LATENCY_US, 3).expect("put histogram");
            for w in &windows {
                get_hist.add(&w.get_latency_us).ok();
                put_hist.add(&w.put_latency_us).ok();
            }

            BlobDbPeriodStats {
                range,
                puts,
                gets,
                scans,
                puts_bytes,
                gets_bytes,
                get_p50_us: get_hist.value_at_quantile(0.5),
                get_p99_us: get_hist.value_at_quantile(0.99),
                put_p50_us: put_hist.value_at_quantile(0.5),
                put_p99_us: put_hist.value_at_quantile(0.99),
            }
        })
    }
}

struct BlobDbPeriodStats {
    range: Range<Instant>,
    puts: u64,
    gets: u64,
    scans: u64,
    puts_bytes: u64,
    gets_bytes: u64,
    get_p50_us: u64,
    get_p99_us: u64,
    put_p50_us: u64,
    put_p99_us: u64,
}

// ─────────────────────────────────────────────────────────────────────────────
// Stats dump loop
// ─────────────────────────────────────────────────────────────────────────────

async fn dump_stats(stats: Arc<BlobDbStatsRecorder>) {
    let mut last_stats_dump: Option<Instant> = None;
    let mut first_dump_start: Option<Instant> = None;
    loop {
        tokio::time::sleep(REPORT_INTERVAL).await;

        let Some(s) = stats.stats_since(STAT_DUMP_LOOKBACK) else {
            continue;
        };

        let interval = s.range.end - s.range.start;
        let should_print = match last_stats_dump {
            Some(last) => (s.range.end - last) >= STAT_DUMP_INTERVAL,
            None => interval >= STAT_DUMP_INTERVAL,
        };
        first_dump_start = first_dump_start.or(Some(s.range.start));

        if should_print {
            let secs = interval.as_secs_f32();
            let put_rate = s.puts as f32 / secs;
            let get_rate = (s.gets + s.scans) as f32 / secs;
            let put_mib = s.puts_bytes as f32 / (1024.0 * 1024.0 * secs);
            let get_mib = s.gets_bytes as f32 / (1024.0 * 1024.0 * secs);

            info!(
                "blob-db stats [elapsed={:.1}s, put/s={:.1} ({:.3} MiB/s), get/s={:.1} ({:.3} MiB/s), scan/s={:.1}, put_p50={}µs, put_p99={}µs, get_p50={}µs, get_p99={}µs, total_puts={}, total_gets={}, total_scans={}]",
                s.range.end.duration_since(first_dump_start.unwrap()).as_secs_f64(),
                put_rate,
                put_mib,
                get_rate,
                get_mib,
                s.scans as f32 / secs,
                s.put_p50_us,
                s.put_p99_us,
                s.get_p50_us,
                s.get_p99_us,
                stats.puts(),
                stats.gets(),
                stats.scans(),
            );
            last_stats_dump = Some(s.range.end);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Write amplification helper
// ─────────────────────────────────────────────────────────────────────────────

fn read_counter(recorder: &DefaultMetricsRecorder, name: &str) -> u64 {
    let snapshot = recorder.snapshot();
    snapshot
        .by_name(name)
        .into_iter()
        .filter_map(|m| match m.value {
            MetricValue::Counter(v) => Some(v),
            _ => None,
        })
        .sum()
}

