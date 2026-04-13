use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use tracing::info;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum OpType {
    Read,
    Update,
    Insert,
    Scan,
    ReadModifyWrite,
}

impl OpType {
    pub(crate) fn label(&self) -> &'static str {
        match self {
            OpType::Read => "READ",
            OpType::Update => "UPDATE",
            OpType::Insert => "INSERT",
            OpType::Scan => "SCAN",
            OpType::ReadModifyWrite => "READ-MODIFY-WRITE",
        }
    }

    pub(crate) const ALL: [OpType; 5] = [
        OpType::Read,
        OpType::Update,
        OpType::Insert,
        OpType::Scan,
        OpType::ReadModifyWrite,
    ];
}

struct OpStats {
    /// Latencies in microseconds. Low: 1us, high: 60s, 3 sig figs.
    histogram: Mutex<Histogram<u64>>,
    ok_count: AtomicU64,
    err_count: AtomicU64,
}

impl OpStats {
    fn new() -> Self {
        Self {
            histogram: Mutex::new(
                Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("valid histogram"),
            ),
            ok_count: AtomicU64::new(0),
            err_count: AtomicU64::new(0),
        }
    }
}

/// Thread-safe shared collector. Clone is cheap (Arc).
#[derive(Clone)]
pub(crate) struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    read: OpStats,
    update: OpStats,
    insert: OpStats,
    scan: OpStats,
    rmw: OpStats,
    started_at: Instant,
}

impl Metrics {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                read: OpStats::new(),
                update: OpStats::new(),
                insert: OpStats::new(),
                scan: OpStats::new(),
                rmw: OpStats::new(),
                started_at: Instant::now(),
            }),
        }
    }

    pub(crate) fn record(&self, op: OpType, latency: Duration, ok: bool) {
        let stats = self.stats_for(op);
        if ok {
            stats.ok_count.fetch_add(1, Ordering::Relaxed);
        } else {
            stats.err_count.fetch_add(1, Ordering::Relaxed);
        }
        let us = latency.as_micros().min(60_000_000) as u64;
        let mut h = stats.histogram.lock().expect("histogram mutex poisoned");
        let _ = h.record(us.max(1));
    }

    fn stats_for(&self, op: OpType) -> &OpStats {
        match op {
            OpType::Read => &self.inner.read,
            OpType::Update => &self.inner.update,
            OpType::Insert => &self.inner.insert,
            OpType::Scan => &self.inner.scan,
            OpType::ReadModifyWrite => &self.inner.rmw,
        }
    }

    /// Total ok operations across all op types — used by the workload driver
    /// to detect when `operationcount` has been reached.
    pub(crate) fn total_ops(&self) -> u64 {
        OpType::ALL
            .iter()
            .map(|op| {
                let s = self.stats_for(*op);
                s.ok_count.load(Ordering::Relaxed) + s.err_count.load(Ordering::Relaxed)
            })
            .sum()
    }

    /// Print the YCSB-style final summary to stdout.
    pub(crate) fn print_final_summary(&self, runtime: Duration) {
        let total_ops = self.total_ops();
        println!("[OVERALL], RunTime(ms), {}", runtime.as_millis());
        let tput = if runtime.as_secs_f64() > 0.0 {
            total_ops as f64 / runtime.as_secs_f64()
        } else {
            0.0
        };
        println!("[OVERALL], Throughput(ops/sec), {tput:.2}");

        for op in OpType::ALL {
            let stats = self.stats_for(op);
            let ok = stats.ok_count.load(Ordering::Relaxed);
            let err = stats.err_count.load(Ordering::Relaxed);
            if ok == 0 && err == 0 {
                continue;
            }
            let label = op.label();
            let h = stats.histogram.lock().expect("histogram mutex poisoned");
            println!("[{label}], Operations, {ok}");
            println!("[{label}], Errors, {err}");
            println!("[{label}], AverageLatency(us), {:.2}", h.mean());
            println!("[{label}], MinLatency(us), {}", h.min());
            println!("[{label}], MaxLatency(us), {}", h.max());
            println!(
                "[{label}], 50thPercentileLatency(us), {}",
                h.value_at_quantile(0.50)
            );
            println!(
                "[{label}], 95thPercentileLatency(us), {}",
                h.value_at_quantile(0.95)
            );
            println!(
                "[{label}], 99thPercentileLatency(us), {}",
                h.value_at_quantile(0.99)
            );
            println!(
                "[{label}], 999thPercentileLatency(us), {}",
                h.value_at_quantile(0.999)
            );
        }
    }

    /// Spawn a background reporter that logs a one-line summary every `interval`.
    /// The returned handle can be aborted to stop the reporter.
    pub(crate) fn spawn_reporter(&self, interval: Duration) -> tokio::task::JoinHandle<()> {
        let metrics = self.clone();
        tokio::spawn(async move {
            let mut prev_total = 0u64;
            let mut prev_time = Instant::now();
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await; // initial immediate tick, skip
            loop {
                ticker.tick().await;
                let now = Instant::now();
                let total = metrics.total_ops();
                let delta = total.saturating_sub(prev_total);
                let dt = now.duration_since(prev_time).as_secs_f64().max(1e-9);
                let interval_tput = delta as f64 / dt;
                let elapsed = now.duration_since(metrics.inner.started_at);

                let mut line = String::new();
                write!(
                    &mut line,
                    "elapsed={}s ops={} interval_tput={:.0}/s",
                    elapsed.as_secs(),
                    total,
                    interval_tput
                )
                .ok();

                for op in OpType::ALL {
                    let stats = metrics.stats_for(op);
                    let ok = stats.ok_count.load(Ordering::Relaxed);
                    if ok == 0 {
                        continue;
                    }
                    let (p50, p99) = {
                        let h = stats.histogram.lock().expect("histogram mutex poisoned");
                        (h.value_at_quantile(0.50), h.value_at_quantile(0.99))
                    };
                    write!(
                        &mut line,
                        " [{}: n={} p50={}us p99={}us]",
                        op.label(),
                        ok,
                        p50,
                        p99,
                    )
                    .ok();
                }

                info!("{line}");
                prev_total = total;
                prev_time = now;
            }
        })
    }
}
