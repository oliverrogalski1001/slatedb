use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use rand::{Rng, SeedableRng};
use rand_xoshiro::Xoshiro256PlusPlus;
use slatedb::config::WriteOptions;
use slatedb::Db;
use tracing::{error, info};

use crate::db_runner::DbRunner;
use crate::keygen::{build_key, make_chooser, AcknowledgedCounter, KeyChooser};
use crate::metrics::{Metrics, OpType};
use crate::properties::{Distribution, Workload};

/// Op chooser backed by cumulative thresholds over the five YCSB op types.
struct OpChooser {
    thresholds: [(f64, OpType); 5],
    total: f64,
}

impl OpChooser {
    fn new(w: &Workload) -> Self {
        let entries = [
            (w.read_proportion, OpType::Read),
            (w.update_proportion, OpType::Update),
            (w.insert_proportion, OpType::Insert),
            (w.scan_proportion, OpType::Scan),
            (w.read_modify_write_proportion, OpType::ReadModifyWrite),
        ];
        let total: f64 = entries.iter().map(|(p, _)| *p).sum();
        let mut cum = 0.0;
        let mut thresholds = [(0.0, OpType::Read); 5];
        for (i, (p, op)) in entries.iter().enumerate() {
            cum += *p;
            thresholds[i] = (cum, *op);
        }
        Self { thresholds, total }
    }

    fn next<R: Rng + ?Sized>(&self, rng: &mut R) -> OpType {
        let r: f64 = rng.random::<f64>() * self.total;
        for (t, op) in &self.thresholds {
            if r < *t {
                return *op;
            }
        }
        self.thresholds.last().unwrap().1
    }
}

/// Bulk-load `[insert_start, insert_start + record_count)` into the DB.
pub(crate) async fn run_load_phase(
    db: Arc<Db>,
    workload: &Workload,
    metrics: Metrics,
    thread_count: u32,
    await_durable: bool,
    seed: u64,
) -> Result<()> {
    let total = workload.record_count;
    let start_key = workload.insert_start;
    let end_key = start_key + total;
    let next_key = Arc::new(AtomicU64::new(start_key));
    let ack = Arc::new(AcknowledgedCounter::new(start_key));

    let runner = Arc::new(DbRunner {
        db: db.clone(),
        write_options: WriteOptions { await_durable },
        field_count: workload.field_count,
        field_length: workload.field_length,
        read_all_fields: workload.read_all_fields,
    });

    info!(
        "load phase starting: {} records, {} threads",
        total, thread_count
    );
    let start_wall = Instant::now();
    let mut handles = Vec::new();
    for tid in 0..thread_count {
        let runner = runner.clone();
        let next_key = next_key.clone();
        let ack = ack.clone();
        let metrics = metrics.clone();
        let insert_order = workload.insert_order;
        let zero_padding = workload.zero_padding;
        let thread_seed = seed.wrapping_add(tid as u64);
        handles.push(tokio::spawn(async move {
            let mut rng = Xoshiro256PlusPlus::seed_from_u64(thread_seed);
            loop {
                let keynum = next_key.fetch_add(1, Ordering::Relaxed);
                if keynum >= end_key {
                    return;
                }
                let key = build_key(keynum, insert_order, zero_padding);
                let t0 = Instant::now();
                let res = runner.insert(&mut rng, key).await;
                let dt = t0.elapsed();
                let ok = res.is_ok();
                if let Err(e) = &res {
                    error!("insert error keynum={keynum}: {e}");
                }
                metrics.record(OpType::Insert, dt, ok);
                if ok {
                    ack.acknowledge(keynum + 1);
                }
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    info!("load phase complete in {:?}", start_wall.elapsed());
    Ok(())
}

/// Execute the run phase: spawn `thread_count` workers and drive the workload
/// until `operation_count` ops have been performed (summed across threads) or
/// `duration` elapses, whichever comes first.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_run_phase(
    db: Arc<Db>,
    workload: &Workload,
    metrics: Metrics,
    thread_count: u32,
    duration: Option<Duration>,
    await_durable: bool,
    seed: u64,
    initial_ack: u64,
) -> Result<()> {
    let ack = Arc::new(AcknowledgedCounter::new(initial_ack));
    let next_insert = Arc::new(AtomicU64::new(initial_ack));
    let op_counter = Arc::new(AtomicU64::new(0));
    let runner = Arc::new(DbRunner {
        db: db.clone(),
        write_options: WriteOptions { await_durable },
        field_count: workload.field_count,
        field_length: workload.field_length,
        read_all_fields: workload.read_all_fields,
    });

    let op_chooser = Arc::new(OpChooser::new(workload));
    let deadline = duration.map(|d| Instant::now() + d);
    let target_ops = workload.operation_count;

    info!(
        "run phase starting: target_ops={} duration={:?} threads={}",
        target_ops, duration, thread_count
    );
    let start_wall = Instant::now();

    let mut handles = Vec::new();
    for tid in 0..thread_count {
        let runner = runner.clone();
        let metrics = metrics.clone();
        let op_chooser = op_chooser.clone();
        let ack = ack.clone();
        let next_insert = next_insert.clone();
        let op_counter = op_counter.clone();
        let workload = workload.clone();
        let thread_seed = seed.wrapping_add(tid as u64).wrapping_add(0xC0FFEE);
        handles.push(tokio::spawn(async move {
            let mut rng = Xoshiro256PlusPlus::seed_from_u64(thread_seed);
            let insert_start = workload.insert_start;
            let insert_end = workload.insert_start + workload.record_count;
            let mut chooser: Box<dyn KeyChooser> = make_chooser(
                workload.request_distribution,
                insert_start,
                insert_end,
                workload.zipfian_constant,
                workload.hotspot_data_fraction,
                workload.hotspot_opn_fraction,
                ack.clone(),
            );
            let scan_length_chooser = workload.scan_length_distribution;
            let max_scan_length = workload.max_scan_length.max(1);

            loop {
                if let Some(d) = deadline {
                    if Instant::now() >= d {
                        return;
                    }
                }
                let done = op_counter.fetch_add(1, Ordering::Relaxed);
                if done >= target_ops && deadline.is_none() {
                    return;
                }

                let op = op_chooser.next(&mut rng);
                let t0 = Instant::now();
                let res: Result<(), slatedb::Error> = match op {
                    OpType::Read => {
                        let keynum = chooser.next_key(&mut rng);
                        let key = build_key(keynum, workload.insert_order, workload.zero_padding);
                        runner.read(key).await
                    }
                    OpType::Update => {
                        let keynum = chooser.next_key(&mut rng);
                        let key = build_key(keynum, workload.insert_order, workload.zero_padding);
                        runner
                            .update(&mut rng, key, workload.write_all_fields)
                            .await
                    }
                    OpType::Insert => {
                        let keynum = next_insert.fetch_add(1, Ordering::Relaxed);
                        let key = build_key(keynum, workload.insert_order, workload.zero_padding);
                        let r = runner.insert(&mut rng, key).await;
                        if r.is_ok() {
                            ack.acknowledge(keynum + 1);
                        }
                        r
                    }
                    OpType::Scan => {
                        let keynum = chooser.next_key(&mut rng);
                        let start_key =
                            build_key(keynum, workload.insert_order, workload.zero_padding);
                        let limit =
                            sample_scan_length(&mut rng, scan_length_chooser, max_scan_length)
                                as usize;
                        runner.scan(start_key, limit).await
                    }
                    OpType::ReadModifyWrite => {
                        let keynum = chooser.next_key(&mut rng);
                        let key = build_key(keynum, workload.insert_order, workload.zero_padding);
                        runner.read_modify_write(&mut rng, key).await
                    }
                };
                let dt = t0.elapsed();
                let ok = res.is_ok();
                if let Err(e) = &res {
                    error!("op error: {e}");
                }
                metrics.record(op, dt, ok);
            }
        }));
    }
    for h in handles {
        let _ = h.await;
    }
    info!("run phase complete in {:?}", start_wall.elapsed());
    Ok(())
}

fn sample_scan_length<R: Rng>(rng: &mut R, dist: Distribution, max: u64) -> u64 {
    match dist {
        Distribution::Uniform => rng.random_range(1..=max),
        Distribution::Zipfian => {
            // Simple approximation: zipfian over [1, max] with theta=0.99. We don't
            // cache this per-workload because sampling scan length is rare enough that
            // a fresh generator per call is fine.
            let z = crate::keygen::Zipfian::new(1, max + 1, 0.99);
            z.next(rng).max(1)
        }
        _ => rng.random_range(1..=max),
    }
}
