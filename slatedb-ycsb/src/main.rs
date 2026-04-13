#![allow(clippy::result_large_err)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use clap::Parser;
use object_store::path::Path;
use slatedb::admin;
use slatedb::config::Settings;
use slatedb::Db;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

use crate::args::{PhaseArgs, YcsbArgs, YcsbCommands};
use crate::metrics::Metrics;
use crate::properties::{parse_properties_file, Workload};
use crate::workload::{run_load_phase, run_run_phase};

mod args;
mod db_runner;
mod keygen;
mod metrics;
mod properties;
mod record;
mod workload;

const REPORT_INTERVAL: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_span_events(FmtSpan::NONE)
        .init();

    let args = YcsbArgs::parse();
    let path = Path::from(args.path.as_str());
    let object_store = admin::load_object_store_from_env(args.env_file.clone())
        .map_err(|e| anyhow::anyhow!("failed to load object store: {e}"))?;

    let settings = if let Some(path) = &args.db_options_path {
        Settings::from_file(path).context("failed to load SlateDB Settings file")?
    } else {
        Settings::load().context("failed to load default SlateDB Settings")?
    };

    let db = Arc::new(
        Db::builder(path.clone(), object_store.clone())
            .with_settings(settings)
            .build()
            .await
            .context("failed to open SlateDB")?,
    );

    let result = match args.command.clone() {
        YcsbCommands::Load(phase) => exec_load(db.clone(), phase).await,
        YcsbCommands::Run(phase) => exec_run(db.clone(), phase).await,
        YcsbCommands::LoadRun(phase) => {
            exec_load(db.clone(), phase.clone()).await?;
            exec_run(db.clone(), phase).await
        }
    };

    db.close().await.context("failed to close SlateDB")?;
    result
}

fn load_workload(phase: &PhaseArgs) -> Result<Workload> {
    let mut props = parse_properties_file(&phase.properties)?;
    for (k, v) in &phase.prop_overrides {
        props.insert(k.clone(), v.clone());
    }
    let mut w = Workload::from_properties(&props)?;
    if let Some(tc) = phase.threadcount {
        w.thread_count = tc;
    }
    Ok(w)
}

async fn exec_load(db: Arc<Db>, phase: PhaseArgs) -> Result<()> {
    let workload = load_workload(&phase)?;
    let metrics = Metrics::new();
    let reporter = metrics.spawn_reporter(REPORT_INTERVAL);
    let started = Instant::now();
    let res = run_load_phase(
        db,
        &workload,
        metrics.clone(),
        workload.thread_count,
        phase.await_durable,
        phase.seed,
    )
    .await;
    reporter.abort();
    metrics.print_final_summary(started.elapsed());
    res
}

async fn exec_run(db: Arc<Db>, phase: PhaseArgs) -> Result<()> {
    let workload = load_workload(&phase)?;
    let metrics = Metrics::new();
    let reporter = metrics.spawn_reporter(REPORT_INTERVAL);
    let started = Instant::now();
    // Treat the full record range as already inserted so workloads that only
    // run the `run` phase against a pre-loaded DB (like in `load` + `run` split
    // mode) can issue reads against the whole range immediately.
    let initial_ack = workload.insert_start + workload.record_count;
    let res = run_run_phase(
        db,
        &workload,
        metrics.clone(),
        workload.thread_count,
        phase.duration.map(Duration::from_secs),
        phase.await_durable,
        phase.seed,
        initial_ack,
    )
    .await;
    reporter.abort();
    metrics.print_final_summary(started.elapsed());
    res
}
