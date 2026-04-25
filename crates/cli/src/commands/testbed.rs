// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs,
    io::{BufWriter, Write as _},
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use dag::{
    authority::Authority,
    config::ImportExport,
    context::TokioCtx,
    metrics::{LATENCY_S, Metrics, MetricsSnapshot, RunKind, RunResult},
};
use eyre::{Context, Result, eyre};
use futures::future;
use replica::{
    builder::ReplicaBuilder,
    config::{LoadGeneratorConfig, PrivateReplicaConfig, PublicReplicaConfig, ReplicaParameters},
    prometheus::{MetricsRegistry, PrometheusServer},
    replica::ReplicaHandle,
};
use serde::Serialize;
use tokio::signal;
use tracing_subscriber::filter::LevelFilter;

use crate::{
    exporter::Exporter,
    terminal::{BannerPrinter, table},
    tracing::ReplicaTracing,
};

/// Configuration of one local-testbed run, persisted to `<output_dir>/config.yaml`
/// alongside the rest of the exporter artefacts.
#[derive(Clone, Serialize)]
pub struct TestbedConfig {
    pub committee_size: usize,
    pub mode: Mode,
    pub heartbeat_interval_secs: u64,
    pub transaction_size_bytes: usize,
    pub load_tx_per_sec: usize,
}

#[derive(Clone, Copy, Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Mode {
    Duration { secs: u64 },
    Perpetual,
}

#[allow(clippy::too_many_arguments)]
pub async fn local_testbed(
    committee_size: usize,
    replica_parameters_path: Option<PathBuf>,
    load_generator_config_path: Option<PathBuf>,
    duration: u64,
    perpetual: bool,
    heartbeat_interval: u64,
    output_dir: PathBuf,
    export_dag: bool,
    log_level: Option<LevelFilter>,
    log_file: Option<PathBuf>,
) -> Result<()> {
    // Wipe + recreate the output dir so each run starts from a clean state (WAL files,
    // tracing log, exporter artefacts). Equivalent to the prior `local-testbed` reset.
    if output_dir.exists() {
        fs::remove_dir_all(&output_dir)
            .wrap_err_with(|| format!("Failed to remove '{}'", output_dir.display()))?;
    }
    let exporter = Exporter::new(output_dir.clone())?;

    // Tracing-log destination, by precedence: --log-file wins (explicit override), else
    // exporter's tracing.log path. Default level is DEBUG so file logs stay informative;
    // stderr stays free for banner / heartbeat / summary.
    let log_path = log_file.or_else(|| Some(exporter.tracing_log_path()));
    let _guard = ReplicaTracing::new(log_level.unwrap_or(LevelFilter::DEBUG))
        .with_log_file(log_path)
        .setup()?;

    // Load optional parameter overrides; fall back to defaults.
    let replica_parameters = match replica_parameters_path {
        Some(path) => {
            tracing::info!("Loading replica parameters from {}", path.display());
            ReplicaParameters::load(&path)?
        }
        None => ReplicaParameters::default(),
    };
    let load_generator_config = match load_generator_config_path {
        Some(path) => {
            tracing::info!("Loading load generator config from {}", path.display());
            LoadGeneratorConfig::load(&path)?
        }
        None => LoadGeneratorConfig::default(),
    };

    let mode = if perpetual {
        Mode::Perpetual
    } else {
        Mode::Duration { secs: duration }
    };

    // Print the startup banner.
    let nodes = committee_size.to_string();
    let tx_size = load_generator_config.transaction_size.to_string();
    let load_str = load_generator_config.load.to_string();
    let mode_str = match mode {
        Mode::Duration { secs } => format!("Duration {secs}s"),
        Mode::Perpetual => "Perpetual".to_string(),
    };
    BannerPrinter::new(
        "Mysticeti",
        &[
            ("Mode", "Local Testbed"),
            ("Run", &mode_str),
            ("Nodes", &nodes),
            ("Tx size", &tx_size),
            ("Load", &load_str),
        ],
    )
    .print();

    let testbed_config = TestbedConfig {
        committee_size,
        mode,
        heartbeat_interval_secs: heartbeat_interval,
        transaction_size_bytes: load_generator_config.transaction_size,
        load_tx_per_sec: load_generator_config.load,
    };

    // Generate a localhost public config with the chosen parameters.
    let ips = vec![IpAddr::V4(Ipv4Addr::LOCALHOST); committee_size];
    let public_config =
        PublicReplicaConfig::new_for_benchmarks(ips).with_parameters(replica_parameters);

    let private_configs = PrivateReplicaConfig::new_for_benchmarks(&output_dir, committee_size);
    for private_config in &private_configs {
        fs::create_dir_all(&private_config.storage_path).wrap_err_with(|| {
            format!(
                "Failed to create directory '{}'",
                private_config.storage_path.display()
            )
        })?;
    }

    tracing::info!("Starting local testbed with {committee_size} replicas");

    // Spin up each replica: own its `Metrics` so we can poll it live for the heartbeat,
    // expose the registry over Prometheus, and start the load generator.
    let mut handles: Vec<ReplicaHandle<TokioCtx>> = Vec::with_capacity(committee_size);
    let mut metrics_servers = Vec::with_capacity(committee_size);
    let mut load_generators = Vec::with_capacity(committee_size);
    let mut metrics_per_replica: Vec<Arc<Metrics>> = Vec::with_capacity(committee_size);
    for (i, private_config) in private_configs.into_iter().enumerate() {
        let authority = Authority::from(i);
        let metrics_address = public_config
            .metrics_address(authority)
            .expect("metrics address must exist");
        let registry = MetricsRegistry::new();
        let metrics = Metrics::new(&registry, committee_size, None);
        metrics_per_replica.push(metrics.clone());
        let mut handle = ReplicaBuilder::new(authority, public_config.clone(), private_config)
            .with_registry(registry.clone())
            .with_metrics(metrics)
            .build()
            .run::<TokioCtx>()
            .await?;
        load_generators.push(handle.start_load_generator(load_generator_config.clone()));
        metrics_servers.push(
            PrometheusServer::new(metrics_address, &registry)
                .bind_all_interfaces()
                .start()
                .await?,
        );
        handles.push(handle);
    }

    let started_at = Instant::now();

    // Heartbeat task is perpetual-only: duration mode keeps stderr quiet between
    // banner and summary.
    let heartbeat_handle = match mode {
        Mode::Perpetual if heartbeat_interval > 0 => Some(spawn_heartbeat(
            metrics_per_replica.clone(),
            Duration::from_secs(heartbeat_interval),
            started_at,
        )),
        _ => None,
    };

    // Wait for the run to end.
    //
    // Duration mode: timer wins → graceful shutdown + summary; Ctrl-C wins → abort
    // with no summary (matching what users expect from a fixed-duration run).
    //
    // Perpetual mode: only Ctrl-C ends the run, and a summary is printed. A second
    // Ctrl-C while we're collecting bypasses the summary.
    let wait_outcome = match mode {
        Mode::Duration { secs } => tokio::select! {
            biased;
            _ = signal::ctrl_c() => WaitOutcome::AbortWithoutSummary,
            _ = tokio::time::sleep(Duration::from_secs(secs)) => WaitOutcome::CollectAndSummarize,
        },
        Mode::Perpetual => {
            tracing::info!(
                "Perpetual mode; press Ctrl-C to stop (heartbeat every {heartbeat_interval}s)."
            );
            signal::ctrl_c()
                .await
                .wrap_err("Failed to listen for Ctrl-C")?;
            WaitOutcome::CollectAndSummarize
        }
    };

    if let Some(handle) = heartbeat_handle {
        handle.abort();
    }

    if matches!(wait_outcome, WaitOutcome::AbortWithoutSummary) {
        tracing::info!("Aborting without summary.");
        return Ok(());
    }

    // Collection. In perpetual mode, run it inside a `select!` against a second
    // Ctrl-C: an impatient user can hit it twice to bail out before the WAL scan
    // completes.
    let elapsed = started_at.elapsed();
    eprintln!("\nCollecting results…");

    let collect_fut = collect_run_result(handles, testbed_config, elapsed, &exporter, export_dag);
    let result = match mode {
        Mode::Perpetual => tokio::select! {
            biased;
            _ = signal::ctrl_c() => {
                eprintln!("Second Ctrl-C; aborting collection.");
                return Ok(());
            }
            r = collect_fut => r?,
        },
        Mode::Duration { .. } => collect_fut.await?,
    };

    print_summary(&result);
    exporter.write_to(&result, 1, 1, None)?;
    Ok(())
}

enum WaitOutcome {
    CollectAndSummarize,
    AbortWithoutSummary,
}

/// Spawn a stderr heartbeat that prints aggregated stats every `interval`. Polled live
/// from the replicas' `Arc<Metrics>` — does not stop the replicas to read.
fn spawn_heartbeat(
    metrics_per_replica: Vec<Arc<Metrics>>,
    interval: Duration,
    started_at: Instant,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // First tick fires immediately; skip it so the first heartbeat lands at `interval`.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let elapsed = started_at.elapsed();
            let snapshots: Vec<MetricsSnapshot> =
                future::join_all(metrics_per_replica.iter().map(|m| m.collect())).await;
            let line = format_heartbeat(elapsed, &snapshots);
            eprintln!("{line}");
        }
    })
}

/// Average a per-replica value, ignoring replicas where the extractor returns `None`.
fn mean_per_replica<F>(snapshots: &[MetricsSnapshot], extract: F) -> Option<f64>
where
    F: Fn(&MetricsSnapshot) -> Option<f64>,
{
    let values: Vec<f64> = snapshots.iter().filter_map(extract).collect();
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

fn format_heartbeat(elapsed: Duration, snapshots: &[MetricsSnapshot]) -> String {
    let elapsed_secs = elapsed.as_secs();
    let max_committed = snapshots
        .iter()
        .map(|s| s.total_committed_leaders())
        .max()
        .unwrap_or(0);
    let mut parts = vec![
        format!("[t={elapsed_secs}s]"),
        format!("committed={max_committed}"),
    ];
    let elapsed_secs_f = elapsed.as_secs_f64();
    if elapsed_secs_f > 0.0 && !snapshots.is_empty() {
        let mean_committed = snapshots
            .iter()
            .map(|s| s.total_committed_leaders())
            .sum::<u64>() as f64
            / snapshots.len() as f64;
        if mean_committed > 0.0 {
            parts.push(format!("{:.1} commits/s", mean_committed / elapsed_secs_f));
        }
        let tx_counts: Vec<u64> = snapshots
            .iter()
            .filter_map(|s| s.histogram_sum_and_count(LATENCY_S))
            .map(|(_, count)| count)
            .collect();
        if !tx_counts.is_empty() {
            let mean_tx = tx_counts.iter().sum::<u64>() as f64 / tx_counts.len() as f64;
            if mean_tx > 0.0 {
                parts.push(format!("{:.0} tx/s", mean_tx / elapsed_secs_f));
            }
        }
    }
    let p50 = mean_per_replica(snapshots, |s| s.latency_percentile_ms(0.5));
    let p90 = mean_per_replica(snapshots, |s| s.latency_percentile_ms(0.9));
    if let (Some(p50), Some(p90)) = (p50, p90) {
        parts.push(format!("p50 {p50:.0} ms · p90 {p90:.0} ms"));
    }
    parts.join(" · ")
}

async fn collect_run_result(
    handles: Vec<ReplicaHandle<TokioCtx>>,
    config: TestbedConfig,
    duration: Duration,
    exporter: &Exporter,
    export_dag: bool,
) -> Result<RunResult<TestbedConfig>> {
    // Shut every replica down sequentially (the syncers borrow each other's storage during
    // the WAL scan, so we just need the borrows to be live, not concurrent).
    let mut syncers = Vec::with_capacity(handles.len());
    for handle in handles {
        syncers.push(handle.shutdown().await);
    }
    // `metrics.collect()` is async (it round-trips through the precise reporter to
    // get fresh percentiles), so collect snapshots concurrently per replica. The
    // storage borrows go in the same order; assemble the parallel arrays separately.
    let snapshots: Vec<_> =
        future::join_all(syncers.iter().map(|syncer| syncer.core().metrics.collect())).await;
    let storages: Vec<&_> = syncers
        .iter()
        .map(|syncer| syncer.core().storage())
        .collect();

    let mut dag_writer = if export_dag {
        let path = exporter.dag_path(1, 1, None)?;
        let file = fs::File::create(&path)
            .wrap_err_with(|| format!("creating DAG log {}", path.display()))?;
        Some(BufWriter::new(file))
    } else {
        None
    };

    let mut builder = RunResult::builder(snapshots, &storages, config, duration, RunKind::Testbed);
    if let Some(writer) = dag_writer.as_mut() {
        builder = builder.with_dag_log(writer);
    }
    let result = builder.collect().map_err(|e| eyre!(e))?;

    // Flush the DAG log before dropping the writer so the file is complete on disk.
    if let Some(mut writer) = dag_writer {
        writer.flush().wrap_err("flushing DAG log")?;
    }
    Ok(result)
}

fn print_summary(result: &RunResult<TestbedConfig>) {
    use dag::metrics::Outcome;
    let badge = match result.outcome {
        Outcome::Pass => "PASS: Commits consistent across all replicas",
        Outcome::NoProgress => "WARN: Safe but no leader was committed",
        Outcome::Diverged => "FAIL: Commits DIVERGED across replicas",
    };
    println!("\n{badge}");
    println!("{}", table::render(table::ReplicaRow::for_result(result)));
}
