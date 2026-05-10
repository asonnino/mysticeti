// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Glue between the `local-testbed` CLI subcommand and
//! [`replica::testbed::LocalTestbedRunner`]. Owns: argument parsing,
//! banner, exporter wiring, tracing setup, the duration / perpetual wait
//! loop, Ctrl-C handling, live progress line, and result rendering. The
//! runner owns: replica spawn loop, prometheus servers, load generators,
//! shutdown + `RunResult` collection.

use std::{
    future::{self, Future},
    path::PathBuf,
    pin::Pin,
    time::{Duration, Instant},
};

use dag::{config::ImportExport, metrics::SnapshotAggregate};
use eyre::{Result, bail};
use replica::result::Outcome;
use replica::{
    config::{LoadGeneratorConfig, ReplicaParameters},
    testbed::{LocalTestbedRunner, TestbedConfig},
};
use tokio::{signal, time};
use tracing_subscriber::filter::LevelFilter;

use crate::{
    args::LocalTestbedArgs,
    exporter::Exporter,
    terminal::{BannerPrinter, Terminal},
    tracing::ReplicaTracing,
};

pub async fn local_testbed(
    args: LocalTestbedArgs,
    log_level: Option<LevelFilter>,
    log_file: Option<PathBuf>,
) -> Result<()> {
    let LocalTestbedArgs {
        committee_size,
        replica_parameters_path,
        load_generator_config_path,
        duration,
        perpetual,
        heartbeat_interval,
        output_dir,
        export_dag,
    } = args;

    let exporter = output_dir.map(Exporter::new).transpose()?;

    let log_path = exporter
        .as_ref()
        .map(Exporter::tracing_log_path)
        .or(log_file);
    // When logs go to stderr, default to WARN so info chatter does not interleave
    // with the banner/heartbeat/summary; when going to a file, keep INFO.
    let default_level = if log_path.is_some() {
        LevelFilter::INFO
    } else {
        LevelFilter::WARN
    };
    let level = log_level.unwrap_or(default_level);
    let _guard = ReplicaTracing::new(level).with_log_file(log_path).setup()?;

    let replica_parameters = match replica_parameters_path {
        Some(path) => {
            tracing::info!("Loading replica parameters from {}", path.display());
            ReplicaParameters::load(&path)?
        }
        None => {
            tracing::info!("Using default replica parameters");
            ReplicaParameters::default()
        }
    };
    let load_generator = match load_generator_config_path {
        Some(path) => {
            tracing::info!("Loading load generator config from {}", path.display());
            LoadGeneratorConfig::load(&path)?
        }
        None => {
            tracing::info!("Using default load generator config");
            LoadGeneratorConfig::new_for_test()
        }
    };

    BannerPrinter::new(
        &replica_parameters.consensus,
        &[
            ("Mode", "Local Testbed"),
            ("Replicas", &committee_size.to_string()),
            (
                "Tx size",
                &format!("{} bytes", load_generator.transaction_size),
            ),
            (
                "Load",
                &format!("{} tx/s", load_generator.load * committee_size),
            ),
        ],
    )
    .print();

    let testbed_config = TestbedConfig {
        committee_size,
        replica_parameters,
        load_generator,
    };

    // Drive the wait policy: duration (timer-driven) or perpetual (Ctrl-C-driven,
    // with a periodic progress line in between).
    let mut deadline: Pin<Box<dyn Future<Output = ()> + Send>> = if perpetual {
        eprintln!("Perpetual mode; press Ctrl-C to stop (heartbeat every {heartbeat_interval}s).");
        eprintln!();
        Box::pin(future::pending())
    } else {
        eprintln!("Running for {} seconds…", duration);
        Box::pin(time::sleep(Duration::from_secs(duration)))
    };

    let mut terminal = Terminal::new(1);
    let run_duration = (!perpetual).then(|| Duration::from_secs(duration));
    terminal.print_config(1, &testbed_config);
    terminal.start_progress_animation(run_duration, "Running…");

    let runner = LocalTestbedRunner::new(testbed_config);
    let handle = runner.run().await?;
    let metrics = handle.metrics().to_vec();
    let started_at = Instant::now();
    tracing::info!("Starting local testbed with {committee_size} replicas");

    let mut heartbeat = time::interval(Duration::from_secs(heartbeat_interval));
    heartbeat.tick().await; // First tick completes immediately
    let mut progress = time::interval(Duration::from_secs(1));
    progress.tick().await; // First tick completes immediately
    loop {
        tokio::select! {
            biased;
            _ = signal::ctrl_c() => break,
            _ = deadline.as_mut() => break,
            _ = progress.tick() => terminal.print_status(started_at.elapsed(), None),
            _ = heartbeat.tick() => {
                let snapshots = metrics.iter().map(|m| m.collect()).collect::<Vec<_>>();
                let statistics = Some(&SnapshotAggregate::new(&snapshots));
                terminal.print_status(started_at.elapsed(), statistics);
            },
        }
    }

    // Results collection: stop the determinate bar cleanly before the prints
    // (otherwise eprintln fights for the line with the bar), then swap to a
    // spinner so the visual doesn't keep advancing past the run's deadline
    // while WAL scans drag on.
    terminal.stop_progress_animation();
    terminal.start_progress_animation(None, "Collecting results…");

    let result = tokio::select! {
        biased;
        _ = signal::ctrl_c() => {
            eprintln!(" Ctrl-C received during collection; aborting.");
            return Ok(());
        }
        r = handle.stop() => r?,
    };

    terminal.stop_progress_animation();
    eprintln!();
    terminal.print_results(&result);
    terminal.print_summary();

    if let Some(exporter) = &exporter {
        // (1, 1, None) = "run 1 of 1, unnamed": the per-run subdir collapses to
        // `output_dir` itself for single-run invocations.
        if export_dag {
            exporter.write_commit_log(&result.storages, 1, 1, None)?;
        }
        exporter.write_run_result(&result, 1, 1, None)?;
    }
    if result.outcome == Outcome::Diverged {
        bail!("local testbed run diverged");
    }
    Ok(())
}
