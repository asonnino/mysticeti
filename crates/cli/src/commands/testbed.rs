// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Glue between the `local-testbed` CLI subcommand and
//! [`replica::local_testbed::LocalTestbedRunner`]. Owns: argument parsing,
//! banner, exporter wiring, tracing setup, the duration / perpetual wait
//! loop, Ctrl-C handling, live progress line, and result rendering. The
//! runner owns: replica spawn loop, prometheus servers, load generators,
//! shutdown + `RunResult` collection.

use std::{
    fs::File,
    io::BufWriter,
    path::PathBuf,
    time::{Duration, Instant},
};

use dag::{config::ImportExport, metrics::Outcome};
use eyre::{Context, Result, bail};
use replica::{
    config::{LoadGeneratorConfig, ReplicaParameters},
    testbed::{LocalTestbedRunner, TestbedConfig},
};
use tokio::{signal, time};
use tracing_subscriber::filter::LevelFilter;

use crate::{
    args::LocalTestbedArgs,
    exporter::Exporter,
    terminal::{self, BannerPrinter, RunResultRender, stderr_supports_color},
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
    let _guard = match log_level {
        Some(level) => ReplicaTracing::new(level),
        None => ReplicaTracing::default(),
    }
    .with_log_file(log_path)
    .setup()?;

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
            LoadGeneratorConfig::default()
        }
    };

    let mode_str = if perpetual {
        "Perpetual".to_string()
    } else {
        format!("Duration {duration}s")
    };
    let nodes = committee_size.to_string();
    let tx_size = load_generator.transaction_size.to_string();
    let load_str = load_generator.load.to_string();
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
        replica_parameters,
        load_generator,
    };

    tracing::info!("Starting local testbed with {committee_size} replicas");

    let mut runner = LocalTestbedRunner::new(testbed_config);
    if export_dag && let Some(exporter) = &exporter {
        // (1, 1, None) = "run 1 of 1, unnamed": the per-run subdir collapses to
        // `output_dir` itself for single-run invocations.
        let path = exporter.dag_path(1, 1, None)?;
        let file =
            File::create(&path).wrap_err_with(|| format!("creating DAG log {}", path.display()))?;
        runner = runner.with_dag_writer(BufWriter::new(file));
    }

    let handle = runner.run().await?;
    let metrics = handle.metrics().to_vec();
    let started_at = Instant::now();

    // Drive the wait policy: duration (timer-driven) or perpetual (Ctrl-C-driven,
    // with a periodic progress line in between). Both branches end with
    // `handle.stop().await` to cancel + collect.
    if perpetual {
        tracing::info!(
            "Perpetual mode; press Ctrl-C to stop (heartbeat every {heartbeat_interval}s)."
        );
        let mut ticker = time::interval(Duration::from_secs(heartbeat_interval));
        // The first tick fires immediately; skip it so the first progress line lands
        // at `interval` instead of zero.
        ticker.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = signal::ctrl_c() => break,
                _ = ticker.tick() => {
                    let snapshots = metrics.iter().map(|m| m.collect()).collect::<Vec<_>>();
                    terminal::print_progress(started_at.elapsed(), &snapshots);
                }
            }
        }
    } else {
        // Duration mode: Ctrl-C aborts without a summary, matching what users expect
        // from a fixed-duration run. We return immediately without stopping the
        // handle — the runner task is killed when the tokio runtime drops at
        // process exit.
        tokio::select! {
            biased;
            _ = signal::ctrl_c() => {
                eprintln!("Ctrl-C received; aborting without summary.");
                return Ok(());
            }
            _ = time::sleep(Duration::from_secs(duration)) => {}
        }
    }

    // Collection. Perpetual mode lets the user bail out of a slow WAL scan with a
    // second Ctrl-C; duration-mode collection always runs to completion (the user
    // asked for a result).
    let result = if perpetual {
        tokio::select! {
            biased;
            _ = signal::ctrl_c() => {
                eprintln!("Second Ctrl-C; aborting collection.");
                return Ok(());
            }
            r = handle.stop() => r?,
        }
    } else {
        handle.stop().await?
    };

    println!();
    println!("{}", result.render(stderr_supports_color()));

    if let Some(exporter) = &exporter {
        exporter.write_to(&result, 1, 1, None)?;
    }
    if result.outcome == Outcome::Diverged {
        bail!("local testbed run diverged");
    }
    Ok(())
}
