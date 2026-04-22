// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use dag::{config::ImportExport, metrics::Outcome};
use eyre::{Result, bail};
use simulator::{SimulationConfig, SimulationMode, SimulationRunner, SimulatorTracing};
use tracing_subscriber::filter::LevelFilter;

use crate::{
    report::{self, SimulationReport},
    terminal::{BannerPrinter, Terminal},
};

pub async fn simulate(
    config_path: Option<PathBuf>,
    dump_config: bool,
    results_file: Option<PathBuf>,
    log_level: Option<LevelFilter>,
    log_file: Option<PathBuf>,
) -> Result<()> {
    // Print default config to stdout and exit.
    if dump_config {
        println!("{}", SimulationConfig::default().to_yaml());
        return Ok(());
    }

    let mut tracing = SimulatorTracing::new().with_log_file(log_file);
    if let Some(level) = log_level {
        tracing = tracing.with_filter(level.to_string());
    }
    let _guard = tracing.setup()?;

    let configs = match config_path {
        Some(path) => {
            tracing::info!("Loading simulation config from {}", path.display());
            SimulationMode::load(&path)?.into_configs()
        }
        None => {
            tracing::info!("Using default simulation config");
            vec![SimulationConfig::default()]
        }
    };

    if configs.is_empty() {
        bail!("simulation suite is empty");
    }

    let total = configs.len();
    BannerPrinter::new(
        "Uncertified DAG",
        &[("Mode", "Simulator"), ("Simulations", &total.to_string())],
    )
    .print();

    let mut terminal = Terminal::new(total);
    let want_report = results_file.is_some();
    let mut reports: Vec<SimulationReport> = if want_report {
        Vec::with_capacity(total)
    } else {
        Vec::new()
    };
    let mut diverged = 0;

    for (index, config) in configs.into_iter().enumerate() {
        terminal.start_run(index + 1, &config);

        let result = tokio::task::spawn_blocking(move || SimulationRunner::new(config).run())
            .await
            .map_err(|error| eyre::eyre!("Simulation task panicked: {error}"))?;

        terminal.stop_run(&result);

        if result.outcome == Outcome::Diverged {
            diverged += 1;
        }
        if want_report {
            reports.push(SimulationReport::new(&result, result.outcome));
        }
    }

    terminal.finish();

    if let Some(path) = results_file {
        report::write_reports(&path, &reports)?;
        tracing::info!("Wrote detailed results to {}", path.display());
    }

    if diverged > 0 {
        bail!("{diverged} of {total} simulation(s) diverged");
    }
    Ok(())
}
