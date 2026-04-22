// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use dag::{config::ImportExport, metrics::Outcome};
use eyre::{Result, bail};
use simulator::{SimulationConfig, SimulationMode, SimulatorTracing};
use tracing_subscriber::filter::LevelFilter;

use crate::{
    report::{self, SimulationReport},
    reporter::Reporter,
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

    let reporter = Reporter::new();
    reporter.banner(
        "Uncertified DAG",
        &[
            ("Mode", "Simulator"),
            ("Simulations", &configs.len().to_string()),
        ],
    );

    let total = configs.len();
    let mut suite_rows = Vec::with_capacity(total);
    let mut reports: Vec<SimulationReport> = if results_file.is_some() {
        Vec::with_capacity(total)
    } else {
        Vec::new()
    };
    let mut diverged = 0;
    for (index, config) in configs.into_iter().enumerate() {
        reporter.config_summary(index + 1, total, &config);
        let want_report = results_file.is_some();
        let (outcome, suite_row, result) = reporter.run(config).await?;
        if outcome == Outcome::Diverged {
            diverged += 1;
        }
        suite_rows.push(suite_row);
        if want_report {
            reports.push(SimulationReport::new(&result, outcome));
        }
    }

    if total > 1 {
        reporter.suite_summary(&suite_rows);
    }

    if let Some(path) = results_file {
        report::write_reports(&path, &reports)?;
        tracing::info!("Wrote detailed results to {}", path.display());
    }

    if diverged > 0 {
        bail!("{diverged} of {total} simulation(s) diverged");
    }
    Ok(())
}
