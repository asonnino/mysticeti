// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt, path::PathBuf};

use dag::config::ImportExport;
use eyre::{Result, bail};
use simulator::{SimulationConfig, SimulationMode, SimulationResults, SimulatorTracing};
use tracing_subscriber::filter::LevelFilter;

use crate::{
    report::{self, SimulationReport},
    reporter::{GREEN, RED, Reporter, YELLOW},
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
        let config_for_report = results_file.is_some().then(|| config.clone());
        let (outcome, suite_row, results) = reporter.run(config).await?;
        if outcome == Outcome::Diverged {
            diverged += 1;
        }
        suite_rows.push(suite_row);
        if let Some(config_snapshot) = config_for_report {
            reports.push(SimulationReport::new(config_snapshot, &results, outcome));
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

#[derive(Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Outcome {
    /// Safety held and at least one leader was committed somewhere.
    Pass,
    /// Safety held but no replica committed a single leader.
    /// Expected under unrecoverable partitions (star, symmetric split).
    NoProgress,
    /// Safety violated: commit histories disagree.
    Diverged,
}

impl From<&SimulationResults> for Outcome {
    fn from(results: &SimulationResults) -> Self {
        if !results.commits_consistent {
            return Self::Diverged;
        }
        if results.commit_counts().iter().all(|c| *c == 0) {
            return Self::NoProgress;
        }
        Self::Pass
    }
}

impl Outcome {
    pub fn glyph(&self) -> &'static str {
        match self {
            Self::Pass => "✓",
            Self::NoProgress => "⚠",
            Self::Diverged => "✗",
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            Self::Pass => "Commits consistent across all replicas",
            Self::NoProgress => "Safe but no leader was committed",
            Self::Diverged => "Commits DIVERGED across replicas",
        }
    }

    pub fn color(&self) -> &'static str {
        match self {
            Self::Pass => GREEN,
            Self::NoProgress => YELLOW,
            Self::Diverged => RED,
        }
    }

    fn label(&self) -> &'static str {
        match self {
            Self::Pass => "PASS",
            Self::NoProgress => "WARN",
            Self::Diverged => "FAIL",
        }
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.label(), self.message())
    }
}
