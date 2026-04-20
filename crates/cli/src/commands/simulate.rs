// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use dag::config::ImportExport;
use eyre::{Result, eyre};
use simulator::{SimulationConfig, SimulationRunner, SimulatorTracing};
use tracing_subscriber::filter::LevelFilter;

use crate::banner;

pub async fn simulate(
    config_path: Option<PathBuf>,
    dump_config: bool,
    log_level: Option<LevelFilter>,
) -> Result<()> {
    // Print default config to stdout and exit.
    if dump_config {
        println!("{}", SimulationConfig::default().to_yaml());
        return Ok(());
    }

    match log_level {
        Some(level) => SimulatorTracing::setup_with_filter(&level.to_string()),
        None => SimulatorTracing::setup(),
    };

    // Load simulation config or fall back to defaults.
    let config = match config_path {
        Some(path) => {
            tracing::info!("Loading simulation config from {}", path.display());
            SimulationConfig::load(&path)?
        }
        None => {
            tracing::info!("Using default simulation config");
            SimulationConfig::default()
        }
    };

    let nodes = config.committee_size.to_string();
    let duration = format!("{}s", config.duration_secs);
    banner::BannerPrinter::new(
        "Mysticeti",
        &[
            ("Mode", "Simulator"),
            ("Nodes", &nodes),
            ("Duration", &duration),
        ],
    )
    .print();

    // Run the simulation on a blocking thread (it uses simulated time, not tokio).
    let results = tokio::task::spawn_blocking(move || SimulationRunner::new(config).run())
        .await
        .map_err(|error| eyre!("Simulation task panicked: {error}"))?;

    // Report results.
    println!();
    if results.commits_consistent {
        println!("Commits consistent across all validators");
    } else {
        eprintln!("ERROR: Commits DIVERGED");
    }
    for (i, leaders) in results.committed_leaders.iter().enumerate() {
        println!("  Validator {i}: {} committed leaders", leaders.len());
    }

    Ok(())
}
