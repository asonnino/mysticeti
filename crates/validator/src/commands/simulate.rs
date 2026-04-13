// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use dag::config::ImportExport;
use eyre::{Context, Result};
use simulator::{SimulationConfig, SimulationRunner};

pub async fn simulate(config_path: Option<PathBuf>, dump_config: bool) -> Result<()> {
    if dump_config {
        let config = SimulationConfig::default();
        let yaml = serde_yaml::to_string(&config).map_err(eyre::Report::msg)?;
        println!("{yaml}");
        return Ok(());
    }

    let config = match config_path {
        Some(path) => SimulationConfig::load(&path).wrap_err("Failed to load simulation config")?,
        None => SimulationConfig::default(),
    };

    println!("Running simulation: {config:?}");
    let results = tokio::task::spawn_blocking(move || SimulationRunner::new(config).run())
        .await
        .expect("Simulation task panicked");

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
