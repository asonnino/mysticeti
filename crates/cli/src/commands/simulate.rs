// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use dag::config::ImportExport;
use eyre::{Result, bail, eyre};
use simulator::{SimulationConfig, SimulationMode, SimulationRunner, SimulatorTracing};
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

    banner::BannerPrinter::new(
        "Mysticeti",
        &[
            ("Mode", "Simulator"),
            ("Simulations", &configs.len().to_string()),
        ],
    )
    .print();

    let total = configs.len();
    let mut diverged = 0;
    for (index, config) in configs.into_iter().enumerate() {
        if total > 1 {
            print_run_header(index + 1, total, &config);
        }
        let results = tokio::task::spawn_blocking(move || SimulationRunner::new(config).run())
            .await
            .map_err(|error| eyre!("Simulation task panicked: {error}"))?;

        println!();
        if results.commits_consistent {
            println!("Commits consistent across all validators");
        } else {
            eprintln!("ERROR: Commits DIVERGED");
            diverged += 1;
        }
        for (i, leaders) in results.committed_leaders.iter().enumerate() {
            println!("  Validator {i}: {} committed leaders", leaders.len());
        }
    }

    if total > 1 {
        let consistent = total - diverged;
        println!();
        println!("{total} simulations run, {consistent} consistent, {diverged} diverged");
    }

    if diverged > 0 {
        bail!("{diverged} of {total} simulation(s) diverged");
    }
    Ok(())
}

fn print_run_header(index: usize, total: usize, config: &SimulationConfig) {
    let label = config.name.as_deref().unwrap_or("unnamed");
    println!();
    println!(
        "──── [{index}/{total}] {label} — {} nodes, {}s ────",
        config.committee_size, config.duration_secs,
    );
}
