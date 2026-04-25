// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fs::File, io::BufWriter, path::PathBuf};

use dag::{config::ImportExport, metrics::Outcome};
use eyre::{Context, Result, bail};
use simulator::{SimulationConfig, SimulationMode, SimulationRunner, SimulatorTracing};
use tracing_subscriber::filter::LevelFilter;

use crate::{
    args::SimulateArgs,
    exporter::Exporter,
    terminal::{BannerPrinter, Terminal},
};

pub async fn simulate(
    args: SimulateArgs,
    log_level: Option<LevelFilter>,
    log_file: Option<PathBuf>,
) -> Result<()> {
    let SimulateArgs {
        config_path,
        dump_config,
        output_dir,
        export_dag,
    } = args;

    // Print default config to stdout and exit.
    if dump_config {
        println!("{}", SimulationConfig::default().to_yaml());
        return Ok(());
    }

    // Build the exporter up-front so output-dir creation, tracing-log path, and per-run
    // subdir lifecycles all flow through one owner.
    let exporter = output_dir.map(Exporter::new).transpose()?;

    // Tracing-log destination, by precedence: --output-dir wins, --log-file is the
    // fallback, otherwise stderr (kept clean by the default warn-only filter).
    let log_path = exporter
        .as_ref()
        .map(Exporter::tracing_log_path)
        .or(log_file);
    let mut tracing = SimulatorTracing::new().with_log_file(log_path);
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
    let mut diverged = 0;

    for (index, config) in configs.into_iter().enumerate() {
        terminal.start_run(index + 1, &config);

        let name = config.name.clone();
        let mut runner = SimulationRunner::new(config);

        if export_dag && let Some(exporter) = &exporter {
            let path = exporter.dag_path(index + 1, total, name.as_deref())?;
            let file = File::create(&path)
                .wrap_err_with(|| format!("creating DAG log {}", path.display()))?;
            runner = runner.with_dag_writer(BufWriter::new(file));
        }

        let result = tokio::task::spawn_blocking(move || runner.run())
            .await
            .map_err(|error| eyre::eyre!("Simulation task panicked: {error}"))??;

        terminal.stop_run(&result);

        if result.outcome == Outcome::Diverged {
            diverged += 1;
        }
        if let Some(exporter) = &exporter {
            exporter.write_to(&result, index + 1, total, name.as_deref())?;
        }
    }

    terminal.finish();

    if diverged > 0 {
        bail!("{diverged} of {total} simulation(s) diverged");
    }
    Ok(())
}
