// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use clap::Parser;
use eyre::Result;
use simulator::SimulatorTracing;
use tracing_subscriber::filter::LevelFilter;
use validator::{
    cli::{Args, Operation},
    commands,
    tracing::ValidatorTracing,
};

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let args = Args::parse();

    match (&args.operation, args.log_level) {
        (Operation::Simulate { .. }, Some(level)) => {
            SimulatorTracing::setup_with_filter(&level.to_string());
        }
        (Operation::Simulate { .. }, None) => {
            SimulatorTracing::setup();
        }
        (Operation::LocalTestbed { .. }, None) => {
            ValidatorTracing::setup_with_level(LevelFilter::DEBUG);
        }
        (_, Some(level)) => {
            ValidatorTracing::setup_with_level(level);
        }
        (_, None) => {
            ValidatorTracing::setup();
        }
    }

    match args.operation {
        Operation::BenchmarkGenesis {
            ips,
            working_directory,
            node_parameters_path,
        } => commands::genesis::benchmark_genesis(ips, working_directory, node_parameters_path)?,
        Operation::Run {
            authority,
            committee_path,
            public_config_path,
            private_config_path,
            client_parameters_path,
        } => {
            commands::run::run(
                authority,
                committee_path,
                public_config_path,
                private_config_path,
                client_parameters_path,
            )
            .await?
        }
        Operation::Simulate {
            config_path,
            dump_config,
        } => commands::simulate::simulate(config_path, dump_config).await?,
        Operation::LocalTestbed {
            config_path,
            dump_config,
        } => commands::testbed::local_testbed(config_path, dump_config).await?,
    }

    Ok(())
}
