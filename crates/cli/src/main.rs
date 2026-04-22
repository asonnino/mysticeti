// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use clap::Parser;
use cli::{
    args::{Args, Command},
    banner, commands,
};
use eyre::Result;

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let args = Args::parse();

    match args.command {
        Command::TestGenesis {
            ips,
            working_directory,
            replica_parameters_path,
        } => commands::genesis::test_genesis(
            ips,
            working_directory,
            replica_parameters_path,
            args.log_level,
            args.log_file,
        )?,
        Command::Run {
            authority,
            public_config_path,
            private_config_path,
            load_generator_config_path,
        } => {
            commands::run::run(
                authority,
                public_config_path,
                private_config_path,
                load_generator_config_path,
                args.log_level,
                args.log_file,
            )
            .await?
        }
        Command::Simulate {
            config_path,
            dump_config,
            results_file,
        } => {
            commands::simulate::simulate(
                config_path,
                dump_config,
                results_file,
                args.log_level,
                args.log_file,
            )
            .await?
        }
        Command::LocalTestbed {
            committee_size,
            replica_parameters_path,
            load_generator_config_path,
        } => {
            commands::testbed::local_testbed(
                committee_size,
                replica_parameters_path,
                load_generator_config_path,
                args.log_level,
                args.log_file,
            )
            .await?
        }
        Command::PrintBanner => {
            banner::BannerPrinter::new("Mysticeti", &[("Mode", "Preview")]).print();
        }
    }

    Ok(())
}
