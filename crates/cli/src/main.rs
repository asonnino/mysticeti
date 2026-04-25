// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use clap::Parser;
use cli::{
    args::{Args, Command},
    commands, terminal,
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
            output_dir,
            export_dag,
        } => {
            commands::simulate::simulate(
                dump_config,
                config_path,
                output_dir,
                export_dag,
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
            terminal::BannerPrinter::new("Mysticeti", &[("Mode", "Preview")]).print();
        }
    }

    Ok(())
}
