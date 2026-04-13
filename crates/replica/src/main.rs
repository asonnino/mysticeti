// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use clap::Parser;
use eyre::Result;
use replica::{
    cli::{Args, Operation},
    commands,
};

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    let args = Args::parse();

    match args.operation {
        Operation::TestGenesis {
            ips,
            working_directory,
            node_parameters_path,
        } => commands::genesis::test_genesis(
            ips,
            working_directory,
            node_parameters_path,
            args.log_level,
        )?,
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
                args.log_level,
            )
            .await?
        }
        Operation::Simulate {
            config_path,
            dump_config,
        } => commands::simulate::simulate(config_path, dump_config, args.log_level).await?,
        Operation::LocalTestbed {
            config_path,
            dump_config,
        } => commands::testbed::local_testbed(config_path, dump_config, args.log_level).await?,
    }

    Ok(())
}
