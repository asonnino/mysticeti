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
    let log_level = args.log_level;
    let log_file = args.log_file;

    match args.command {
        Command::TestGenesis(sub) => commands::genesis::test_genesis(sub, log_level, log_file)?,
        Command::Run(sub) => commands::run::run(sub, log_level, log_file).await?,
        Command::Simulate(sub) => commands::simulate::simulate(sub, log_level, log_file).await?,
        Command::LocalTestbed(sub) => {
            commands::testbed::local_testbed(sub, log_level, log_file).await?
        }
        Command::PrintBanner => {
            terminal::BannerPrinter::new("Mysticeti", &[("Mode", "Preview")]).print();
        }
    }

    Ok(())
}
