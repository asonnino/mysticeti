// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod benchmark;
pub mod protocol;
mod testbed;

use std::fmt::Display;

use eyre::{Context, Result};
use orchestrator::{
    provider::ServerProviderClient, settings::Settings, ssh::SshConnectionManager, testbed::Testbed,
};

use crate::{args::RemoteTestbedCommand, terminal::Progress};

use benchmark::RemoteBenchmarkDriver;
use testbed::TestbedStatusRender;

pub(crate) struct RemoteTestbedDriver<C> {
    pub(crate) testbed: Testbed<C>,
    pub(crate) settings: Settings,
    pub(crate) color: bool,
    progress: Progress,
}

impl<C: ServerProviderClient> RemoteTestbedDriver<C> {
    pub(crate) async fn new(settings: Settings, client: C, color: bool) -> Result<Self> {
        let testbed = Testbed::new(settings.clone(), client)
            .await
            .wrap_err("Failed to create testbed")?;
        Ok(Self {
            testbed,
            settings,
            color,
            progress: Progress::new(color),
        })
    }
}

impl<C: ServerProviderClient + Display> RemoteTestbedDriver<C> {
    pub(crate) async fn run(mut self, command: RemoteTestbedCommand) -> Result<()> {
        match command {
            RemoteTestbedCommand::Status => {
                eprint!("{}", self.testbed.status().render(self.color));
                Ok(())
            }
            RemoteTestbedCommand::Create { instances, region } => {
                let label = format!("Creating instances ({instances} per region)");
                self.progress
                    .track(&label, self.testbed.create(instances, region))
                    .await
                    .wrap_err("Failed to create testbed")
            }
            RemoteTestbedCommand::Start { instances } => self
                .progress
                .track("Booting instances", self.testbed.start(instances))
                .await
                .wrap_err("Failed to start testbed"),
            RemoteTestbedCommand::Stop => self
                .progress
                .track("Stopping instances", self.testbed.stop())
                .await
                .wrap_err("Failed to stop testbed"),
            RemoteTestbedCommand::Destroy => self
                .progress
                .track("Destroying testbed", self.testbed.destroy())
                .await
                .wrap_err("Failed to destroy testbed"),
            RemoteTestbedCommand::Benchmark {
                committee,
                loads,
                skip_testbed_update,
                skip_testbed_configuration,
            } => {
                let instances = self.testbed.instances();
                let setup_commands = self
                    .testbed
                    .setup_commands()
                    .await
                    .wrap_err("Failed to load testbed setup commands")?;
                let ssh_manager = SshConnectionManager::new(
                    self.testbed.username().into(),
                    self.settings.ssh_private_key_file.clone(),
                )
                .with_timeout(self.settings.ssh_timeout)
                .with_retries(self.settings.ssh_retries);
                RemoteBenchmarkDriver::new(self.settings, ssh_manager, self.color)
                    .benchmark(
                        instances,
                        setup_commands,
                        committee,
                        loads,
                        skip_testbed_update,
                        skip_testbed_configuration,
                    )
                    .await
            }
        }
    }
}
