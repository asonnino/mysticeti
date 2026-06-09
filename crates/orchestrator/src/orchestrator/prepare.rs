// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    benchmark::Parameters,
    error::TestbedResult,
    monitor::Monitor,
    protocol::{ProtocolCommands, ProtocolMetrics},
    report::ConfigureReport,
    ssh::{CommandContext, CommandStatus},
};

use super::Orchestrator;

impl<P: ProtocolCommands + ProtocolMetrics> Orchestrator<P> {
    /// Install the codebase and its dependencies on the testbed.
    pub async fn install(&self) -> TestbedResult<()> {
        let working_dir = self.settings.working_dir.display();
        let url = &self.settings.repository.url;
        let basic_commands = [
            "sudo apt-get update",
            "sudo apt-get -y upgrade",
            "sudo apt-get -y autoremove",
            // Disable "pending kernel upgrade" message.
            "sudo apt-get -y remove needrestart",
            // The following dependencies
            // * build-essential: prevent the error: [error: linker `cc` not found].
            // * sysstat - for getting disk stats
            // * iftop - for getting network stats
            // * libssl-dev - Required to compile the orchestrator
            // TODO: Remove libssl-dev dependency #7
            "sudo apt-get -y install build-essential sysstat iftop libssl-dev",
            "sudo apt-get -y install linux-tools-common linux-tools-generic pkg-config",
            // Install rust (non-interactive).
            "curl --proto \"=https\" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y",
            "echo \"source $HOME/.cargo/env\" | tee -a ~/.bashrc",
            "source $HOME/.cargo/env",
            "rustup default stable",
            // Create the working directory.
            &format!("mkdir -p {working_dir}"),
            // Clone the repo.
            &format!("(git clone {url} || true)"),
        ];

        let command = [
            &basic_commands[..],
            &Monitor::dependencies()
                .iter()
                .map(|x| x.as_str())
                .collect::<Vec<_>>()[..],
            &self
                .instance_setup_commands
                .iter()
                .map(|x| x.as_str())
                .collect::<Vec<_>>()[..],
            &self.protocol_commands.protocol_dependencies()[..],
        ]
        .concat()
        .join(" && ");

        let active = self.instances.iter().filter(|x| x.is_active()).cloned();
        let context = CommandContext::default();
        self.ssh_manager.execute(active, command, context).await?;

        Ok(())
    }

    /// Update all instances to use the version of the codebase specified in the setting file.
    pub async fn update(&self) -> TestbedResult<()> {
        // Update all active instances. This requires compiling the codebase in release (which
        // may take a long time) so we run the command in the background to avoid keeping alive
        // many ssh connections for too long.
        let commit = &self.settings.repository.commit;
        let command = [
            &format!("git fetch origin {commit}"),
            &format!("(git checkout -b {commit} || git checkout -f origin/{commit})"),
            "source $HOME/.cargo/env",
            "RUSTFLAGS=-Ctarget-cpu=native cargo build --release",
        ]
        .join(" && ");

        let active = self.instances.iter().filter(|x| x.is_active()).cloned();

        let id = "update";
        let repo_name = self.settings.repository_name();
        let context = CommandContext::new()
            .run_background(id.into())
            .with_execute_from_path(repo_name.into());
        self.ssh_manager
            .execute(active.clone(), command, context)
            .await?;

        // Wait until the command finished running.
        self.ssh_manager
            .wait_for_command(active, id, CommandStatus::Terminated)
            .await?;

        Ok(())
    }

    /// Configure the instances with the appropriate configuration files.
    pub async fn configure(&self, parameters: &Parameters<P>) -> TestbedResult<ConfigureReport> {
        // Select instances to configure.
        let (clients, nodes, _) = self.select_instances(parameters)?;
        let report = ConfigureReport::new(&nodes, &clients);

        // Generate the genesis configuration file and the keystore allowing access to gas objects.
        let command = self
            .protocol_commands
            .genesis_command(nodes.iter(), parameters)
            .await;

        let id = "configure";
        let repo_name = self.settings.repository_name();
        let context = CommandContext::new()
            .run_background(id.into())
            .with_log_file(format!("~/{id}.log").into())
            .with_execute_from_path(repo_name.into());
        let mut instances = nodes;
        if parameters.settings.dedicated_clients != 0 {
            instances.extend(clients);
        };

        self.ssh_manager
            .execute(instances.clone(), command, context)
            .await?;
        self.ssh_manager
            .wait_for_command(instances, id, CommandStatus::Terminated)
            .await?;

        Ok(report)
    }
}
