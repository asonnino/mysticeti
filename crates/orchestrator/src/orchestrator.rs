// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, VecDeque},
    fs,
    net::SocketAddr,
    path::PathBuf,
};

use crate::{
    benchmark::Parameters,
    ensure,
    error::{TestbedError, TestbedResult},
    faults::{CrashRecoveryAction, CrashRecoverySchedule},
    logs::{LogsAnalyzer, LogsReport},
    monitor::Monitor,
    protocol::{ProtocolCommands, ProtocolMetrics},
    provider::Instance,
    settings::Settings,
    ssh::{CommandContext, CommandStatus, SshConnectionManager},
};

/// Per-instance addresses produced by [`Orchestrator::configure`]. The caller
/// uses this to render the "Configuring instances" table; `Orchestrator` itself
/// never prints.
pub struct ConfigureReport {
    pub nodes: Vec<(usize, SocketAddr)>,
    pub clients: Vec<(usize, SocketAddr)>,
}

impl ConfigureReport {
    /// Snapshot the addresses of every node and client the orchestrator just
    /// configured.
    pub fn new(nodes: &[Instance], clients: &[Instance]) -> Self {
        let enumerate_addresses = |instances: &[Instance]| {
            instances
                .iter()
                .enumerate()
                .map(|(index, instance)| (index, instance.ssh_address()))
                .collect()
        };
        Self {
            nodes: enumerate_addresses(nodes),
            clients: enumerate_addresses(clients),
        }
    }
}

/// Outcome of a successful [`Orchestrator::start_monitoring`] when the testbed
/// has a dedicated monitoring instance configured. `None` from `start_monitoring`
/// means monitoring is disabled in [`Settings`].
pub struct MonitoringReport {
    pub grafana_address: String,
    /// PromQL endpoint that consumers can hand to [`crate::collector::Collector`]
    /// when they want lib-provided metric collection. Always populated when this
    /// report is returned — the same monitoring instance hosts both servers.
    pub prometheus_address: String,
}

/// An orchestrator to deploy nodes and run benchmarks on a testbed.
pub struct Orchestrator<P> {
    /// The testbed's settings.
    settings: Settings,
    /// The state of the testbed (reflecting accurately the state of the machines).
    instances: Vec<Instance>,
    /// Provider-specific commands to install on the instance.
    instance_setup_commands: Vec<String>,
    /// Protocol-specific commands generator to generate the protocol configuration files,
    /// boot clients and nodes, etc.
    protocol_commands: P,
    /// Handle ssh connections to instances.
    ssh_manager: SshConnectionManager,
}

impl<P> Orchestrator<P> {
    /// Make a new orchestrator.
    pub fn new(
        settings: Settings,
        instances: Vec<Instance>,
        instance_setup_commands: Vec<String>,
        protocol_commands: P,
        ssh_manager: SshConnectionManager,
    ) -> Self {
        Self {
            settings,
            instances,
            instance_setup_commands,
            protocol_commands,
            ssh_manager,
        }
    }

    /// Borrow the protocol implementation the orchestrator is driving. Callers
    /// that need protocol-specific data (e.g. metric names for the PromQL
    /// collector) should ask the orchestrator rather than re-instantiating the
    /// protocol from `Settings`, so a non-default `P` is honored.
    pub fn protocol(&self) -> &P {
        &self.protocol_commands
    }
}

impl<P: ProtocolCommands + ProtocolMetrics> Orchestrator<P> {
    /// Returns the instances of the testbed on which to run the benchmarks.
    ///
    /// This function returns two vectors of instances; the first contains the instances on which to
    /// run the load generators and the second contains the instances on which to run the nodes.
    /// Additionally returns an optional monitoring instance.
    pub fn select_instances(
        &self,
        parameters: &Parameters<P>,
    ) -> TestbedResult<(Vec<Instance>, Vec<Instance>, Option<Instance>)> {
        // Ensure there are enough active instances.
        let available_instances: Vec<_> = self.instances.iter().filter(|x| x.is_active()).collect();
        let minimum_instances = parameters.nodes
            + self.settings.dedicated_clients
            + if self.settings.monitoring { 1 } else { 0 };
        ensure!(
            available_instances.len() >= minimum_instances,
            TestbedError::InsufficientCapacity(minimum_instances - available_instances.len())
        );

        // Sort the instances by region. This step ensures that the instances are selected as
        // equally as possible from all regions.
        let mut instances_by_regions = HashMap::new();
        for instance in available_instances {
            instances_by_regions
                .entry(&instance.region)
                .or_insert_with(VecDeque::new)
                .push_back(instance);
        }

        // Select the instance to host the monitoring stack. If monitoring is
        // enabled but the first region has no active instance to spare, that's
        // a capacity error — silently dropping monitoring would surprise the
        // caller (who already saw the action banner and expects a Grafana URL).
        let mut monitoring_instance = None;
        if self.settings.monitoring {
            let region = &self.settings.regions[0];
            monitoring_instance = Some(
                instances_by_regions
                    .get_mut(region)
                    .and_then(|instances| instances.pop_front())
                    .ok_or(TestbedError::InsufficientCapacity(1))?
                    .clone(),
            );
        }

        // Select the instances to host exclusively load generators.
        let mut client_instances = Vec::new();
        for region in self.settings.regions.iter().cycle() {
            if client_instances.len() == self.settings.dedicated_clients {
                break;
            }
            if let Some(regional_instances) = instances_by_regions.get_mut(region)
                && let Some(instance) = regional_instances.pop_front()
            {
                client_instances.push(instance.clone());
            }
        }

        // Select the instances to host the nodes.
        let mut nodes_instances = Vec::new();
        for region in self.settings.regions.iter().cycle() {
            if nodes_instances.len() == parameters.nodes {
                break;
            }
            if let Some(regional_instances) = instances_by_regions.get_mut(region)
                && let Some(instance) = regional_instances.pop_front()
            {
                nodes_instances.push(instance.clone());
            }
        }

        // Spawn a load generate collocated with each node if there are no instances dedicated
        // to excursively run load generators.
        if client_instances.is_empty() {
            client_instances.clone_from(&nodes_instances);
        }

        Ok((client_instances, nodes_instances, monitoring_instance))
    }

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

    /// Cleanup all instances and optionally delete their log files.
    pub async fn cleanup(&self, delete_logs: bool) -> TestbedResult<()> {
        // Kill all tmux servers and delete the nodes dbs. Optionally clear logs.
        let mut command = vec!["(tmux kill-server || true)".into()];
        for path in self.protocol_commands.db_directories() {
            command.push(format!("(rm -rf {} || true)", path.display()));
        }
        if delete_logs {
            command.push("(rm -rf ~/*log* || true)".into());
        }
        let command = command.join(" ; ");

        // Execute the deletion on all machines.
        let active = self.instances.iter().filter(|x| x.is_active()).cloned();
        let context = CommandContext::default();
        self.ssh_manager.execute(active, command, context).await?;

        Ok(())
    }

    /// Reload prometheus and grafana. Returns `Some(report)` when monitoring is
    /// enabled (the testbed has a dedicated monitor instance) and `None`
    /// otherwise.
    pub async fn start_monitoring(
        &self,
        parameters: &Parameters<P>,
    ) -> TestbedResult<Option<MonitoringReport>> {
        let (clients, nodes, instance) = self.select_instances(parameters)?;
        let Some(instance) = instance else {
            return Ok(None);
        };

        let monitor = Monitor::new(instance, clients, nodes, self.ssh_manager.clone());
        let commands = &self.protocol_commands;
        monitor.start_prometheus(commands, parameters).await?;
        monitor.start_grafana().await?;

        Ok(Some(MonitoringReport {
            grafana_address: monitor.grafana_address(),
            prometheus_address: monitor.prometheus_address(),
        }))
    }

    /// Boot a node on the specified instances.
    async fn boot_nodes(
        &self,
        instances: Vec<Instance>,
        parameters: &Parameters<P>,
    ) -> TestbedResult<()> {
        // Run one node per instance.
        let targets = self
            .protocol_commands
            .node_command(instances.clone(), parameters);

        let repo = self.settings.repository_name();
        let context = CommandContext::new()
            .run_background("node".into())
            .with_log_file("~/node.log".into())
            .with_execute_from_path(repo.into());
        self.ssh_manager
            .execute_per_instance(targets, context)
            .await?;

        // Wait until all nodes are reachable.
        let commands = self
            .protocol_commands
            .nodes_metrics_command(instances.clone(), parameters);
        self.ssh_manager.wait_for_success(commands).await;

        Ok(())
    }

    /// Deploy the nodes.
    pub async fn run_nodes(&self, parameters: &Parameters<P>) -> TestbedResult<()> {
        // Select the instances to run.
        let (_, nodes, _) = self.select_instances(parameters)?;

        // Boot one node per instance.
        self.boot_nodes(nodes, parameters).await?;

        Ok(())
    }

    /// Deploy the load generators.
    pub async fn run_clients(&self, parameters: &Parameters<P>) -> TestbedResult<()> {
        if parameters.load == 0 {
            return Ok(());
        }

        // Select the instances to run.
        let (clients, _, _) = self.select_instances(parameters)?;

        // Deploy the load generators.
        let targets = self
            .protocol_commands
            .client_command(clients.clone(), parameters);

        let repo = self.settings.repository_name();
        let context = CommandContext::new()
            .run_background("client".into())
            .with_log_file("~/client.log".into())
            .with_execute_from_path(repo.into());
        self.ssh_manager
            .execute_per_instance(targets, context)
            .await?;

        // Wait until all load generators are reachable.
        let commands = self
            .protocol_commands
            .clients_metrics_command(clients, parameters);
        self.ssh_manager.wait_for_success(commands).await;

        Ok(())
    }

    /// Advance the fault schedule by one step: query [`CrashRecoverySchedule`]
    /// for the next [`CrashRecoveryAction`], then SSH-kill any newly-failed
    /// instances and SSH-boot any recovered ones. Returns the action so the
    /// caller can update its own "killed" tracking and render whatever banner
    /// it wants.
    pub async fn apply_faults_step(
        &self,
        parameters: &Parameters<P>,
        schedule: &mut CrashRecoverySchedule,
    ) -> TestbedResult<CrashRecoveryAction> {
        let action = schedule.update();
        if !action.kill.is_empty() {
            self.ssh_manager.kill(action.kill.clone(), "node").await?;
        }
        if !action.boot.is_empty() {
            self.boot_nodes(action.boot.clone(), parameters).await?;
        }
        Ok(action)
    }

    /// Download the log files from the nodes and clients.
    pub async fn download_logs(&self, parameters: &Parameters<P>) -> TestbedResult<LogsReport> {
        // Select the instances to run.
        let (clients, nodes, _) = self.select_instances(parameters)?;

        // Create a log sub-directory for this run.
        let commit = &self.settings.repository.commit;
        let path: PathBuf = [
            &self.settings.logs_dir,
            &format!("logs-{commit}").into(),
            &format!("logs-{parameters:?}").into(),
        ]
        .iter()
        .collect();
        fs::create_dir_all(&path).expect("Failed to create log directory");

        // NOTE: Our ssh library does not seem to be able to transfers files in parallel reliably.
        let mut log_parsers = Vec::new();

        // Download the clients log files.
        for (index, instance) in clients.iter().enumerate() {
            let connection = self.ssh_manager.connect(instance.ssh_address()).await?;
            let client_log_content = connection.download("client.log").await?;

            let client_log_file = [path.clone(), format!("client-{index}.log").into()]
                .iter()
                .collect::<PathBuf>();
            fs::write(&client_log_file, client_log_content.as_bytes())
                .expect("Cannot write log file");

            let mut log_parser = LogsAnalyzer::default();
            log_parser.set_client_errors(&client_log_content);
            log_parsers.push(log_parser)
        }

        for (index, instance) in nodes.iter().enumerate() {
            let connection = self.ssh_manager.connect(instance.ssh_address()).await?;
            let node_log_content = connection.download("node.log").await?;

            let node_log_file = [path.clone(), format!("node-{index}.log").into()]
                .iter()
                .collect::<PathBuf>();
            fs::write(&node_log_file, node_log_content.as_bytes()).expect("Cannot write log file");

            let mut log_parser = LogsAnalyzer::default();
            log_parser.set_node_errors(&node_log_content);
            log_parsers.push(log_parser)
        }

        Ok(log_parsers
            .into_iter()
            .max()
            .expect("At least one log parser")
            .summarise())
    }
}
