// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use futures::future::try_join_all;

use crate::{
    benchmark::Parameters,
    error::TestbedResult,
    faults::{CrashRecoveryAction, CrashRecoverySchedule},
    logs::{LogsAnalyzer, LogsReport},
    protocol::{ProtocolCommands, ProtocolMetrics},
    provider::Instance,
    ssh::CommandContext,
};

use super::Orchestrator;

impl<P: ProtocolCommands + ProtocolMetrics> Orchestrator<P> {
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
        self.ssh_manager
            .wait_for_success(commands, self.settings.health_check_timeout)
            .await?;

        Ok(())
    }

    /// Deploy the nodes.
    pub async fn run_nodes(&self, parameters: &Parameters<P>) -> TestbedResult<()> {
        let (_, nodes, _) = self.select_instances(parameters)?;
        self.boot_nodes(nodes, parameters).await?;
        Ok(())
    }

    /// Deploy the load generators.
    pub async fn run_clients(&self, parameters: &Parameters<P>) -> TestbedResult<()> {
        if parameters.load == 0 {
            return Ok(());
        }

        let (clients, _, _) = self.select_instances(parameters)?;

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
        self.ssh_manager
            .wait_for_success(commands, self.settings.health_check_timeout)
            .await?;

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
        tokio::fs::create_dir_all(&path)
            .await
            .expect("Failed to create log directory");

        let (client_parsers, node_parsers) = tokio::try_join!(
            fetch_logs(
                &clients,
                self.ssh_manager.clone(),
                path.clone(),
                "client.log",
                "client",
                LogsAnalyzer::set_client_errors,
            ),
            fetch_logs(
                &nodes,
                self.ssh_manager.clone(),
                path,
                "node.log",
                "node",
                LogsAnalyzer::set_node_errors,
            ),
        )?;

        Ok(client_parsers
            .into_iter()
            .chain(node_parsers)
            .max_by_key(|a| {
                let s = a.summarize();
                (s.node_panic, s.client_panic, s.node_errors, s.client_errors)
            })
            .expect("At least one log parser")
            .summarize())
    }
}

/// Download one log file per instance in parallel, write each to `log_dir`, and
/// return a [`LogsAnalyzer`] per instance. `set_errors` is a method pointer
/// (`LogsAnalyzer::set_client_errors` or `LogsAnalyzer::set_node_errors`) that
/// applies the content to the analyser.
async fn fetch_logs(
    instances: &[Instance],
    ssh_manager: crate::ssh::SshConnectionManager,
    log_dir: PathBuf,
    remote_file: &'static str,
    local_prefix: &'static str,
    set_errors: fn(&mut LogsAnalyzer, &str),
) -> TestbedResult<Vec<LogsAnalyzer>> {
    try_join_all(instances.iter().enumerate().map(|(index, instance)| {
        let ssh_manager = ssh_manager.clone();
        let path = log_dir.clone();
        let address = instance.ssh_address();
        async move {
            let connection = ssh_manager.connect(address).await?;
            let content = connection.download(remote_file).await?;
            let log_file: PathBuf = [path, format!("{local_prefix}-{index}.log").into()]
                .iter()
                .collect();
            tokio::fs::write(&log_file, content.as_bytes())
                .await
                .expect("Cannot write log file");
            let mut parser = LogsAnalyzer::default();
            set_errors(&mut parser, &content);
            TestbedResult::Ok(parser)
        }
    }))
    .await
}
