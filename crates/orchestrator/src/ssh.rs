// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod command;
mod connection;

pub use command::{CommandContext, CommandStatus};
pub use connection::SshConnection;

use std::{path::PathBuf, time::Duration};

use futures::future::try_join_all;
use tokio::{
    task::JoinHandle,
    time::{Instant, sleep},
};

use crate::{
    error::{SshError, SshResult},
    provider::Instance,
};

#[derive(Clone)]
pub struct SshConnectionManager {
    /// The ssh username.
    username: String,
    /// The ssh private key to connect to the instances.
    private_key_file: PathBuf,
    /// The timeout value of the connection.
    timeout: Option<Duration>,
    /// The number of retries before giving up to execute the command.
    retries: usize,
}

impl SshConnectionManager {
    /// Delay before re-attempting an ssh execution.
    const RETRY_DELAY: Duration = Duration::from_secs(5);

    /// Create a new ssh manager from the instances username and private keys.
    pub fn new(username: String, private_key_file: PathBuf) -> Self {
        Self {
            username,
            private_key_file,
            timeout: None,
            retries: 0,
        }
    }

    /// Set a timeout duration for the connections.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set the maximum number of times to retries to establish a connection and execute commands.
    pub fn with_retries(mut self, retries: usize) -> Self {
        self.retries = retries;
        self
    }

    /// Create a new ssh connection with the provided host.
    pub async fn connect(&self, address: std::net::SocketAddr) -> SshResult<SshConnection> {
        let mut error = None;
        for _ in 0..self.retries + 1 {
            match SshConnection::new(
                address,
                &self.username,
                self.private_key_file.clone(),
                self.timeout,
            )
            .await
            {
                Ok(x) => return Ok(x.with_retries(self.retries)),
                Err(e) => error = Some(e),
            }
            sleep(Self::RETRY_DELAY).await;
        }
        Err(error.unwrap())
    }

    /// Execute the specified ssh command on all provided instances.
    pub async fn execute<I, S>(
        &self,
        instances: I,
        command: S,
        context: CommandContext,
    ) -> SshResult<Vec<(String, String)>>
    where
        I: IntoIterator<Item = Instance>,
        S: Into<String> + Clone + Send + 'static,
    {
        let targets = instances
            .into_iter()
            .map(|instance| (instance, command.clone()));
        self.execute_per_instance(targets, context).await
    }

    /// Execute the ssh command associated with each instance.
    pub async fn execute_per_instance<I, S>(
        &self,
        instances: I,
        context: CommandContext,
    ) -> SshResult<Vec<(String, String)>>
    where
        I: IntoIterator<Item = (Instance, S)>,
        S: Into<String> + Send + 'static,
    {
        let handles = self.run_per_instance(instances, context);

        try_join_all(handles)
            .await
            .unwrap()
            .into_iter()
            .collect::<SshResult<_>>()
    }

    pub fn run_per_instance<I, S>(
        &self,
        instances: I,
        context: CommandContext,
    ) -> Vec<JoinHandle<SshResult<(String, String)>>>
    where
        I: IntoIterator<Item = (Instance, S)>,
        S: Into<String> + Send + 'static,
    {
        instances
            .into_iter()
            .map(|(instance, command)| {
                let ssh_manager = self.clone();
                let context = context.clone();

                tokio::spawn(async move {
                    let connection = ssh_manager.connect(instance.ssh_address()).await?;
                    connection.execute(context.apply(command)).await
                })
            })
            .collect::<Vec<_>>()
    }

    /// Wait until a command running in the background returns or started.
    pub async fn wait_for_command<I>(
        &self,
        instances: I,
        command_id: &str,
        status: CommandStatus,
    ) -> SshResult<()>
    where
        I: IntoIterator<Item = Instance> + Clone,
    {
        loop {
            sleep(Self::RETRY_DELAY).await;

            let result = self
                .execute(
                    instances.clone(),
                    "(tmux ls || true)",
                    CommandContext::default(),
                )
                .await?;
            if result
                .iter()
                .all(|(stdout, _)| CommandStatus::status(command_id, stdout) == status)
            {
                break;
            }
        }
        Ok(())
    }

    pub async fn wait_for_success<I, S>(&self, instances: I, timeout: Duration) -> SshResult<()>
    where
        I: IntoIterator<Item = (Instance, S)> + Clone,
        S: Into<String> + Send + 'static + Clone,
    {
        let deadline = Instant::now() + timeout;
        loop {
            sleep(Self::RETRY_DELAY).await;
            if self
                .execute_per_instance(instances.clone(), CommandContext::default())
                .await
                .is_ok()
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(SshError::WaitTimeout { timeout });
            }
        }
    }

    /// Kill a command running in the background of the specified instances.
    pub async fn kill<I>(&self, instances: I, command_id: &str) -> SshResult<()>
    where
        I: IntoIterator<Item = Instance>,
    {
        let ssh_command = format!("(tmux kill-session -t {command_id} || true)");
        let targets = instances.into_iter().map(|x| (x, ssh_command.clone()));
        self.execute_per_instance(targets, CommandContext::default())
            .await?;
        Ok(())
    }
}
