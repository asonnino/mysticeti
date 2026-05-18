// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use futures::future::try_join_all;
use russh::{
    ChannelMsg,
    client::{self, Handle},
    keys::{PrivateKeyWithHashAlg, PublicKey, load_secret_key},
};
use russh_sftp::client::SftpSession;
use tokio::{task::JoinHandle, time::sleep};

use crate::{
    ensure,
    error::{SshError, SshResult},
    provider::Instance,
};

#[derive(PartialEq, Eq)]
/// The status of a ssh command running in the background.
pub enum CommandStatus {
    Running,
    Terminated,
}

impl CommandStatus {
    /// Return whether a background command is still running. Returns `Terminated` if the
    /// command is not running in the background.
    pub fn status(command_id: &str, text: &str) -> Self {
        if text.contains(command_id) {
            Self::Running
        } else {
            Self::Terminated
        }
    }
}

/// The command to execute on all specified remote machines.
#[derive(Clone, Default)]
pub struct CommandContext {
    /// Whether to run the command in the background (and return immediately). Commands
    /// running in the background are identified by a unique id.
    pub background: Option<String>,
    /// The path from where to execute the command.
    pub path: Option<PathBuf>,
    /// The log file to redirect all stdout and stderr.
    pub log_file: Option<PathBuf>,
}

impl CommandContext {
    /// Create a new ssh command.
    pub fn new() -> Self {
        Self {
            background: None,
            path: None,
            log_file: None,
        }
    }

    /// Set id of the command and indicate that it should run in the background.
    pub fn run_background(mut self, id: String) -> Self {
        self.background = Some(id);
        self
    }

    /// Set the path from where to execute the command.
    pub fn with_execute_from_path(mut self, path: PathBuf) -> Self {
        self.path = Some(path);
        self
    }

    /// Set the log file where to redirect stdout and stderr.
    pub fn with_log_file(mut self, path: PathBuf) -> Self {
        self.log_file = Some(path);
        self
    }

    /// Apply the context to a base command.
    pub fn apply<S: Into<String>>(&self, base_command: S) -> String {
        let mut str = base_command.into();
        if let Some(log_file) = &self.log_file {
            str = format!("{str} |& tee {}", log_file.as_path().display());
        }
        if let Some(id) = &self.background {
            str = format!("tmux new -d -s \"{id}\" \"{str}\"");
        }
        if let Some(exec_path) = &self.path {
            str = format!("(cd {} && {str})", exec_path.as_path().display());
        }
        str
    }
}

#[derive(Clone)]
pub struct SshConnectionManager {
    /// The ssh username.
    username: String,
    /// The ssh primate key to connect to the instances.
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
    pub async fn connect(&self, address: SocketAddr) -> SshResult<SshConnection> {
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

    pub async fn wait_for_success<I, S>(&self, instances: I)
    where
        I: IntoIterator<Item = (Instance, S)> + Clone,
        S: Into<String> + Send + 'static + Clone,
    {
        loop {
            sleep(Self::RETRY_DELAY).await;

            if self
                .execute_per_instance(instances.clone(), CommandContext::default())
                .await
                .is_ok()
            {
                break;
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

/// Accepts any server host key. This matches the previous `ssh2`-based behavior, which did
/// not perform host-key verification. Real verification is tracked as a follow-up.
struct AcceptAllHostKeys;

impl client::Handler for AcceptAllHostKeys {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &PublicKey,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

/// Representation of an ssh connection.
pub struct SshConnection {
    /// The russh client handle for this session.
    handle: Handle<AcceptAllHostKeys>,
    /// The host address.
    address: SocketAddr,
    /// The number of retries before giving up to execute the command.
    retries: usize,
}

impl SshConnection {
    /// Default duration before timing out the ssh connection.
    const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

    /// Create a new ssh connection with a specific host.
    pub async fn new<P: AsRef<Path>>(
        address: SocketAddr,
        username: &str,
        private_key_file: P,
        timeout: Option<Duration>,
    ) -> SshResult<Self> {
        let private_key = load_secret_key(private_key_file.as_ref(), None).map_err(|e| {
            SshError::SessionError {
                address,
                source: e.into(),
            }
        })?;

        let config = Arc::new(client::Config {
            inactivity_timeout: Some(timeout.unwrap_or(Self::DEFAULT_TIMEOUT)),
            ..Default::default()
        });

        let mut handle = client::connect(config, address, AcceptAllHostKeys)
            .await
            .map_err(|source| SshError::SessionError { address, source })?;

        let key = PrivateKeyWithHashAlg::new(Arc::new(private_key), None);
        let authenticated = handle
            .authenticate_publickey(username, key)
            .await
            .map_err(|source| SshError::SessionError { address, source })?;

        if !authenticated.success() {
            return Err(SshError::SessionError {
                address,
                source: russh::Error::NotAuthenticated,
            });
        }

        Ok(Self {
            handle,
            address,
            retries: 0,
        })
    }

    /// Set the maximum number of times to retries to establish a connection and execute commands.
    pub fn with_retries(mut self, retries: usize) -> Self {
        self.retries = retries;
        self
    }

    /// Wrap a russh error with the connection's address.
    fn session_error(&self, source: russh::Error) -> SshError {
        SshError::SessionError {
            address: self.address,
            source,
        }
    }

    /// Wrap an SFTP error with the connection's address.
    fn sftp_error(&self, source: russh_sftp::client::error::Error) -> SshError {
        SshError::SftpError {
            address: self.address,
            source,
        }
    }

    /// Execute an ssh command on the remote machine.
    pub async fn execute(&self, command: String) -> SshResult<(String, String)> {
        let mut error = None;
        for _ in 0..self.retries + 1 {
            let mut channel = match self.handle.channel_open_session().await {
                Ok(c) => c,
                Err(e) => {
                    error = Some(self.session_error(e));
                    continue;
                }
            };

            if let Err(e) = channel.exec(true, command.as_bytes()).await {
                error = Some(self.session_error(e));
                continue;
            }

            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            let mut exit_status: Option<u32> = None;
            let mut signal: Option<(String, bool)> = None;

            // Drain channel messages until the remote side closes the channel. `wait` returns
            // None when the channel is closed.
            while let Some(msg) = channel.wait().await {
                match msg {
                    ChannelMsg::Data { data } => stdout.extend_from_slice(&data),
                    // ext == 1 is SSH_EXTENDED_DATA_STDERR per RFC 4254.
                    ChannelMsg::ExtendedData { data, ext: 1 } => stderr.extend_from_slice(&data),
                    ChannelMsg::ExitStatus { exit_status: code } => exit_status = Some(code),
                    ChannelMsg::ExitSignal {
                        signal_name,
                        core_dumped,
                        ..
                    } => signal = Some((format!("{signal_name:?}"), core_dumped)),
                    _ => {}
                }
            }

            let stdout = String::from_utf8_lossy(&stdout).into_owned();
            let stderr = String::from_utf8_lossy(&stderr).into_owned();

            if let Some((signal, core_dumped)) = signal {
                return Err(SshError::TerminatedBySignal {
                    address: self.address,
                    signal,
                    core_dumped,
                });
            }
            let code = match exit_status {
                Some(code) => code as i32,
                None => {
                    return Err(SshError::MissingExitStatus {
                        address: self.address,
                    });
                }
            };

            ensure!(
                code == 0,
                SshError::NonZeroExitCode {
                    address: self.address,
                    code,
                    message: stderr.clone()
                }
            );

            return Ok((stdout, stderr));
        }
        Err(error.unwrap())
    }

    /// Download a file from the remote machine over SFTP.
    pub async fn download<P: AsRef<Path>>(&self, path: P) -> SshResult<String> {
        let path = path.as_ref();
        let mut error = None;
        for _ in 0..self.retries + 1 {
            let channel = match self.handle.channel_open_session().await {
                Ok(c) => c,
                Err(e) => {
                    error = Some(self.session_error(e));
                    continue;
                }
            };
            if let Err(e) = channel.request_subsystem(true, "sftp").await {
                error = Some(self.session_error(e));
                continue;
            }

            let sftp = match SftpSession::new(channel.into_stream()).await {
                Ok(s) => s,
                Err(e) => {
                    error = Some(self.sftp_error(e));
                    continue;
                }
            };

            match sftp.read(path.to_string_lossy().as_ref()).await {
                Ok(bytes) => return Ok(String::from_utf8_lossy(&bytes).into_owned()),
                Err(e) => error = Some(self.sftp_error(e)),
            }
        }
        Err(error.unwrap())
    }
}
