// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, path::Path, string::FromUtf8Error, sync::Arc, time::Duration};

use russh::{
    ChannelMsg, Sig,
    client::{self, Handle},
    keys::{PrivateKeyWithHashAlg, PublicKey, load_secret_key},
};
use russh_sftp::client::SftpSession;

use crate::error::{SshError, SshResult};

/// Render a russh signal name without the Debug-format wrapping that would
/// otherwise appear for `Sig::Custom(_)`. Unit variants format as their plain
/// signal name (e.g. `TERM`); a `Custom` payload is unwrapped so the remote
/// supplied name is preserved verbatim.
fn render_signal(sig: Sig) -> String {
    match sig {
        Sig::ABRT => "ABRT".into(),
        Sig::ALRM => "ALRM".into(),
        Sig::FPE => "FPE".into(),
        Sig::HUP => "HUP".into(),
        Sig::ILL => "ILL".into(),
        Sig::INT => "INT".into(),
        Sig::KILL => "KILL".into(),
        Sig::PIPE => "PIPE".into(),
        Sig::QUIT => "QUIT".into(),
        Sig::SEGV => "SEGV".into(),
        Sig::TERM => "TERM".into(),
        Sig::USR1 => "USR1".into(),
        Sig::Custom(s) => s,
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
pub(crate) struct SshConnection {
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
    pub(crate) async fn new<P: AsRef<Path>>(
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
    pub(crate) fn with_retries(mut self, retries: usize) -> Self {
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

    /// Wrap a UTF-8 decoding failure with the connection's address.
    fn utf8_error(&self, source: FromUtf8Error) -> SshError {
        SshError::InvalidUtf8 {
            address: self.address,
            source,
        }
    }

    /// Execute an ssh command on the remote machine.
    pub(crate) async fn execute(&self, command: String) -> SshResult<(String, String)> {
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
                    } => signal = Some((render_signal(signal_name), core_dumped)),
                    _ => {}
                }
            }

            let stdout = match String::from_utf8(stdout) {
                Ok(s) => s,
                Err(e) => {
                    error = Some(self.utf8_error(e));
                    continue;
                }
            };
            let stderr = match String::from_utf8(stderr) {
                Ok(s) => s,
                Err(e) => {
                    error = Some(self.utf8_error(e));
                    continue;
                }
            };

            if let Some((signal, core_dumped)) = signal {
                error = Some(SshError::TerminatedBySignal {
                    address: self.address,
                    signal,
                    core_dumped,
                });
                continue;
            }
            let code = match exit_status {
                Some(code) => code as i32,
                None => {
                    error = Some(SshError::MissingExitStatus {
                        address: self.address,
                    });
                    continue;
                }
            };

            if code != 0 {
                error = Some(SshError::NonZeroExitCode {
                    address: self.address,
                    code,
                    message: stderr,
                });
                continue;
            }

            return Ok((stdout, stderr));
        }
        Err(error.unwrap())
    }

    /// Download a file from the remote machine over SFTP.
    pub(crate) async fn download<P: AsRef<Path>>(&self, path: P) -> SshResult<String> {
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

            let bytes = match sftp.read(path.to_string_lossy().as_ref()).await {
                Ok(b) => b,
                Err(e) => {
                    error = Some(self.sftp_error(e));
                    continue;
                }
            };
            match String::from_utf8(bytes) {
                Ok(s) => return Ok(s),
                Err(e) => error = Some(self.utf8_error(e)),
            }
        }
        Err(error.unwrap())
    }
}
