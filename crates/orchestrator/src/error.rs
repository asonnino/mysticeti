// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, string::FromUtf8Error, time::Duration};

#[macro_export(local_inner_macros)]
macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            return Err($e);
        }
    };
}

pub type SettingsResult<T> = Result<T, SettingsError>;

#[derive(thiserror::Error, Debug)]
pub enum SettingsError {
    #[error("Failed to read settings file '{file:?}': {message}")]
    InvalidSettings { file: String, message: String },

    #[error("Failed to read ssh public key file '{file:?}': {message}")]
    SshPublicKeyFileError { file: String, message: String },
}

pub type CloudProviderResult<T> = Result<T, CloudProviderError>;

#[derive(thiserror::Error, Debug)]
pub enum CloudProviderError {
    #[error("Failed to send server request: {0}")]
    RequestError(String),

    #[error("Unexpected response: {0}")]
    UnexpectedResponse(String),

    #[error("Received error status code ({0}): {1}")]
    FailureResponseCode(String, String),

    #[error("SSH key \"{0}\" not found")]
    SshKeyNotFound(String),

    #[error("Operation not supported by this provider: {0}")]
    Unsupported(String),
}

pub type SshResult<T> = Result<T, SshError>;

#[derive(thiserror::Error, Debug)]
pub enum SshError {
    #[error("Failed to create ssh session with {address}: {source}")]
    SessionError {
        address: SocketAddr,
        #[source]
        source: russh::Error,
    },

    #[error("SFTP error on {address}: {source}")]
    SftpError {
        address: SocketAddr,
        #[source]
        source: russh_sftp::client::error::Error,
    },

    #[error("Remote execution on {address} returned exit code ({code}): {message}")]
    NonZeroExitCode {
        address: SocketAddr,
        code: i32,
        message: String,
    },

    #[error("Remote execution on {address} killed by {signal} (core_dumped: {core_dumped})")]
    TerminatedBySignal {
        address: SocketAddr,
        signal: String,
        core_dumped: bool,
    },

    #[error("Remote execution on {address} did not report an exit status")]
    MissingExitStatus { address: SocketAddr },

    #[error("Remote output from {address} was not valid UTF-8: {source}")]
    InvalidUtf8 {
        address: SocketAddr,
        #[source]
        source: FromUtf8Error,
    },

    #[error("Timed out after {timeout:?} waiting for remote service to become healthy")]
    WaitTimeout { timeout: Duration },
}

pub(crate) type MonitorResult<T> = Result<T, MonitorError>;

#[derive(thiserror::Error, Debug)]
pub enum MonitorError {
    #[error(transparent)]
    SshError(#[from] SshError),

    #[error("Failed to start Grafana: {0}")]
    GrafanaError(String),

    #[error("Prometheus query failed: {0}")]
    PrometheusError(#[from] prometheus_http_query::Error),

    #[error("Unexpected Prometheus response: instant query did not return a vector")]
    UnexpectedPrometheusResponse,

    #[error("Failed to write benchmark results: {0}")]
    ResultsWriteError(#[from] std::io::Error),
}

pub type TestbedResult<T> = Result<T, TestbedError>;

#[derive(thiserror::Error, Debug)]
pub enum TestbedError {
    #[error(transparent)]
    SettingsError(#[from] SettingsError),

    #[error(transparent)]
    CloudProviderError(#[from] CloudProviderError),

    #[error(transparent)]
    SshError(#[from] SshError),

    #[error(
        "Not enough active instances: need {needed} \
        ({nodes} node(s) + {clients} dedicated client(s) + {monitoring} monitoring) \
        but only {available} active"
    )]
    InsufficientCapacity {
        needed: usize,
        available: usize,
        nodes: usize,
        clients: usize,
        monitoring: usize,
    },

    #[error("No active instance available in region '{region}' to host the monitoring stack")]
    NoMonitoringInstance { region: String },

    #[error(transparent)]
    MonitorError(#[from] MonitorError),
}
