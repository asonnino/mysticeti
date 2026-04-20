// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs, io,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::crypto::PublicKey;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("{path}: {source}")]
    Io { path: PathBuf, source: io::Error },
    #[error("{path}: {source}")]
    Format {
        path: PathBuf,
        source: serde_yaml::Error,
    },
}

pub trait ImportExport: Serialize + DeserializeOwned {
    fn load<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let content = fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        serde_yaml::from_str(&content).map_err(|source| ConfigError::Format {
            path: path.to_path_buf(),
            source,
        })
    }

    fn to_yaml(&self) -> String {
        serde_yaml::to_string(self).expect("Failed to serialize config to YAML")
    }

    fn print<P: AsRef<Path>>(&self, path: P) -> Result<(), ConfigError> {
        let path = path.as_ref();
        let content = serde_yaml::to_string(self).map_err(|source| ConfigError::Format {
            path: path.to_path_buf(),
            source,
        })?;
        fs::write(path, content).map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DagParameters {
    /// Override the round timeout. When `None`, the runtime falls
    /// back to the chosen consensus protocol's default.
    #[serde(default)]
    pub round_timeout: Option<Duration>,
    #[serde(default = "dag_defaults::default_max_block_size")]
    pub max_block_size: usize,
    #[serde(default = "dag_defaults::default_enable_synchronizer")]
    pub enable_synchronizer: bool,
    #[serde(default = "dag_defaults::default_fsync")]
    pub fsync: bool,
}

pub mod dag_defaults {
    pub fn default_max_block_size() -> usize {
        4 * 1024 * 1024
    }

    pub fn default_enable_synchronizer() -> bool {
        false
    }

    pub fn default_fsync() -> bool {
        false
    }
}

impl Default for DagParameters {
    fn default() -> Self {
        Self {
            round_timeout: None,
            max_block_size: dag_defaults::default_max_block_size(),
            enable_synchronizer: dag_defaults::default_enable_synchronizer(),
            fsync: dag_defaults::default_fsync(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ReplicaIdentifier {
    pub public_key: PublicKey,
    pub network_address: SocketAddr,
    pub metrics_address: SocketAddr,
}
