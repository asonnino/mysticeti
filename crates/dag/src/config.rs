// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs, io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    authority::Authority,
    block::RoundNumber,
    crypto::PublicKey,
    crypto::{Signer, dummy_signer},
};

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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeParameters {
    #[serde(default = "node_defaults::default_wave_length")]
    pub wave_length: RoundNumber,
    #[serde(default = "node_defaults::default_leader_timeout")]
    pub leader_timeout: Duration,
    #[serde(default = "node_defaults::default_max_block_size")]
    pub max_block_size: usize,
    #[serde(default = "node_defaults::default_leader_count")]
    pub leader_count: NonZeroUsize,
    #[serde(default = "node_defaults::default_enable_pipelining")]
    pub enable_pipelining: bool,
    #[serde(default = "node_defaults::default_enable_synchronizer")]
    pub enable_synchronizer: bool,
}

pub mod node_defaults {
    pub fn default_wave_length() -> super::RoundNumber {
        3
    }

    pub fn default_leader_timeout() -> std::time::Duration {
        std::time::Duration::from_secs(2)
    }

    pub fn default_max_block_size() -> usize {
        4 * 1024 * 1024
    }

    pub fn default_leader_count() -> super::NonZeroUsize {
        super::NonZeroUsize::new(2).unwrap()
    }

    pub fn default_enable_pipelining() -> bool {
        true
    }

    pub fn default_enable_synchronizer() -> bool {
        false
    }
}

impl Default for NodeParameters {
    fn default() -> Self {
        Self {
            wave_length: node_defaults::default_wave_length(),
            leader_timeout: node_defaults::default_leader_timeout(),
            max_block_size: node_defaults::default_max_block_size(),
            leader_count: node_defaults::default_leader_count(),
            enable_pipelining: node_defaults::default_enable_pipelining(),
            enable_synchronizer: node_defaults::default_enable_synchronizer(),
        }
    }
}

impl ImportExport for NodeParameters {}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct NodeIdentifier {
    pub public_key: PublicKey,
    pub network_address: SocketAddr,
    pub metrics_address: SocketAddr,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodePublicConfig {
    pub identifiers: Vec<NodeIdentifier>,
    pub parameters: NodeParameters,
}

impl NodePublicConfig {
    pub const DEFAULT_FILENAME: &'static str = "public-config.yaml";
    pub const PORT_OFFSET_FOR_TESTS: u16 = 1500;

    pub fn new_for_tests(committee_size: usize) -> Self {
        let keys = Signer::new_for_test(committee_size);
        let ips = vec![IpAddr::V4(Ipv4Addr::LOCALHOST); committee_size];
        let benchmark_port_offset = ips.len() as u16;
        let mut identifiers = Vec::new();
        for (i, (ip, key)) in ips.into_iter().zip(keys.into_iter()).enumerate() {
            let public_key = key.public_key();
            let network_port = Self::PORT_OFFSET_FOR_TESTS + i as u16;
            let metrics_port = benchmark_port_offset + network_port;
            let network_address = SocketAddr::new(ip, network_port);
            let metrics_address = SocketAddr::new(ip, metrics_port);
            identifiers.push(NodeIdentifier {
                public_key,
                network_address,
                metrics_address,
            });
        }

        Self {
            identifiers,
            parameters: NodeParameters::default(),
        }
    }

    pub fn new_for_benchmarks(ips: Vec<IpAddr>, node_parameters: Option<NodeParameters>) -> Self {
        let default_with_ips = Self::new_for_tests(ips.len()).with_ips(ips);
        Self {
            identifiers: default_with_ips.identifiers,
            parameters: node_parameters.unwrap_or_default(),
        }
    }

    pub fn with_ips(mut self, ips: Vec<IpAddr>) -> Self {
        for (id, ip) in self.identifiers.iter_mut().zip(ips) {
            id.network_address.set_ip(ip);
            id.metrics_address.set_ip(ip);
        }
        self
    }

    pub fn with_port_offset(mut self, port_offset: u16) -> Self {
        for id in self.identifiers.iter_mut() {
            id.network_address
                .set_port(id.network_address.port() + port_offset);
            id.metrics_address
                .set_port(id.metrics_address.port() + port_offset);
        }
        self
    }

    /// Return all network addresses (including our own) in the order of the authority index.
    pub fn all_network_addresses(&self) -> impl Iterator<Item = SocketAddr> + '_ {
        self.identifiers.iter().map(|id| id.network_address)
    }

    /// Return all metric addresses (including our own) in the order of the authority index.
    pub fn all_metric_addresses(&self) -> impl Iterator<Item = SocketAddr> + '_ {
        self.identifiers.iter().map(|id| id.metrics_address)
    }

    pub fn network_address(&self, authority: Authority) -> Option<SocketAddr> {
        self.identifiers
            .get(authority.index())
            .map(|id| id.network_address)
    }

    pub fn metrics_address(&self, authority: Authority) -> Option<SocketAddr> {
        self.identifiers
            .get(authority.index())
            .map(|id| id.metrics_address)
    }
}

impl ImportExport for NodePublicConfig {}

#[derive(Serialize, Deserialize)]
pub struct NodePrivateConfig {
    authority: Authority,
    pub keypair: Signer,
    pub storage_path: PathBuf,
}

impl NodePrivateConfig {
    pub fn new_for_tests(index: Authority) -> Self {
        Self {
            authority: index,
            keypair: dummy_signer(),
            storage_path: PathBuf::from("storage"),
        }
    }

    pub fn new_for_benchmarks(working_dir: &Path, committee_size: usize) -> Vec<Self> {
        Signer::new_for_test(committee_size)
            .into_iter()
            .enumerate()
            .map(|(i, keypair)| {
                let authority = Authority::from(i);
                let path = working_dir.join(NodePrivateConfig::default_storage_path(authority));
                Self {
                    authority,
                    keypair,
                    storage_path: path,
                }
            })
            .collect()
    }

    pub fn default_filename(authority: Authority) -> PathBuf {
        format!("private-config-{authority}.yaml").into()
    }

    pub fn default_storage_path(authority: Authority) -> PathBuf {
        format!("storage-{authority}").into()
    }

    pub fn wal(&self) -> PathBuf {
        self.storage_path.join("wal")
    }
}

impl ImportExport for NodePrivateConfig {}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ClientParameters {
    /// The number of transactions to send to the network per second.
    #[serde(default = "client_defaults::default_load")]
    pub load: usize,
    /// The size of transactions to send to the network in bytes.
    #[serde(default = "client_defaults::default_transaction_size")]
    pub transaction_size: usize,
    /// The initial delay before starting to send transactions.
    #[serde(default = "client_defaults::default_initial_delay")]
    pub initial_delay: Duration,
}

mod client_defaults {
    use super::Duration;

    pub fn default_load() -> usize {
        10
    }

    pub fn default_transaction_size() -> usize {
        512
    }

    pub fn default_initial_delay() -> Duration {
        Duration::from_secs(30)
    }
}

impl Default for ClientParameters {
    fn default() -> Self {
        Self {
            load: client_defaults::default_load(),
            transaction_size: client_defaults::default_transaction_size(),
            initial_delay: client_defaults::default_initial_delay(),
        }
    }
}

impl ImportExport for ClientParameters {}
