// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs,
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    crypto::Signer,
    types::{AuthorityIndex, PublicKey, RoundNumber},
};

pub trait ImportExport: Serialize + DeserializeOwned {
    fn load<P: AsRef<Path>>(path: P) -> Result<Self, io::Error> {
        let content = fs::read_to_string(&path)?;
        let object =
            serde_yaml::from_str(&content).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(object)
    }

    fn print<P: AsRef<Path>>(&self, path: P) -> Result<(), io::Error> {
        let content =
            serde_yaml::to_string(self).expect("Failed to serialize object to YAML string");
        fs::write(&path, content)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NodeParameters {
    #[serde(default = "node_defaults::default_wave_length")]
    pub wave_length: RoundNumber,
    #[serde(default = "node_defaults::default_leader_timeout")]
    pub leader_timeout: Duration,
    #[serde(default = "node_defaults::default_rounds_in_epoch")]
    pub rounds_in_epoch: RoundNumber,
    #[serde(default = "node_defaults::default_shutdown_grace_period")]
    pub shutdown_grace_period: Duration,
    #[serde(default = "node_defaults::default_number_of_leaders")]
    pub number_of_leaders: usize,
    #[serde(default = "node_defaults::default_enable_pipelining")]
    pub enable_pipelining: bool,
    #[serde(default = "node_defaults::default_consensus_only")]
    pub consensus_only: bool,
}

pub mod node_defaults {
    pub fn default_wave_length() -> super::RoundNumber {
        3
    }

    pub fn default_leader_timeout() -> std::time::Duration {
        std::time::Duration::from_secs(2)
    }

    pub fn default_rounds_in_epoch() -> super::RoundNumber {
        3_600_000
    }

    pub fn default_shutdown_grace_period() -> std::time::Duration {
        std::time::Duration::from_secs(2)
    }

    pub fn default_number_of_leaders() -> usize {
        2
    }

    pub fn default_enable_pipelining() -> bool {
        true
    }

    pub fn default_consensus_only() -> bool {
        true
    }
}

impl Default for NodeParameters {
    fn default() -> Self {
        Self {
            wave_length: node_defaults::default_wave_length(),
            leader_timeout: node_defaults::default_leader_timeout(),
            rounds_in_epoch: node_defaults::default_rounds_in_epoch(),
            shutdown_grace_period: node_defaults::default_shutdown_grace_period(),
            number_of_leaders: node_defaults::default_number_of_leaders(),
            enable_pipelining: node_defaults::default_enable_pipelining(),
            consensus_only: node_defaults::default_consensus_only(),
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

    pub fn network_address(&self, authority: AuthorityIndex) -> Option<SocketAddr> {
        self.identifiers
            .get(authority as usize)
            .map(|id| id.network_address)
    }

    pub fn metrics_address(&self, authority: AuthorityIndex) -> Option<SocketAddr> {
        self.identifiers
            .get(authority as usize)
            .map(|id| id.metrics_address)
    }
}

impl ImportExport for NodePublicConfig {}

#[derive(Serialize, Deserialize)]
pub struct NodePrivateConfig {
    authority: AuthorityIndex,
    pub keypair: Signer,
    pub storage_path: PathBuf,
}

impl NodePrivateConfig {
    pub fn new_for_benchmarks(working_dir: &Path, committee_size: usize) -> Vec<Self> {
        Signer::new_for_test(committee_size)
            .into_iter()
            .enumerate()
            .map(|(i, keypair)| {
                let authority = i as AuthorityIndex;
                let path = working_dir.join(NodePrivateConfig::default_storage_path(authority));
                Self {
                    authority,
                    keypair,
                    storage_path: path,
                }
            })
            .collect()
    }

    pub fn default_filename(authority: AuthorityIndex) -> PathBuf {
        format!("private-config-{authority}.yaml").into()
    }

    pub fn default_storage_path(authority: AuthorityIndex) -> PathBuf {
        format!("storage-{authority}").into()
    }

    pub fn certified_transactions_log(&self) -> PathBuf {
        self.storage_path.join("certified.txt")
    }

    pub fn committed_transactions_log(&self) -> PathBuf {
        self.storage_path.join("committed.txt")
    }

    pub fn wal(&self) -> PathBuf {
        self.storage_path.join("wal")
    }
}

impl ImportExport for NodePrivateConfig {}

#[derive(Serialize, Deserialize, Clone)]
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
