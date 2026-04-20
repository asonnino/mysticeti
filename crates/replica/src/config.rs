// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use consensus::protocol::ConsensusProtocol;
use dag::{
    authority::Authority,
    committee::{AuthorityInfo, Committee},
    config::{DagParameters, ImportExport, ReplicaIdentifier},
    crypto::Signer,
};
use rand::{SeedableRng, rngs::StdRng};
use serde::{Deserialize, Serialize};

/// Tunable knobs for a replica: dag-level + consensus protocol.
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct ReplicaParameters {
    #[serde(default)]
    pub dag: DagParameters,
    #[serde(default)]
    pub consensus: ConsensusProtocol,
}

impl ImportExport for ReplicaParameters {}

/// Public-facing replica config: tunable parameters and committee identities.
#[derive(Serialize, Deserialize, Clone)]
pub struct PublicReplicaConfig {
    pub parameters: ReplicaParameters,
    pub identifiers: Vec<ReplicaIdentifier>,
}

impl PublicReplicaConfig {
    pub const DEFAULT_FILENAME: &'static str = "public-replica-config.yaml";
    pub const PORT_OFFSET_FOR_TESTS: u16 = 1500;

    pub fn new_for_tests(committee_size: usize) -> Self {
        let mut rng = StdRng::seed_from_u64(0);
        let ips = vec![IpAddr::V4(Ipv4Addr::LOCALHOST); committee_size];
        let benchmark_port_offset = ips.len() as u16;
        let mut identifiers = Vec::new();
        for (i, ip) in ips.into_iter().enumerate() {
            let key = Signer::new(&mut rng);
            let public_key = key.public_key();
            let network_port = Self::PORT_OFFSET_FOR_TESTS + i as u16;
            let metrics_port = benchmark_port_offset + network_port;
            let network_address = SocketAddr::new(ip, network_port);
            let metrics_address = SocketAddr::new(ip, metrics_port);
            identifiers.push(ReplicaIdentifier {
                public_key,
                stake: 1,
                network_address,
                metrics_address,
            });
        }

        Self {
            parameters: ReplicaParameters::default(),
            identifiers,
        }
    }

    /// Build the `Committee` implied by this config's identifiers.
    pub fn committee(&self) -> Arc<Committee> {
        Committee::new(
            self.identifiers
                .iter()
                .map(|id| AuthorityInfo::new(id.stake, id.public_key.clone()))
                .collect(),
        )
    }

    pub fn new_for_benchmarks(ips: Vec<IpAddr>) -> Self {
        Self::new_for_tests(ips.len()).with_ips(ips)
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

    pub fn with_parameters(mut self, parameters: ReplicaParameters) -> Self {
        self.parameters = parameters;
        self
    }

    /// Return all network addresses (including our own) in authority-index order.
    pub fn all_network_addresses(&self) -> impl Iterator<Item = SocketAddr> + '_ {
        self.identifiers.iter().map(|id| id.network_address)
    }

    /// Return all metric addresses (including our own) in authority-index order.
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

impl ImportExport for PublicReplicaConfig {}

#[derive(Serialize, Deserialize)]
pub struct PrivateReplicaConfig {
    authority: Authority,
    pub keypair: Signer,
    pub storage_path: PathBuf,
}

impl PrivateReplicaConfig {
    pub fn new_for_benchmarks(working_dir: &Path, committee_size: usize) -> Vec<Self> {
        let mut rng = StdRng::seed_from_u64(0);
        (0..committee_size)
            .map(|i| {
                let keypair = Signer::new(&mut rng);
                let authority = Authority::from(i);
                let path = working_dir.join(PrivateReplicaConfig::default_storage_path(authority));
                Self {
                    authority,
                    keypair,
                    storage_path: path,
                }
            })
            .collect()
    }

    pub fn default_filename(authority: Authority) -> PathBuf {
        format!("private-replica-config-{authority}.yaml").into()
    }

    pub fn default_storage_path(authority: Authority) -> PathBuf {
        format!("storage-{authority}").into()
    }

    pub fn wal(&self) -> PathBuf {
        self.storage_path.join("wal")
    }
}

impl ImportExport for PrivateReplicaConfig {}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LoadGeneratorConfig {
    /// The number of transactions to send to the network per second.
    #[serde(default = "load_generator_defaults::default_load")]
    pub load: usize,
    /// The size of transactions to send to the network in bytes.
    #[serde(default = "load_generator_defaults::default_transaction_size")]
    pub transaction_size: usize,
    /// The initial delay before starting to send transactions.
    #[serde(default = "load_generator_defaults::default_initial_delay")]
    pub initial_delay: Duration,
}

mod load_generator_defaults {
    use std::time::Duration;

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

impl Default for LoadGeneratorConfig {
    fn default() -> Self {
        Self {
            load: load_generator_defaults::default_load(),
            transaction_size: load_generator_defaults::default_transaction_size(),
            initial_delay: load_generator_defaults::default_initial_delay(),
        }
    }
}

impl ImportExport for LoadGeneratorConfig {}
