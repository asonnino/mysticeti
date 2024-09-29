// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use crate::{
    committee::Authority,
    config::NodeIdentifier,
    crypto::{PublicKey, Signer},
    types::{AuthorityIndex, RoundNumber, Stake},
};

#[derive(Serialize, Deserialize, Debug)]
pub struct AuxiliaryCommittee {
    authorities: HashMap<AuthorityIndex, Authority>,
}

impl Default for AuxiliaryCommittee {
    fn default() -> Self {
        Self {
            authorities: HashMap::new(),
        }
    }
}

impl AuxiliaryCommittee {
    pub const AUX_AUTHORITY_INDEX_OFFSET: usize = 1000;

    pub fn new(authorities: HashMap<AuthorityIndex, Authority>) -> Arc<Self> {
        Arc::new(Self { authorities })
    }

    pub fn new_for_benchmarks(aux_committee_size: usize) -> Arc<Self> {
        let authorities =
            Signer::new_for_test_with_offset(aux_committee_size, Self::AUX_AUTHORITY_INDEX_OFFSET)
                .into_iter()
                .enumerate()
                .map(|(i, keypair)| {
                    let index = (i + Self::AUX_AUTHORITY_INDEX_OFFSET) as AuthorityIndex;
                    let authority = Authority {
                        stake: 1,
                        public_key: keypair.public_key(),
                    };
                    (index, authority)
                })
                .collect();

        Arc::new(Self { authorities })
    }

    pub fn get_public_key(&self, authority: AuthorityIndex) -> Option<&PublicKey> {
        self.authorities.get(&authority).map(Authority::public_key)
    }

    pub fn get_stake(&self, authority: AuthorityIndex) -> Option<Stake> {
        self.authorities.get(&authority).map(Authority::stake)
    }

    pub fn exists(&self, authority: AuthorityIndex) -> bool {
        self.authorities.get(&authority).is_some()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AuxNodeParameters {
    /// The minimum auxiliary stake required for liveness. When core validators include
    /// blocks from auxiliary validators, they must include at least this stake threshold.
    pub liveness_threshold: Stake,
    /// The period (in rounds) after which blocks must include at least `liveness_threshold` auxiliary blocks.
    /// Note that Mysticeti mainnet processes about 13-15 rounds per second. So if we want to include auxiliary blocks
    /// every 10 seconds, it enough to set `inclusion_period = 150`.
    pub inclusion_period: RoundNumber,
    /// Maximum block size (in bytes) for auxiliary blocks.
    pub max_block_size: usize,
}

impl AuxNodeParameters {
    pub fn new_for_tests() -> Self {
        Self {
            liveness_threshold: 1,
            inclusion_period: 150,
            max_block_size: 1024 * 50,
        }
    }

    pub fn inclusion_round(&self, round: RoundNumber) -> bool {
        round % self.inclusion_period == 0
    }
}

impl Default for AuxNodeParameters {
    fn default() -> Self {
        Self {
            liveness_threshold: 0,
            inclusion_period: 150,
            max_block_size: 1024 * 50,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AuxNodePublicConfig {
    pub identifiers: HashMap<AuthorityIndex, NodeIdentifier>,
    pub parameters: AuxNodeParameters,
}

impl AuxNodePublicConfig {
    pub const DEFAULT_FILENAME: &'static str = "aux-public-config.yaml";
    pub const PORT_OFFSET_FOR_TESTS: u16 = 2500;

    pub fn new_for_tests(aux_committee_size: usize) -> Self {
        let aux_committee = AuxiliaryCommittee::new_for_benchmarks(aux_committee_size);
        let ips = vec![IpAddr::V4(Ipv4Addr::LOCALHOST); aux_committee_size];
        let mut identifiers = HashMap::new();
        for (ip, (index, authority)) in ips.into_iter().zip(aux_committee.authorities.iter()) {
            let metrics_port = Self::PORT_OFFSET_FOR_TESTS + *index as u16;
            let metrics_address = SocketAddr::new(ip, metrics_port);
            let identifies = NodeIdentifier {
                public_key: authority.public_key().clone(),
                network_address: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 1), // Unused
                metrics_address,
            };
            identifiers.insert(*index, identifies);
        }

        Self {
            identifiers,
            parameters: AuxNodeParameters::default(),
        }
    }

    pub fn with_ips(mut self, ips: Vec<IpAddr>) -> Self {
        for (id, ip) in self.identifiers.values_mut().zip(ips) {
            id.metrics_address.set_ip(ip);
        }
        self
    }

    pub fn new_for_benchmarks(
        ips: Vec<IpAddr>,
        aux_node_parameters: Option<AuxNodeParameters>,
    ) -> Self {
        let default_with_ips = Self::new_for_tests(ips.len()).with_ips(ips);
        Self {
            identifiers: default_with_ips.identifiers,
            parameters: aux_node_parameters.unwrap_or_default(),
        }
    }

    pub fn metrics_address(&self, authority: AuthorityIndex) -> Option<SocketAddr> {
        self.identifiers
            .get(&authority)
            .map(|id| id.metrics_address)
    }
}
