// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{
    committee::Authority,
    crypto::{PublicKey, Signer},
    types::{AuthorityIndex, RoundNumber, Stake},
};

#[derive(Serialize, Deserialize)]
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

    pub fn new_for_benchmarks(committee_size: usize) -> Arc<Self> {
        let authorities =
            Signer::new_for_test_with_offset(committee_size, Self::AUX_AUTHORITY_INDEX_OFFSET)
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

#[derive(Serialize, Deserialize, Clone)]
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
