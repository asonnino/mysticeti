// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Borrow, collections::HashSet, ops::Range, sync::Arc};

use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::{
    config::ImportExport,
    crypto::{PublicKey, Signer, dummy_public_key},
    types::{AuthorityIndex, AuthoritySet, Stake},
};

#[derive(Serialize, Deserialize)]
pub struct Committee {
    authorities: Vec<Authority>,
    total_stake: Stake,
}

impl Committee {
    pub const DEFAULT_FILENAME: &'static str = "committee.yaml";

    pub fn new_test(stake: Vec<Stake>) -> Arc<Self> {
        let authorities = stake.into_iter().map(Authority::test_from_stake).collect();
        Self::new(authorities)
    }

    pub fn new(authorities: Vec<Authority>) -> Arc<Self> {
        // todo - check duplicate public keys
        // Ensure the list is not empty
        assert!(!authorities.is_empty());

        // Ensure all stakes are positive
        assert!(authorities.iter().all(|a| a.stake() > 0));
        // For now AuthoritySet only supports up to 128 authorities
        assert!(authorities.len() <= 128);

        let mut total_stake: Stake = 0;
        for a in authorities.iter() {
            total_stake = total_stake
                .checked_add(a.stake())
                .expect("Total stake overflow");
        }
        Arc::new(Committee {
            authorities,
            total_stake,
        })
    }

    pub fn total_stake(&self) -> Stake {
        self.total_stake
    }

    pub fn get_stake(&self, authority: AuthorityIndex) -> Option<Stake> {
        self.authorities
            .get(authority as usize)
            .map(Authority::stake)
    }

    pub fn get_public_key(&self, authority: AuthorityIndex) -> Option<&PublicKey> {
        self.authorities
            .get(authority as usize)
            .map(Authority::public_key)
    }

    pub fn known_authority(&self, authority: AuthorityIndex) -> bool {
        authority < self.len() as AuthorityIndex
    }

    pub fn authorities(&self) -> Range<AuthorityIndex> {
        0u64..(self.authorities.len() as AuthorityIndex)
    }

    pub fn get_total_stake<A: Borrow<AuthorityIndex>>(&self, authorities: &HashSet<A>) -> Stake {
        let mut total_stake = 0;
        for authority in authorities {
            total_stake += self.authorities[*authority.borrow() as usize].stake();
        }
        total_stake
    }

    pub fn random_authority(&self, rng: &mut impl Rng) -> AuthorityIndex {
        rng.gen_range(self.authorities())
    }

    pub fn len(&self) -> usize {
        self.authorities.len()
    }

    pub fn is_empty(&self) -> bool {
        self.authorities.is_empty()
    }

    pub fn new_for_benchmarks(committee_size: usize) -> Arc<Self> {
        Self::new(
            Signer::new_for_test(committee_size)
                .into_iter()
                .map(|keypair| Authority {
                    stake: 1,
                    public_key: keypair.public_key(),
                })
                .collect(),
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Authority {
    stake: Stake,
    public_key: PublicKey,
}

impl Authority {
    pub fn test_from_stake(stake: Stake) -> Self {
        Self {
            stake,
            public_key: dummy_public_key(),
        }
    }

    pub fn stake(&self) -> Stake {
        self.stake
    }

    pub fn public_key(&self) -> &PublicKey {
        &self.public_key
    }
}

impl ImportExport for Committee {}

pub struct StakeAggregator {
    votes: AuthoritySet,
    stake: Stake,
    threshold: Stake,
}

impl StakeAggregator {
    pub fn new(threshold: Stake) -> Self {
        Self {
            votes: Default::default(),
            stake: 0,
            threshold,
        }
    }

    pub fn add(&mut self, vote: AuthorityIndex, committee: &Committee) -> bool {
        let stake = committee.get_stake(vote).expect("Authority not found");
        if self.votes.insert(vote) {
            self.stake += stake;
        }
        self.stake >= self.threshold
    }

    pub fn clear(&mut self) {
        self.votes.clear();
        self.stake = 0;
    }

    pub fn voters(&self) -> impl Iterator<Item = AuthorityIndex> + '_ {
        self.votes.present()
    }
}
