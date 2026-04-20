// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Borrow, collections::HashSet, sync::Arc};

use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::{
    authority::{Authority, AuthoritySet},
    crypto::PublicKey,
};

pub type Stake = u64;

#[derive(Serialize, Deserialize)]
pub struct Committee {
    authorities: Vec<AuthorityInfo>,
    total_stake: Stake,
}

impl Committee {
    pub fn new_test(stake: Vec<Stake>) -> Arc<Self> {
        let authorities = stake
            .into_iter()
            .map(AuthorityInfo::test_from_stake)
            .collect();
        Self::new(authorities)
    }

    pub fn new(authorities: Vec<AuthorityInfo>) -> Arc<Self> {
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

    pub fn get_stake(&self, authority: Authority) -> Option<Stake> {
        self.authorities
            .get(authority.index())
            .map(AuthorityInfo::stake)
    }

    pub fn get_public_key(&self, authority: Authority) -> Option<&PublicKey> {
        self.authorities
            .get(authority.index())
            .map(AuthorityInfo::public_key)
    }

    pub fn known_authority(&self, authority: Authority) -> bool {
        authority.index() < self.len()
    }

    pub fn authorities(&self) -> impl Iterator<Item = Authority> + '_ {
        (0..self.authorities.len()).map(|i| Authority::new(i as u64))
    }

    pub fn get_total_stake<A: Borrow<Authority>>(&self, authorities: &HashSet<A>) -> Stake {
        let mut total_stake = 0;
        for authority in authorities {
            total_stake += self.authorities[authority.borrow().index()].stake();
        }
        total_stake
    }

    pub fn random_authority(&self, rng: &mut impl Rng) -> Authority {
        let index = rng.gen_range(0..self.len());
        Authority::new(index as u64)
    }

    pub fn len(&self) -> usize {
        self.authorities.len()
    }

    pub fn is_empty(&self) -> bool {
        self.authorities.is_empty()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthorityInfo {
    stake: Stake,
    public_key: PublicKey,
}

impl AuthorityInfo {
    pub fn new(stake: Stake, public_key: PublicKey) -> Self {
        Self { stake, public_key }
    }

    pub fn test_from_stake(stake: Stake) -> Self {
        Self {
            stake,
            public_key: PublicKey::dummy(),
        }
    }

    pub fn stake(&self) -> Stake {
        self.stake
    }

    pub fn public_key(&self) -> &PublicKey {
        &self.public_key
    }
}

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

    pub fn add(&mut self, vote: Authority, committee: &Committee) -> bool {
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

    pub fn voters(&self) -> impl Iterator<Item = Authority> + '_ {
        self.votes.present()
    }
}
