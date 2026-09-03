// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dag::{authority::Authority, block::RoundNumber};

/// Shifts the fake coin's schedule off the sync round-robin.
const FAKE_COIN_SEED: RoundNumber = 5;

/// Determines the leader for each round. Different
/// consensus protocols may use different election
/// strategies; this struct encapsulates that choice.
pub struct LeaderElector {
    committee_len: usize,
}

impl LeaderElector {
    pub fn new(committee_len: usize) -> Self {
        Self { committee_len }
    }

    /// Round-robin leader election.
    pub fn elect_leader(&self, round: RoundNumber) -> Authority {
        Authority::new(round % self.committee_len as u64)
    }

    /// Stand-in for the common coin: shifted round-robin. Fully predictable —
    /// real unpredictability requires the threshold coin, which we assume;
    /// adversary models must not consult it.
    pub fn elect_fake_coin_leader(&self, round: RoundNumber) -> Authority {
        self.elect_leader(round + FAKE_COIN_SEED)
    }
}
