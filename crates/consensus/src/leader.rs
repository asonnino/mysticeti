// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dag::types::{AuthorityIndex, RoundNumber};

/// Determines the leader for each round. Different
/// consensus protocols may use different election
/// strategies; this struct encapsulates that choice.
pub(crate) struct LeaderElector {
    committee_len: usize,
}

impl LeaderElector {
    pub(crate) fn new(committee_len: usize) -> Self {
        Self { committee_len }
    }

    /// Round-robin leader election.
    pub(crate) fn elect_leader(&self, round: RoundNumber) -> AuthorityIndex {
        (round % self.committee_len as u64) as AuthorityIndex
    }
}
