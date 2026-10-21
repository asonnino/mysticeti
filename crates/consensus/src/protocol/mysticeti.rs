// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroUsize;

use dag::committee::Stake;

use super::{LeaderElection, Protocol};

impl Protocol {
    /// Mysticeti
    ///
    /// "Mysticeti: Reaching the Latency Limits with Uncertified DAGs" NDSS 2025.
    /// <https://sonnino.com/papers/mysticeti.pdf>
    pub fn mysticeti(total_stake: Stake, leader_count: NonZeroUsize) -> Self {
        let quorum = 2 * total_stake / 3 + 1;
        Self {
            direct_commit_quorum: quorum,
            direct_skip_quorum: quorum,
            certificate_quorum: quorum,
            quorum_threshold: quorum,
            fast_path: None,
            anchor_link_size: 1,
            wave_length: 3,
            merged_certificates: false,
            union_certificates: false,
            leader_count,
            pipeline: true,
            leader_wait: true,
            require_crypto: true,
            leader_election: LeaderElection::RoundRobin,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{protocol::ConsensusProtocol, test_scenarios::scenario_tests};

    scenario_tests!(baseline, sizes: [n4, n20], |leader_count| {
        ConsensusProtocol::Mysticeti { leader_count }
    });
}
