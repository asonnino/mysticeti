// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroUsize;

use dag::committee::Stake;

use super::{LeaderElection, Protocol};

impl Protocol {
    /// Blue Bottle (partially synchronous variant)
    ///
    /// "BlueBottle: Fast and Robust Blockchains through Subsystem Specialization"
    /// <https://sonnino.com/papers/bluebottle.pdf>
    pub fn blue_bottle_partially_synchronous(
        total_stake: Stake,
        leader_count: NonZeroUsize,
    ) -> Self {
        let strong_quorum = 4 * total_stake / 5 + 1;
        let weak_quorum = 2 * total_stake / 5 + 1;
        Self {
            direct_commit_quorum: strong_quorum,
            direct_skip_quorum: strong_quorum,
            certificate_quorum: strong_quorum,
            quorum_threshold: strong_quorum,
            fast_path: None,
            anchor_link_size: weak_quorum,
            wave_length: 2,
            merged_certificates: true,
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
        ConsensusProtocol::BlueBottlePartiallySynchronous { leader_count }
    });
}
