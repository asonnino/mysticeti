// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroUsize;

use dag::committee::Stake;

use super::{LeaderElection, Protocol};

impl Protocol {
    /// Nemo-Nemo
    ///
    /// "Finding Nemo-Nemo: CFT DAG-based Consensus in the WAN"
    /// <https://sonnino.com/papers/nemo-nemo.pdf>
    pub fn nemo_nemo(total_stake: Stake, leader_count: NonZeroUsize) -> Self {
        let quorum = total_stake / 2 + 1;
        Self {
            direct_commit_quorum: quorum,
            direct_skip_quorum: total_stake,
            certificate_quorum: quorum,
            quorum_threshold: quorum,
            fast_path: None,
            anchor_link_size: 1,
            wave_length: 2,
            merged_certificates: true,
            union_certificates: false,
            leader_count,
            pipeline: true,
            leader_wait: true,
            require_crypto: false,
            leader_election: LeaderElection::RoundRobin,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{protocol::ConsensusProtocol, test_scenarios::scenario_tests};

    scenario_tests!(baseline, sizes: [n4, n20], |leader_count| {
        ConsensusProtocol::NemoNemo { leader_count }
    });
}
