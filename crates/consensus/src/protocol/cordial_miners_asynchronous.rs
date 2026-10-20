// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroUsize;

use dag::committee::Stake;

use super::Protocol;

impl Protocol {
    /// Cordial Miners (asynchronous version)
    ///
    /// "Cordial Miners: Fast and Efficient Consensus for Every Eventuality" DISC 2023
    /// <https://arxiv.org/abs/2205.09174>
    pub fn cordial_miners_asynchronous(total_stake: Stake) -> Self {
        let quorum = 2 * total_stake / 3 + 1;
        Self {
            direct_commit_quorum: quorum,
            direct_skip_quorum: total_stake,
            certificate_quorum: quorum,
            quorum_threshold: quorum,
            fast_path: None,
            anchor_link_size: 1,
            wave_length: 5,
            merged_certificates: false,
            leader_count: NonZeroUsize::new(1).unwrap(),
            pipeline: false,
            leader_wait: false,
            require_crypto: true,
        }
    }
}
