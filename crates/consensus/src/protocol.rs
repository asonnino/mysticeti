// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::num::NonZeroUsize;

use dag::types::{RoundNumber, Stake};

/// Protocol-specific parameters for the consensus committer.
pub struct Protocol {
    /// The strong quorum threshold to directly commit a leader.
    pub strong_quorum: Stake,
    /// The weak quorum to indirectly commit a leader.
    pub weak_quorum: Stake,
    /// The number of rounds to commit a leader.
    pub wave_length: RoundNumber,
    /// The number of leaders per round.
    pub leader_count: NonZeroUsize,
    /// Whether the protocol commits one leader per round.
    pub pipeline: bool,
    /// Whether the protocol should wait for leader blocks before proposing.
    pub leader_wait: bool,
}

impl Protocol {
    /// Cordial Miners (partially synchronous version)
    ///
    /// "Cordial Miners: Fast and Efficient Consensus for Every Eventuality" DISC 2023
    /// <https://arxiv.org/abs/2205.09174>
    pub fn cordial_miners_partially_synchronous(total_stake: Stake) -> Self {
        let quorum = 2 * total_stake / 3 + 1;
        Self {
            strong_quorum: quorum,
            weak_quorum: quorum,
            wave_length: 3,
            leader_count: NonZeroUsize::new(1).unwrap(),
            pipeline: false,
            leader_wait: true,
        }
    }

    /// Cordial Miners (asynchronous version)
    ///
    /// "Cordial Miners: Fast and Efficient Consensus for Every Eventuality" DISC 2023
    /// <https://arxiv.org/abs/2205.09174>
    pub fn cordial_miners_asynchronous(total_stake: Stake) -> Self {
        let quorum = 2 * total_stake / 3 + 1;
        Self {
            strong_quorum: quorum,
            weak_quorum: quorum,
            wave_length: 5,
            leader_count: NonZeroUsize::new(1).unwrap(),
            pipeline: false,
            leader_wait: false,
        }
    }

    /// Mysticeti
    ///
    /// "Mysticeti: Reaching the Latency Limits with Uncertified DAGs" NDSS 2025.
    /// <https://sonnino.com/papers/mysticeti.pdf>
    pub fn mysticeti(total_stake: Stake, leader_count: NonZeroUsize) -> Self {
        let quorum = 2 * total_stake / 3 + 1;
        Self {
            strong_quorum: quorum,
            weak_quorum: quorum,
            wave_length: 3,
            leader_count,
            pipeline: true,
            leader_wait: true,
        }
    }

    /// Odontoceti
    ///
    /// "BlueBottle: Fast and Robust Blockchains through Subsystem Specialization"
    /// <https://sonnino.com/papers/bluebottle.pdf>
    pub fn odontoceti(total_stake: Stake, leader_count: NonZeroUsize) -> Self {
        Self {
            strong_quorum: 4 * total_stake / 5 + 1,
            weak_quorum: 2 * total_stake / 5 + 1,
            wave_length: 2,
            leader_count,
            pipeline: true,
            leader_wait: true,
        }
    }

    /// Mahi-Mahi
    ///
    /// "Mahi-Mahi: Low-Latency Asynchronous BFT DAG-Based Consensus" ICDCS 2025.
    /// <https://sonnino.com/papers/mahi-mahi.pdf>
    pub fn mahi_mahi(
        total_stake: Stake,
        leader_count: NonZeroUsize,
        wave_length: RoundNumber,
    ) -> Self {
        assert!(
            wave_length == 4 || wave_length == 5,
            "MahiMahi requires wave_length of 4 or 5"
        );
        let quorum = 2 * total_stake / 3 + 1;
        Self {
            strong_quorum: quorum,
            weak_quorum: quorum,
            wave_length,
            leader_count,
            pipeline: true,
            leader_wait: false,
        }
    }
}
