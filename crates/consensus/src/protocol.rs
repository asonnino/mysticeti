// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dag::types::{RoundNumber, Stake};

/// Protocol-specific parameters for the consensus committer.
pub struct Protocol {
    /// The strong quorum threshold to commit a leader.
    pub strong_quorum: Stake,
    pub weak_quorum: Stake,
    /// The number of rounds to commit a leader.
    pub wave_length: RoundNumber,
    /// The number of leaders per round.
    pub leader_count: usize,
    /// Whether the protocol commits one leader per round.
    pub pipeline: bool,
    /// Whether the DAG should wait for leader blocks
    /// before proposing. When false, the DAG waits for
    /// all authorities equally.
    pub leader_wait: bool,
}

impl Protocol {
    pub fn cordial_miners(total_stake: Stake) -> Self {
        let quorum = 2 * total_stake / 3 + 1;
        Self {
            strong_quorum: quorum,
            weak_quorum: quorum,
            wave_length: 3,
            leader_count: 1,
            pipeline: false,
            leader_wait: true,
        }
    }

    pub fn mysticeti(total_stake: Stake, leader_count: usize) -> Self {
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

    pub fn odontoceti(total_stake: Stake, leader_count: usize) -> Self {
        Self {
            strong_quorum: 4 * total_stake / 5 + 1,
            weak_quorum: 2 * total_stake / 5 + 1,
            wave_length: 2,
            leader_count,
            pipeline: true,
            leader_wait: true,
        }
    }

    pub fn mahi_mahi(total_stake: Stake, wave_length: RoundNumber) -> Self {
        assert!(
            wave_length == 4 || wave_length == 5,
            "MahiMahi requires wave_length of 4 or 5"
        );
        let quorum = 2 * total_stake / 3 + 1;
        Self {
            strong_quorum: quorum,
            weak_quorum: quorum,
            wave_length,
            leader_count: 1,
            pipeline: true,
            leader_wait: false,
        }
    }
}
