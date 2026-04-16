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
    pub number_of_leaders: usize,
    /// Whether the protocol commits one leader per round.
    pub pipeline: bool,
}

impl Protocol {
    pub fn mysticeti(total_stake: Stake, number_of_leaders: usize) -> Self {
        let quorum = 2 * total_stake / 3 + 1;
        Self {
            strong_quorum: quorum,
            weak_quorum: quorum,
            wave_length: 3,
            number_of_leaders,
            pipeline: true,
        }
    }

    pub fn odontoceti(total_stake: Stake, number_of_leaders: usize) -> Self {
        Self {
            strong_quorum: 4 * total_stake / 5 + 1,
            weak_quorum: 2 * total_stake / 5 + 1,
            wave_length: 2,
            number_of_leaders,
            pipeline: true,
        }
    }
}
