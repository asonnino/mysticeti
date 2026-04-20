// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::cmp::Ordering;

use crate::{
    block::{Block, BlockReference, RoundNumber},
    committee::Stake,
    committee::{Committee, StakeAggregator},
};

/// A block is threshold clock valid if:
/// - all included blocks have a round number lower
///   than the block round number.
/// - the set of authorities with blocks included has a
///   quorum in the current committee.
pub fn threshold_clock_valid_non_genesis(
    block: &Block,
    committee: &Committee,
    quorum_threshold: Stake,
) -> bool {
    let round_number = block.reference().round;
    assert!(round_number > 0);

    for include in block.includes() {
        if include.round >= block.reference().round {
            return false;
        }
    }

    let mut aggregator = StakeAggregator::new(quorum_threshold);
    let mut is_quorum = false;
    for include in block.includes() {
        if include.round == round_number - 1 {
            is_quorum = aggregator.add(include.authority, committee);
        }
    }

    is_quorum
}

pub struct ThresholdClockAggregator {
    aggregator: StakeAggregator,
    quorum_threshold: Stake,
    round: RoundNumber,
}

impl ThresholdClockAggregator {
    pub fn new(round: RoundNumber, quorum_threshold: Stake) -> Self {
        Self {
            aggregator: StakeAggregator::new(quorum_threshold),
            quorum_threshold,
            round,
        }
    }

    pub fn add_block(&mut self, block: BlockReference, committee: &Committee) {
        match block.round.cmp(&self.round) {
            Ordering::Less => {}
            Ordering::Greater => {
                self.aggregator = StakeAggregator::new(self.quorum_threshold);
                self.aggregator.add(block.authority, committee);
                self.round = block.round;
            }
            Ordering::Equal => {
                if self.aggregator.add(block.authority, committee) {
                    self.aggregator = StakeAggregator::new(self.quorum_threshold);
                    self.round = block.round + 1;
                }
            }
        }
        if block.round > self.round {
            self.round = block.round;
        }
    }

    pub fn get_round(&self) -> RoundNumber {
        self.round
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::test::Dag;

    #[test]
    fn test_threshold_clock_valid() {
        let committee = Committee::new_test(vec![1, 1, 1, 1]);
        let quorum = 2 * committee.total_stake() / 3 + 1;
        assert!(!threshold_clock_valid_non_genesis(
            &Dag::draw_block("A1:[]"),
            &committee,
            quorum,
        ));
        assert!(!threshold_clock_valid_non_genesis(
            &Dag::draw_block("A1:[A0, B0]"),
            &committee,
            quorum,
        ));
        assert!(threshold_clock_valid_non_genesis(
            &Dag::draw_block("A1:[A0, B0, C0]"),
            &committee,
            quorum,
        ));
        assert!(threshold_clock_valid_non_genesis(
            &Dag::draw_block("A1:[A0, B0, C0, D0]"),
            &committee,
            quorum,
        ));
        assert!(!threshold_clock_valid_non_genesis(
            &Dag::draw_block("A2:[A1, B1, C0, D0]"),
            &committee,
            quorum,
        ));
        assert!(threshold_clock_valid_non_genesis(
            &Dag::draw_block("A2:[A1, B1, C1, D0]"),
            &committee,
            quorum,
        ));
    }

    #[test]
    fn test_threshold_clock_aggregator() {
        let committee = Committee::new_test(vec![1, 1, 1, 1]);
        let quorum = 2 * committee.total_stake() / 3 + 1;
        let mut aggregator = ThresholdClockAggregator::new(0, quorum);

        aggregator.add_block(BlockReference::new_test(0, 0), &committee);
        assert_eq!(aggregator.get_round(), 0);
        aggregator.add_block(BlockReference::new_test(0, 1), &committee);
        assert_eq!(aggregator.get_round(), 1);
        aggregator.add_block(BlockReference::new_test(1, 0), &committee);
        assert_eq!(aggregator.get_round(), 1);
        aggregator.add_block(BlockReference::new_test(1, 1), &committee);
        assert_eq!(aggregator.get_round(), 1);
        aggregator.add_block(BlockReference::new_test(2, 1), &committee);
        assert_eq!(aggregator.get_round(), 2);
        aggregator.add_block(BlockReference::new_test(3, 1), &committee);
        assert_eq!(aggregator.get_round(), 2);
    }
}
