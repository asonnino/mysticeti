// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The decision rules of union certificates, which count votes across blocks.

use std::collections::HashMap;

use dag::{
    block::{Block, RoundNumber},
    committee::StakeAggregator,
    data::Data,
};

use super::BaseCommitter;
use crate::{union_certificate::union_direct_commit, wave::WaveNumber};

impl BaseCommitter {
    /// Whether the voting-round blocks in the causal history of `anchor` hold a
    /// `certificate_quorum` of distinct-author votes for `leader_block`.
    pub(super) fn enough_votes_linked(
        &self,
        anchor: &Data<Block>,
        leader_block: &Data<Block>,
        wave: WaveNumber,
    ) -> bool {
        let voting_blocks = self
            .block_reader
            .get_blocks_by_round(self.wave.voting_round(wave));
        let mut aggregator = StakeAggregator::new(self.certificate_quorum);
        voting_blocks.iter().any(|block| {
            self.block_reader.linked(anchor, block)
                && self.is_vote(block, leader_block)
                && aggregator.add(block.author(), &self.committee)
        })
    }

    /// Whether the decision-round blocks hold votes for `leader_block` that every later anchor is
    /// guaranteed to see (see [`union_direct_commit`]).
    pub(super) fn enough_union_support(
        &self,
        decision_round: RoundNumber,
        leader_block: &Data<Block>,
    ) -> bool {
        let voting_round = self
            .wave
            .voting_round(self.wave.number(leader_block.round()));

        // Many decision-round blocks reference the same voting-round block.
        let mut is_vote_by_block = HashMap::new();

        // One decision-round block per author: any choice is safe, so take the first.
        let mut authors = vec![];
        let mut vote_masks = vec![];
        for decision_block in self.block_reader.get_blocks_by_round(decision_round) {
            let author = decision_block.author().index();
            if authors.contains(&author) {
                continue;
            }
            let mut votes = 0u64;
            for reference in decision_block
                .includes()
                .iter()
                .filter(|reference| reference.round() == voting_round)
            {
                let is_vote = *is_vote_by_block.entry(*reference).or_insert_with(|| {
                    let voting_block = self
                        .block_reader
                        .get_block(*reference)
                        .expect("We should have the whole sub-dag by now");
                    self.is_vote(&voting_block, leader_block)
                });
                if is_vote {
                    votes |= 1 << reference.authority.index();
                }
            }
            authors.push(author);
            vote_masks.push(votes);
        }
        union_direct_commit(
            &self.stakes,
            self.quorum_threshold,
            self.certificate_quorum,
            &authors,
            &vote_masks,
        )
    }
}
