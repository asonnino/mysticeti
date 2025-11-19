// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt::Display, sync::Arc};

use super::{LeaderStatus, DEFAULT_WAVE_LENGTH};
use crate::{
    block_store::BlockStore,
    committee::{Committee, IndirectQuorumThreshold, QuorumThreshold, StakeAggregator},
    consensus::MINIMUM_WAVE_LENGTH,
    data::Data,
    types::{format_authority_round, AuthorityIndex, RoundNumber, StatementBlock},
};

/// The consensus protocol operates in 'waves'. Each wave is composed of a leader round
/// and a decision round.
type WaveNumber = u64;

pub struct BaseCommitterOptions {
    /// The length of a wave (minimum 2)
    pub wave_length: u64,
    /// The offset used in the leader-election protocol. This is used by the multi-committer to
    /// ensure that each [`BaseCommitter`] instance elects a different leader.
    pub leader_offset: u64,
    /// The offset of the first wave. This is used by the pipelined committer to ensure that each
    /// [`BaseCommitter`] instances operates on a different view of the dag.
    pub round_offset: u64,
}

impl Default for BaseCommitterOptions {
    fn default() -> Self {
        Self {
            wave_length: DEFAULT_WAVE_LENGTH,
            leader_offset: 0,
            round_offset: 0,
        }
    }
}

/// The [`BaseCommitter`] contains the bare bone commit logic. Once instantiated, the method
/// `try_direct_decide` and `try_indirect_decide` can be called at any time and any number
/// of times (it is idempotent) to determine whether a leader can be committed or skipped.
pub struct BaseCommitter {
    /// The committee information
    committee: Arc<Committee>,
    /// Keep all block data
    block_store: BlockStore,
    /// The options used by this committer
    options: BaseCommitterOptions,
}

impl BaseCommitter {
    pub fn new(committee: Arc<Committee>, block_store: BlockStore) -> Self {
        Self {
            committee,
            block_store,
            options: BaseCommitterOptions::default(),
        }
    }

    pub fn with_options(mut self, options: BaseCommitterOptions) -> Self {
        assert!(options.wave_length >= MINIMUM_WAVE_LENGTH);
        self.options = options;
        self
    }

    /// Return the wave in which the specified round belongs.
    fn wave_number(&self, round: RoundNumber) -> WaveNumber {
        round.saturating_sub(self.options.round_offset) / self.options.wave_length
    }

    /// Return the leader round of the specified wave number. The leader round is always the first
    /// round of the wave.
    fn leader_round(&self, wave: WaveNumber) -> RoundNumber {
        wave * self.options.wave_length + self.options.round_offset
    }

    /// Return the decision round of the specified wave. The decision round is always the last
    /// round of the wave.
    fn decision_round(&self, wave: WaveNumber) -> RoundNumber {
        let wave_length = self.options.wave_length;
        wave * wave_length + wave_length - 1 + self.options.round_offset
    }

    /// The leader-elect protocol is offset by `leader_offset` to ensure that different committers
    /// with different leader offsets elect different leaders for the same round number. This
    /// function returns `None` if there are no leaders for the specified round.
    pub fn elect_leader(&self, round: RoundNumber) -> Option<AuthorityIndex> {
        let wave = self.wave_number(round);
        if self.leader_round(wave) != round {
            return None;
        }

        let offset = self.options.leader_offset as RoundNumber;
        Some(self.committee.elect_leader(round + offset))
    }

    /// Decide the status of a target leader from the specified anchor. We commit the target leader
    /// if it has enough support (that is, 2f+1 supports) in the causal history the anchor.
    /// Otherwise, we skip the target leader.
    fn decide_leader_from_anchor(
        &self,
        anchor: &Data<StatementBlock>,
        leader: AuthorityIndex,
        leader_round: RoundNumber,
    ) -> LeaderStatus {
        // Get the block(s) proposed by the leader. There could be more than one leader block
        // per round (produced by a Byzantine leader).
        let leader_blocks = self
            .block_store
            .get_blocks_at_authority_round(leader, leader_round);

        // Get all blocks that could be potential supports for the target leader. These blocks
        // are in the decision round of the target leader and are linked to the anchor.
        let wave = self.wave_number(leader_round);
        let decision_round = self.decision_round(wave);
        let decision_blocks = self.block_store.get_blocks_by_round(decision_round);
        let potential_supports: Vec<_> = decision_blocks
            .iter()
            .filter(|block| self.block_store.linked(anchor, block))
            .collect();

        // For each leader block, check if it has enough support in the causal history
        let mut supported_leader_blocks: Vec<_> = leader_blocks
            .into_iter()
            .filter(|_| {
                let mut indirect_support_stake_aggregator =
                    StakeAggregator::<IndirectQuorumThreshold>::new();

                // Count supports for this leader block in the causal history
                for block in &potential_supports {
                    let authority = block.reference().authority;
                    if block
                        .includes()
                        .iter()
                        .any(|include| include.authority == leader)
                    {
                        if indirect_support_stake_aggregator.add(authority, &self.committee) {
                            return true; // We have enough support (2f+1)
                        }
                    }
                }
                false // Not enough support
            })
            .collect();

        // There can be at most one supported leader, otherwise it means the BFT assumption
        // is broken.
        if supported_leader_blocks.len() > 1 {
            panic!("More than one supported block at wave {wave} from leader {leader}")
        }

        // We commit the target leader if it has enough support in the anchor's caussal history.
        // Otherwise skip it.
        match supported_leader_blocks.pop() {
            Some(supported_leader_block) => LeaderStatus::Commit(supported_leader_block.clone()),
            None => LeaderStatus::Skip(leader, leader_round),
        }
    }

    /// Check whether the specified leader has enough blames (that is, 4f+1 non-supports) to be
    /// directly skipped.
    fn enough_leader_blame(&self, decision_round: RoundNumber, leader: AuthorityIndex) -> bool {
        let decision_blocks = self.block_store.get_blocks_by_round(decision_round);

        let mut blame_stake_aggregator = StakeAggregator::<QuorumThreshold>::new();
        for decision_block in &decision_blocks {
            let decider = decision_block.reference().authority;
            if decision_block
                .includes()
                .iter()
                .all(|include| include.authority != leader)
            {
                tracing::trace!(
                    "[{self}] {decision_block:?} is a blame for leader {}",
                    format_authority_round(leader, decision_round - 1)
                );
                if blame_stake_aggregator.add(decider, &self.committee) {
                    return true;
                }
            }
        }
        false
    }

    /// Check whether the specified leader has enough support (that is, 4f+1 supports)
    /// to be directly committed.
    fn enough_leader_support(
        &self,
        decision_round: RoundNumber,
        leader_block: &Data<StatementBlock>,
    ) -> bool {
        let decision_blocks = self.block_store.get_blocks_by_round(decision_round);

        let mut support_stake_aggregator = StakeAggregator::<QuorumThreshold>::new();
        for decision_block in &decision_blocks {
            let decider = decision_block.reference().authority;
            if decision_block
                .includes()
                .iter()
                .any(|include| include.authority == leader_block.author_round().0)
            {
                tracing::trace!(
                    "[{self}] {decision_block:?} is a support for leader {leader_block:?}"
                );
                if support_stake_aggregator.add(decider, &self.committee) {
                    return true;
                }
            }
        }
        false
    }

    /// Apply the indirect decision rule to the specified leader to see whether we can
    /// indirect-commit or indirect-skip it.
    #[tracing::instrument(skip_all, fields(leader = %format_authority_round(leader, leader_round)))]
    pub fn try_indirect_decide<'a>(
        &self,
        leader: AuthorityIndex,
        leader_round: RoundNumber,
        leaders: impl Iterator<Item = &'a LeaderStatus>,
    ) -> LeaderStatus {
        // The anchor is the first committed leader with round higher than the decision round of the
        // target leader. We must stop the iteration upon encountering an undecided leader.
        let anchors = leaders.filter(|x| leader_round + self.options.wave_length <= x.round());

        for anchor in anchors {
            tracing::trace!(
                "[{self}] Trying to indirect-decide {} using anchor {anchor}",
                format_authority_round(leader, leader_round),
            );
            match anchor {
                LeaderStatus::Commit(anchor) => {
                    return self.decide_leader_from_anchor(anchor, leader, leader_round);
                }
                LeaderStatus::Skip(..) => (),
                LeaderStatus::Undecided(..) => break,
            }
        }

        LeaderStatus::Undecided(leader, leader_round)
    }

    /// Apply the direct decision rule to the specified leader to see whether we can direct-commit
    /// or direct-skip it.
    #[tracing::instrument(skip_all, fields(leader = %format_authority_round(leader, leader_round)))]
    pub fn try_direct_decide(
        &self,
        leader: AuthorityIndex,
        leader_round: RoundNumber,
    ) -> LeaderStatus {
        // Check whether the leader has enough blame. That is, whether there are 4f+1 non-supports
        // for that leader.
        let wave = self.wave_number(leader_round);
        let decision_round = self.decision_round(wave);
        if self.enough_leader_blame(decision_round, leader) {
            return LeaderStatus::Skip(leader, leader_round);
        }

        // Check whether the leader(s) has enough support. That is, whether there are 4f+1
        // supports for the leader. Note that there could be more than one leader block
        // (created by Byzantine leaders).
        let leader_blocks = self
            .block_store
            .get_blocks_at_authority_round(leader, leader_round);
        let mut leaders_with_enough_support: Vec<_> = leader_blocks
            .into_iter()
            .filter(|l| self.enough_leader_support(decision_round, l))
            .map(LeaderStatus::Commit)
            .collect();

        // There can be at most one leader with enough support for each round, otherwise it means
        // the BFT assumption is broken.
        if leaders_with_enough_support.len() > 1 {
            panic!(
                "[{self}] More than one leader block with enough support for {}",
                format_authority_round(leader, leader_round)
            )
        }

        leaders_with_enough_support
            .pop()
            .unwrap_or_else(|| LeaderStatus::Undecided(leader, leader_round))
    }
}

impl Display for BaseCommitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Committer-L{}-R{}",
            self.options.leader_offset, self.options.round_offset
        )
    }
}
