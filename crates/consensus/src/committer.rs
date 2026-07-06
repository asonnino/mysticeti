// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::VecDeque, sync::Arc};

use crate::{base::BaseCommitter, leader::LeaderElector, protocol::Protocol};
use dag::{
    authority::Authority,
    block::RoundNumber,
    committee::Committee,
    committee::Stake,
    consensus::{DagConsensus, LeaderStatus},
    storage::BlockReader,
};

#[cfg(any(test, feature = "test-utils"))]
use crate::protocol::ConsensusProtocol;
#[cfg(any(test, feature = "test-utils"))]
use dag::storage::Storage;

/// A universal committer uses a collection of committers to commit a sequence of leaders.
/// It can be configured to use a combination of different commit strategies, including
/// multi-leaders, backup leaders, and pipelines.
pub struct Committer {
    block_reader: BlockReader,
    base_committers: Vec<BaseCommitter>,
    quorum_threshold: Stake,
    leader_wait: bool,
    /// Whether the protocol has an optimistic fast path; drives the test-only
    /// round-depth queries.
    #[cfg(any(test, feature = "test-utils"))]
    has_fast_path: bool,
    /// Reusable buffer for commit decisions.
    leaders: VecDeque<LeaderStatus>,
}

impl Committer {
    pub fn new(committee: Arc<Committee>, block_reader: BlockReader, protocol: Protocol) -> Self {
        let mut base_committers = Vec::new();
        let pipeline_stages = if protocol.pipeline {
            protocol.wave_length
        } else {
            1
        };

        for round_offset in 0..pipeline_stages {
            for leader_offset in 0..protocol.leader_count.get() {
                let committer = BaseCommitter::new(
                    committee.clone(),
                    block_reader.clone(),
                    LeaderElector::new(committee.len()),
                    &protocol,
                    leader_offset as RoundNumber,
                    round_offset,
                );
                base_committers.push(committer);
            }
        }

        Self {
            block_reader,
            base_committers,
            quorum_threshold: protocol.quorum_threshold,
            leader_wait: protocol.leader_wait,
            #[cfg(any(test, feature = "test-utils"))]
            has_fast_path: protocol.fast_path.is_some(),
            leaders: VecDeque::new(),
        }
    }

    /// Try to commit part of the dag. This function is idempotent and returns a list of
    /// ordered decided leaders. `last_decided` is the slot of the most recently consumed
    /// decision; pass `None` on a fresh start to yield every decided leader from round 0
    /// upward.
    #[tracing::instrument(level = "debug", skip_all, fields(last_decided = ?last_decided))]
    pub fn try_commit(
        &mut self,
        last_decided: Option<(RoundNumber, Authority)>,
    ) -> impl Iterator<Item = LeaderStatus> + '_ {
        let highest_known_round = self.block_reader.highest_round();
        let last_decided_round = last_decided.map(|(round, _)| round).unwrap_or(0);

        // Try to decide as many leaders as possible, starting with the highest round.
        self.leaders.clear();
        for round in (last_decided_round..=highest_known_round).rev() {
            for committer in self.base_committers.iter().rev() {
                // Skip committers that don't have a leader for this round.
                let Some(leader) = committer.elect_leader(round) else {
                    continue;
                };
                tracing::debug!(
                    "Trying to decide {} with {committer}",
                    leader.with_round(round)
                );

                // Try to directly decide the leader.
                let mut status = committer.try_direct_decide(leader, round);
                tracing::debug!("Outcome of direct rule: {status}");

                // If we can't directly decide the leader, try to indirectly decide it.
                if !status.is_decided() {
                    status = committer.try_indirect_decide(leader, round, self.leaders.iter());
                    tracing::debug!("Outcome of indirect rule: {status}");
                }

                self.leaders.push_front(status);
            }
        }

        // The decided sequence is the longest prefix of decided leaders.
        self.leaders
            .drain(..)
            // Position past the previously-yielded decision, if any. When `None`
            // (fresh start), yield every decided leader from round 0 upward.
            .skip_while(move |x| match last_decided {
                Some(round_author) => (x.round(), x.authority()) != round_author,
                None => false,
            })
            .skip(if last_decided.is_some() { 1 } else { 0 })
            // Filter out all the genesis.
            .filter(|x| x.round() > 0)
            // Stop the sequence upon encountering an undecided leader.
            .take_while(|x| x.is_decided())
            .inspect(|x| tracing::debug!("Decided {x}"))
    }
}

/// Test-only constructor and round-arithmetic queries.
#[cfg(any(test, feature = "test-utils"))]
impl Committer {
    /// Build a [`Committer`] over the given storage from a [`ConsensusProtocol`]
    /// spec. Mirrors [`Storage::new_for_test`] so integration tests can stamp
    /// out fresh committers without depending on `BlockReader` or
    /// [`Protocol::to_protocol`] internals.
    pub fn new_for_test(
        committee: &Arc<Committee>,
        storage: &Storage,
        spec: &ConsensusProtocol,
    ) -> Self {
        Self::new(
            committee.clone(),
            storage.block_reader().clone(),
            spec.to_protocol(committee).expect("valid protocol"),
        )
    }

    /// True if any of this committer's base committers owns a leader at `round`.
    pub fn is_leader_round(&self, round: RoundNumber) -> bool {
        self.base_committers
            .iter()
            .any(|bc| bc.wave.is_leader_round(round))
    }

    /// Smallest leader round strictly greater than `round`.
    pub fn next_leader_round_after(&self, round: RoundNumber) -> RoundNumber {
        (round + 1..)
            .find(|&r| self.is_leader_round(r))
            .expect("leader rounds are unbounded above")
    }

    /// The `n`-th leader round counting from 0 (1-indexed: `n=1` returns the
    /// first non-genesis leader round).
    pub fn nth_leader_round(&self, n: u64) -> RoundNumber {
        let mut round = 0;
        for _ in 0..n {
            round = self.next_leader_round_after(round);
        }
        round
    }

    /// Voting round for the leader at `leader_round`.
    /// Panics if `leader_round` is not a leader round.
    pub fn voting_round_for(&self, leader_round: RoundNumber) -> RoundNumber {
        let bc = self
            .base_committers
            .iter()
            .find(|bc| bc.wave.is_leader_round(leader_round))
            .expect("not a leader round");
        bc.wave.voting_round(bc.wave.number(leader_round))
    }

    /// Decision round for the leader at `leader_round`.
    /// Panics if `leader_round` is not a leader round.
    pub fn decision_round_for(&self, leader_round: RoundNumber) -> RoundNumber {
        let bc = self
            .base_committers
            .iter()
            .find(|bc| bc.wave.is_leader_round(leader_round))
            .expect("not a leader round");
        bc.wave.decision_round(bc.wave.number(leader_round))
    }

    /// Shallowest DAG depth at which the direct rule can decide the leader at
    /// `leader_round`: the voting round when a fast path is configured (votes
    /// alone can commit), else the decision round.
    /// Panics if `leader_round` is not a leader round.
    pub fn earliest_decision_round_for(&self, leader_round: RoundNumber) -> RoundNumber {
        if self.has_fast_path {
            self.voting_round_for(leader_round)
        } else {
            self.decision_round_for(leader_round)
        }
    }
}

impl DagConsensus for Committer {
    fn quorum_threshold(&self) -> Stake {
        self.quorum_threshold
    }

    fn try_commit(
        &mut self,
        last_decided: Option<(RoundNumber, Authority)>,
    ) -> impl Iterator<Item = LeaderStatus> {
        self.try_commit(last_decided)
    }

    fn get_leaders(&self, round: RoundNumber) -> Option<impl Iterator<Item = Authority>> {
        if self.leader_wait {
            Some(
                self.base_committers
                    .iter()
                    .filter_map(move |c| c.elect_leader(round)),
            )
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use dag::{consensus::DagConsensus, storage::Storage, test_util::committee};

    use crate::{
        committer::Committer,
        protocol::{FastPath, Protocol},
    };

    fn test_protocol(fast_path: Option<FastPath>) -> Protocol {
        Protocol {
            direct_commit_quorum: 3,
            direct_skip_quorum: 3,
            certificate_quorum: 3,
            quorum_threshold: 4,
            fast_path,
            anchor_link_size: 1,
            wave_length: 3,
            leader_count: NonZeroUsize::new(1).unwrap(),
            pipeline: false,
            leader_wait: true,
            require_crypto: false,
        }
    }

    #[test]
    fn quorum_threshold_sourced_from_protocol_field() {
        let committee = committee(4);
        let storage = Storage::new_for_test(&committee);
        let committer = Committer::new(
            committee.clone(),
            storage.block_reader().clone(),
            test_protocol(None),
        );
        assert_eq!(committer.quorum_threshold(), 4);
    }

    /// `earliest_decision_round_for` is the decision round for single-tier protocols
    /// and the voting round when a fast path is configured.
    #[test]
    fn earliest_decision_round_tracks_fast_path() {
        let committee = committee(4);
        let storage = Storage::new_for_test(&committee);
        let slow_committer = Committer::new(
            committee.clone(),
            storage.block_reader().clone(),
            test_protocol(None),
        );
        let fast_committer = Committer::new(
            committee.clone(),
            storage.block_reader().clone(),
            test_protocol(Some(FastPath {
                commit_quorum: 3,
                weak_indirect_quorum: 2,
            })),
        );
        // Wave length 3: leader round 3 → voting round 4, decision round 5.
        assert_eq!(slow_committer.earliest_decision_round_for(3), 5);
        assert_eq!(fast_committer.earliest_decision_round_for(3), 4);
    }
}
