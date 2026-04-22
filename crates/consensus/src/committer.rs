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

/// A universal committer uses a collection of committers to commit a sequence of leaders.
/// It can be configured to use a combination of different commit strategies, including
/// multi-leaders, backup leaders, and pipelines.
pub struct Committer {
    block_reader: BlockReader,
    base_committers: Vec<BaseCommitter>,
    strong_quorum: Stake,
    leader_wait: bool,
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
            strong_quorum: protocol.strong_quorum,
            leader_wait: protocol.leader_wait,
            leaders: VecDeque::new(),
        }
    }

    /// Try to commit part of the dag. This function is idempotent and returns a list of
    /// ordered decided leaders. `last_decided` is the slot of the most recently consumed
    /// decision; pass `None` on a fresh start to yield every decided leader from round 0
    /// upward.
    #[tracing::instrument(level = "debug", skip_all, fields(last_decided = ?last_decided))]
    pub(crate) fn try_commit(
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

impl DagConsensus for Committer {
    fn quorum_threshold(&self) -> Stake {
        self.strong_quorum
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
