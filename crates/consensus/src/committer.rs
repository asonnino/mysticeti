// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::VecDeque, sync::Arc};

use crate::{
    base::{BaseCommitter, BaseCommitterOptions},
    leader::LeaderElector,
    protocol::Protocol,
};
use dag::{
    committee::Committee,
    consensus::{DagConsensus, LeaderStatus},
    metrics::Metrics,
    storage::BlockReader,
    types::{AuthorityIndex, BlockReference, RoundNumber, Stake, format_authority_round},
};

/// A universal committer uses a collection of committers to commit a sequence of leaders.
/// It can be configured to use a combination of different commit strategies, including
/// multi-leaders, backup leaders, and pipelines.
pub struct Committer {
    block_reader: BlockReader,
    base_committers: Vec<BaseCommitter>,
    strong_quorum: Stake,
    metrics: Arc<Metrics>,
}

impl Committer {
    pub fn new(
        committee: Arc<Committee>,
        block_reader: BlockReader,
        protocol: Protocol,
        metrics: Arc<Metrics>,
    ) -> Self {
        let mut base_committers = Vec::new();
        let pipeline_stages = if protocol.pipeline {
            protocol.wave_length
        } else {
            1
        };

        for round_offset in 0..pipeline_stages {
            for leader_offset in 0..protocol.number_of_leaders {
                let options = BaseCommitterOptions {
                    strong_quorum: protocol.strong_quorum,
                    wave_length: protocol.wave_length,
                    round_offset,
                    leader_offset: leader_offset as RoundNumber,
                };
                let committer = BaseCommitter::new(
                    committee.clone(),
                    block_reader.clone(),
                    LeaderElector::new(committee.len()),
                    options,
                );
                base_committers.push(committer);
            }
        }

        Self {
            block_reader,
            base_committers,
            strong_quorum: protocol.strong_quorum,
            metrics,
        }
    }

    pub fn strong_quorum(&self) -> Stake {
        self.strong_quorum
    }

    /// Try to commit part of the dag. This function is idempotent and returns a list of
    /// ordered decided leaders.
    #[tracing::instrument(skip_all, fields(last_decided = %last_decided))]
    pub fn try_commit(&self, last_decided: BlockReference) -> Vec<LeaderStatus> {
        let highest_known_round = self.block_reader.highest_round();
        let last_decided_round = last_decided.round();
        let last_decided_round_authority = (last_decided.round(), last_decided.authority);

        // Try to decide as many leaders as possible, starting with the highest round.
        let mut leaders = VecDeque::new();
        for round in (last_decided_round..=highest_known_round).rev() {
            for committer in self.base_committers.iter().rev() {
                // Skip committers that don't have a leader for this round.
                let Some(leader) = committer.elect_leader(round) else {
                    continue;
                };
                tracing::debug!(
                    "Trying to decide {} with {committer}",
                    format_authority_round(leader, round)
                );

                // Try to directly decide the leader.
                let mut status = committer.try_direct_decide(leader, round);
                self.update_metrics(&status, true);
                tracing::debug!("Outcome of direct rule: {status}");

                // If we can't directly decide the leader, try to indirectly decide it.
                if !status.is_decided() {
                    status = committer.try_indirect_decide(leader, round, leaders.iter());
                    self.update_metrics(&status, false);
                    tracing::debug!("Outcome of indirect rule: {status}");
                }

                leaders.push_front(status);
            }
        }

        // The decided sequence is the longest prefix of decided leaders.
        leaders
            .into_iter()
            // Skip all leaders before the last decided round.
            .skip_while(|x| (x.round(), x.authority()) != last_decided_round_authority)
            // Skip the last decided leader.
            .skip(1)
            // Filter out all the genesis.
            .filter(|x| x.round() > 0)
            // Stop the sequence upon encountering an undecided leader.
            .take_while(|x| x.is_decided())
            .inspect(|x| tracing::debug!("Decided {x}"))
            .collect()
    }

    /// Return list of leaders for the round. Syncer may give those leaders some extra time.
    /// To preserve (theoretical) liveness, we should wait `Delta` time for at least the
    /// first leader. Can return empty vec if round does not have a designated leader.
    pub fn get_leaders(&self, round: RoundNumber) -> Vec<AuthorityIndex> {
        self.base_committers
            .iter()
            .filter_map(|committer| committer.elect_leader(round))
            .collect()
    }

    /// Update metrics.
    fn update_metrics(&self, leader: &LeaderStatus, direct_decide: bool) {
        let authority = leader.authority().to_string();
        let direct_or_indirect = if direct_decide { "direct" } else { "indirect" };
        let status = match leader {
            LeaderStatus::Commit(..) => format!("{direct_or_indirect}-commit"),
            LeaderStatus::Skip(..) => format!("{direct_or_indirect}-skip"),
            LeaderStatus::Undecided(..) => return,
        };
        self.metrics.inc_committed_leaders(&authority, &status);
    }
}

impl DagConsensus for Committer {
    fn quorum_threshold(&self) -> Stake {
        self.strong_quorum
    }

    fn try_commit(&self, last_decided: BlockReference) -> Vec<LeaderStatus> {
        self.try_commit(last_decided)
    }

    fn get_leaders(&self, round: RoundNumber) -> Vec<AuthorityIndex> {
        self.get_leaders(round)
    }
}
