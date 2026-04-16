// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use dag::{
    committee::Committee,
    consensus::{DagConsensus, LeaderStatus},
    metrics::Metrics,
    storage::BlockReader,
    types::{AuthorityIndex, BlockReference, RoundNumber, Stake},
};

use crate::builder::CommitterBuilder;

/// Mysticeti consensus protocol (n = 3f + 1).
///
/// Operates with a wave length of 3 (leader, vote,
/// decision), pipelining enabled, and a strong quorum
/// of 2f + 1.
pub struct Mysticeti {
    committer: crate::committer::Committer,
}

impl Mysticeti {
    pub const WAVE_LENGTH: RoundNumber = 3;

    pub fn new(
        committee: Arc<Committee>,
        block_reader: BlockReader,
        metrics: Arc<Metrics>,
        number_of_leaders: usize,
    ) -> Self {
        let quorum = 2 * committee.total_stake() / 3 + 1;

        let committer = CommitterBuilder::new(committee, block_reader, metrics)
            .with_strong_quorum(quorum)
            .with_wave_length(Self::WAVE_LENGTH)
            .with_number_of_leaders(number_of_leaders)
            .with_pipeline(true)
            .build();

        Self { committer }
    }
}

impl DagConsensus for Mysticeti {
    fn quorum_threshold(&self) -> Stake {
        self.committer.strong_quorum()
    }

    fn try_commit(&self, last_decided: BlockReference) -> Vec<LeaderStatus> {
        self.committer.try_commit(last_decided)
    }

    fn get_leaders(&self, round: RoundNumber) -> Vec<AuthorityIndex> {
        self.committer.get_leaders(round)
    }
}
