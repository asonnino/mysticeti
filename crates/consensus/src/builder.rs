// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use crate::{
    base::{BaseCommitter, BaseCommitterOptions},
    committer::Committer,
    leader::LeaderElector,
};
use dag::{
    committee::Committee,
    metrics::Metrics,
    storage::BlockReader,
    types::{RoundNumber, Stake},
};

/// A builder for a committer. By default, the builder creates a single base committer,
/// that is, a single leader and no pipeline.
pub struct CommitterBuilder {
    committee: Arc<Committee>,
    block_reader: BlockReader,
    metrics: Arc<Metrics>,
    strong_quorum: Stake,
    wave_length: RoundNumber,
    number_of_leaders: usize,
    pipeline: bool,
}

impl CommitterBuilder {
    pub fn new(
        committee: Arc<Committee>,
        block_reader: BlockReader,
        metrics: Arc<Metrics>,
    ) -> Self {
        let total_stake = committee.total_stake();
        Self {
            committee,
            block_reader,
            metrics,
            strong_quorum: 2 * total_stake / 3 + 1,
            wave_length: 2,
            number_of_leaders: 1,
            pipeline: false,
        }
    }

    pub fn with_strong_quorum(mut self, strong_quorum: Stake) -> Self {
        self.strong_quorum = strong_quorum;
        self
    }

    pub fn with_wave_length(mut self, wave_length: RoundNumber) -> Self {
        self.wave_length = wave_length;
        self
    }

    pub fn with_number_of_leaders(mut self, number_of_leaders: usize) -> Self {
        self.number_of_leaders = number_of_leaders;
        self
    }

    pub fn with_pipeline(mut self, pipeline: bool) -> Self {
        self.pipeline = pipeline;
        self
    }

    pub fn build(self) -> Committer {
        let mut committers = Vec::new();
        let pipeline_stages = if self.pipeline { self.wave_length } else { 1 };

        for round_offset in 0..pipeline_stages {
            for leader_offset in 0..self.number_of_leaders {
                let options = BaseCommitterOptions {
                    strong_quorum: self.strong_quorum,
                    wave_length: self.wave_length,
                    round_offset,
                    leader_offset: leader_offset as RoundNumber,
                };
                let committer = BaseCommitter::new(
                    self.committee.clone(),
                    self.block_reader.clone(),
                    LeaderElector::new(self.committee.len()),
                    options,
                );
                committers.push(committer);
            }
        }

        Committer::new(
            self.block_reader,
            committers,
            self.strong_quorum,
            self.metrics,
        )
    }
}
