// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

use tokio::sync::{Mutex, mpsc};

use dag::{
    authority::Authority,
    block::{Block, BlockReference, RoundNumber},
    consensus::{CommittedSubDag, DagConsensus},
    core::{core_thread::CoreDispatch, syncer::Syncer},
    data::Data,
};

use crate::context::SimulatorContext;

pub struct InlineDispatcher<D: DagConsensus> {
    syncer: Mutex<Syncer<SimulatorContext, D>>,
    commit_consumer: Option<mpsc::Sender<CommittedSubDag>>,
}

impl<D: DagConsensus> InlineDispatcher<D> {
    pub fn new(
        syncer: Syncer<SimulatorContext, D>,
        commit_consumer: Option<mpsc::Sender<CommittedSubDag>>,
    ) -> Self {
        Self {
            syncer: Mutex::new(syncer),
            commit_consumer,
        }
    }

    /// Forward newly committed sub-dags to the consumer. Awaited while the caller still
    /// holds the syncer lock, so the consumer observes commits in commit order and a full
    /// consumer suspends the producing task (backpressure).
    async fn forward_commits(&self, committed: Vec<CommittedSubDag>) {
        let Some(consumer) = &self.commit_consumer else {
            return;
        };
        for subdag in committed {
            if consumer.send(subdag).await.is_err() {
                return;
            }
        }
    }
}

impl<D: DagConsensus> CoreDispatch<SimulatorContext, D> for InlineDispatcher<D> {
    async fn add_blocks(&self, blocks: Vec<Data<Block>>) {
        let mut syncer = self.syncer.lock().await;
        let committed = syncer.add_blocks(blocks);
        self.forward_commits(committed).await;
    }

    async fn force_new_block(&self, round: RoundNumber) {
        let mut syncer = self.syncer.lock().await;
        let committed = syncer.force_new_block(round);
        self.forward_commits(committed).await;
    }

    async fn cleanup(&self) {
        self.syncer.lock().await.core().cleanup();
    }

    async fn get_missing_blocks(&self) -> Vec<HashSet<BlockReference>> {
        self.syncer
            .lock()
            .await
            .core()
            .block_manager()
            .missing_blocks()
            .to_vec()
    }

    async fn authority_connection(&self, authority: Authority, connected: bool) {
        let mut lock = self.syncer.lock().await;
        if connected {
            lock.connect_authority(authority);
        } else {
            lock.disconnect_authority(authority);
        }
    }

    fn stop(self) -> Syncer<SimulatorContext, D> {
        self.syncer.into_inner()
    }
}
