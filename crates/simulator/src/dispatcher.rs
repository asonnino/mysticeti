// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

use parking_lot::Mutex;

use dag::{
    consensus::DagConsensus,
    core::{core_thread::CoreDispatch, syncer::Syncer},
    data::Data,
    types::{Authority, BlockReference, RoundNumber, StatementBlock},
};

use crate::context::SimulatorContext;

pub struct InlineDispatcher<D: DagConsensus> {
    syncer: Mutex<Syncer<SimulatorContext, D>>,
}

impl<D: DagConsensus> InlineDispatcher<D> {
    pub fn new(syncer: Syncer<SimulatorContext, D>) -> Self {
        Self {
            syncer: Mutex::new(syncer),
        }
    }
}

impl<D: DagConsensus> CoreDispatch<SimulatorContext, D> for InlineDispatcher<D> {
    async fn add_blocks(&self, blocks: Vec<Data<StatementBlock>>) {
        self.syncer.lock().add_blocks(blocks);
    }

    async fn force_new_block(&self, round: RoundNumber) {
        self.syncer.lock().force_new_block(round);
    }

    async fn cleanup(&self) {
        self.syncer.lock().core().cleanup();
    }

    async fn get_missing_blocks(&self) -> Vec<HashSet<BlockReference>> {
        self.syncer
            .lock()
            .core()
            .block_manager()
            .missing_blocks()
            .to_vec()
    }

    async fn authority_connection(&self, authority: Authority, connected: bool) {
        let mut lock = self.syncer.lock();
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
