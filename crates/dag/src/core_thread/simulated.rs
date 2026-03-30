// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

use parking_lot::Mutex;

use crate::{
    data::Data,
    syncer::Syncer,
    types::{AuthorityIndex, BlockReference, RoundNumber, StatementBlock},
};

pub struct CoreThreadDispatcher {
    syncer: Mutex<Syncer>,
}

impl CoreThreadDispatcher {
    pub fn start(syncer: Syncer) -> Self {
        Self {
            syncer: Mutex::new(syncer),
        }
    }

    pub fn stop(self) -> Syncer {
        self.syncer.into_inner()
    }

    pub async fn add_blocks(&self, blocks: Vec<Data<StatementBlock>>) {
        self.syncer.lock().add_blocks(blocks);
    }

    pub async fn force_new_block(&self, round: RoundNumber) {
        self.syncer.lock().force_new_block(round);
    }

    pub async fn cleanup(&self) {
        self.syncer.lock().core().cleanup();
    }

    pub async fn get_missing_blocks(&self) -> Vec<HashSet<BlockReference>> {
        self.syncer
            .lock()
            .core()
            .block_manager()
            .missing_blocks()
            .to_vec()
    }

    pub async fn authority_connection(&self, authority_index: AuthorityIndex, connected: bool) {
        let mut lock = self.syncer.lock();
        if connected {
            lock.connected_authorities.insert(authority_index);
        } else {
            lock.connected_authorities.remove(&authority_index);
        }
    }
}
