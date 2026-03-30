// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod block_store;
pub mod log;
pub mod state;
pub mod wal;

use std::{io, path::Path, sync::Arc};

use minibytes::Bytes;

use crate::{
    committee::Committee,
    data::Data,
    metrics::Metrics,
    types::{AuthorityIndex, StatementBlock},
};

use self::{
    block_store::{
        BlockStore, CommitData, OwnBlockData, WAL_ENTRY_BLOCK, WAL_ENTRY_COMMIT, WAL_ENTRY_PAYLOAD,
        WAL_ENTRY_STATE,
    },
    state::RecoveredState,
    wal::{open_file_for_wal, walf, WalPosition, WalSyncer, WalWriter},
};

pub struct Storage {
    wal_writer: WalWriter,
    block_store: BlockStore,
}

impl Storage {
    pub fn open(
        authority: AuthorityIndex,
        wal_path: impl AsRef<Path>,
        metrics: Arc<Metrics>,
        committee: &Committee,
    ) -> io::Result<(Self, RecoveredState)> {
        let wal_file = open_file_for_wal(wal_path)?;
        let (wal_writer, wal_reader) = walf(wal_file)?;
        let (block_store, recovered) = BlockStore::open(
            authority,
            Arc::new(wal_reader),
            &wal_writer,
            metrics,
            committee,
        );
        Ok((
            Self {
                wal_writer,
                block_store,
            },
            recovered,
        ))
    }

    pub fn new_for_tests(
        authority: AuthorityIndex,
        metrics: Arc<Metrics>,
        committee: &Committee,
    ) -> (Self, RecoveredState) {
        let file = tempfile::tempfile().expect("Failed to create temp file");
        let (wal_writer, wal_reader) = walf(file).expect("Failed to open wal");
        let (block_store, recovered) = BlockStore::open(
            authority,
            Arc::new(wal_reader),
            &wal_writer,
            metrics,
            committee,
        );
        (
            Self {
                wal_writer,
                block_store,
            },
            recovered,
        )
    }

    pub fn write_payload(&mut self, payload: &[u8]) -> WalPosition {
        self.wal_writer
            .write(WAL_ENTRY_PAYLOAD, payload)
            .expect("Failed to write statements to wal")
    }

    pub fn write_state(&mut self, state: &[u8]) {
        #[cfg(feature = "simulator")]
        if state.len() >= wal::MAX_ENTRY_SIZE {
            return;
        }
        self.wal_writer
            .write(WAL_ENTRY_STATE, state)
            .expect("Write to wal has failed");
    }

    pub fn write_commits(&mut self, commits: &[CommitData], state: &Bytes) {
        let serialized =
            bincode::serialize(&(commits, state)).expect("Commits serialization failed");
        self.wal_writer
            .write(WAL_ENTRY_COMMIT, &serialized)
            .expect("Write to wal has failed");
    }

    pub fn sync(&mut self) {
        self.wal_writer.sync().expect("Wal sync failed");
    }

    pub fn syncer(&self) -> WalSyncer {
        self.wal_writer
            .syncer()
            .expect("Failed to create wal syncer")
    }

    pub fn block_store(&self) -> &BlockStore {
        &self.block_store
    }

    pub fn insert_block(&mut self, block: Data<StatementBlock>) -> WalPosition {
        let pos = self
            .wal_writer
            .write(WAL_ENTRY_BLOCK, block.serialized_bytes())
            .expect("Writing to wal failed");
        self.block_store.insert_block(block, pos);
        pos
    }

    pub fn insert_own_block(&mut self, data: &OwnBlockData) {
        let block_pos = data.write_to_wal(&mut self.wal_writer);
        self.block_store.insert_block(data.block.clone(), block_pos);
    }
}
