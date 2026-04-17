// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub(crate) mod block_store;
mod reader;
pub(crate) mod state;
pub(crate) mod wal;

use std::{io, path::Path, sync::Arc};

use crate::{
    committee::Committee,
    data::Data,
    metrics::Metrics,
    types::{Authority, StatementBlock},
};

use self::{
    block_store::{CommitData, OwnBlockData, WAL_ENTRY_BLOCK, WAL_ENTRY_COMMIT, WAL_ENTRY_PAYLOAD},
    wal::{WalWriter, open_file_for_wal, walf},
};

pub use self::block_store::BlockReader;
pub use self::state::RecoveredState;
pub use self::wal::{WalPosition, WalSyncer};

pub struct Storage {
    wal_writer: WalWriter,
    reader: BlockReader,
}

impl Storage {
    pub fn open(
        authority: Authority,
        wal_path: impl AsRef<Path>,
        metrics: Arc<Metrics>,
        committee: &Committee,
    ) -> io::Result<(Self, RecoveredState)> {
        let wal_file = open_file_for_wal(wal_path)?;
        let (wal_writer, wal_reader) = walf(wal_file)?;
        let (reader, recovered) =
            BlockReader::open(authority, wal_reader, &wal_writer, metrics, committee);
        Ok((Self { wal_writer, reader }, recovered))
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn new_for_tests(
        authority: Authority,
        metrics: Arc<Metrics>,
        committee: &Committee,
    ) -> (Self, RecoveredState) {
        let file = tempfile::tempfile().expect("Failed to create temp file");
        let (wal_writer, wal_reader) = walf(file).expect("Failed to open wal");
        let (reader, recovered) =
            BlockReader::open(authority, wal_reader, &wal_writer, metrics, committee);
        (Self { wal_writer, reader }, recovered)
    }

    pub(crate) fn write_payload(&mut self, payload: &[u8]) -> WalPosition {
        self.wal_writer
            .write(WAL_ENTRY_PAYLOAD, payload)
            .expect("Failed to write statements to wal")
    }

    pub(crate) fn write_commits(&mut self, commits: &[CommitData]) {
        let serialized = bincode::serialize(commits).expect("Commits serialization failed");
        self.wal_writer
            .write(WAL_ENTRY_COMMIT, &serialized)
            .expect("Write to wal has failed");
    }

    pub(crate) fn sync(&mut self) {
        self.wal_writer.sync().expect("Wal sync failed");
    }

    pub(crate) fn syncer(&self) -> WalSyncer {
        self.wal_writer
            .syncer()
            .expect("Failed to create wal syncer")
    }

    pub fn block_reader(&self) -> &BlockReader {
        &self.reader
    }

    pub(crate) fn insert_block(&mut self, block: Data<StatementBlock>) -> WalPosition {
        let pos = self
            .wal_writer
            .write(WAL_ENTRY_BLOCK, block.serialized_bytes())
            .expect("Writing to wal failed");
        self.reader.inc_block_store_entries();
        self.reader.write_inner().add_loaded(pos, block);
        pos
    }

    pub(crate) fn insert_own_block(&mut self, data: &OwnBlockData) {
        let block_pos = data.write_to_wal(&mut self.wal_writer);
        self.reader.inc_block_store_entries();
        self.reader
            .write_inner()
            .add_loaded(block_pos, data.block.clone());
    }
}
