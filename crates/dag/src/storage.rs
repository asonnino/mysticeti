// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub(crate) mod block_store;
mod reader;
pub(crate) mod state;
pub(crate) mod wal;

use std::{io, path::Path, sync::Arc};

use crate::{
    authority::Authority, block::Block, committee::Committee, data::Data, metrics::Metrics,
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

    /// Stream every committed sub-dag written to this storage so far, in order.
    /// Each `WAL_ENTRY_COMMIT` entry carries a `Vec<CommitData>` (one batch per commit
    /// flush), which this iterator flattens to a single `CommitData` stream.
    pub fn iter_commits(&self) -> impl Iterator<Item = CommitData> + '_ {
        self.reader
            .wal_reader()
            .iter_until(&self.wal_writer)
            .filter_map(|(_pos, (tag, data))| (tag == WAL_ENTRY_COMMIT).then_some(data))
            .flat_map(|data| {
                let batch: Vec<CommitData> = bincode::deserialize(&data)
                    .expect("Failed to deserialize commit data from wal");
                batch.into_iter()
            })
    }

    pub(crate) fn insert_block(&mut self, block: Data<Block>) -> WalPosition {
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

#[cfg(test)]
mod tests {
    use crate::{
        authority::Authority,
        block::BlockReference,
        committee::Committee,
        metrics::Metrics,
        storage::{Storage, block_store::CommitData},
    };

    fn commit(leader_authority: u64, round: u64, sub_dag: &[(u64, u64)]) -> CommitData {
        CommitData {
            leader: BlockReference::new_test(leader_authority, round),
            sub_dag: sub_dag
                .iter()
                .map(|(a, r)| BlockReference::new_test(*a, *r))
                .collect(),
        }
    }

    #[test]
    fn iter_commits_yields_every_batch_in_order() {
        let committee = Committee::new_test(vec![1; 4]);
        let (mut storage, _recovered) =
            Storage::new_for_tests(Authority::from(0u64), Metrics::new_for_test(0), &committee);

        let batch_one = vec![commit(0, 1, &[(0, 1), (1, 1)]), commit(1, 2, &[(1, 2)])];
        let batch_two = vec![commit(2, 3, &[(2, 3), (3, 3)])];
        let batch_three = vec![
            commit(3, 4, &[(3, 4)]),
            commit(0, 5, &[(0, 5), (1, 5), (2, 5)]),
        ];

        storage.write_commits(&batch_one);
        storage.write_commits(&batch_two);
        storage.write_commits(&batch_three);

        let expected: Vec<_> = batch_one
            .iter()
            .chain(&batch_two)
            .chain(&batch_three)
            .collect();
        let actual: Vec<_> = storage.iter_commits().collect();

        assert_eq!(actual.len(), expected.len());
        for (got, want) in actual.iter().zip(expected.iter()) {
            assert_eq!(got.leader, want.leader);
            assert_eq!(got.sub_dag, want.sub_dag);
        }
    }

    #[test]
    fn iter_commits_is_empty_for_fresh_storage() {
        let committee = Committee::new_test(vec![1; 4]);
        let (storage, _recovered) =
            Storage::new_for_tests(Authority::from(0u64), Metrics::new_for_test(0), &committee);
        assert_eq!(storage.iter_commits().count(), 0);
    }
}
