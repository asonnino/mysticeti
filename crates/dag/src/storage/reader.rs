// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{sync::Arc, time::Instant};

use parking_lot::{RwLock, RwLockWriteGuard};

use crate::{
    committee::Committee,
    data::Data,
    metrics::Metrics,
    types::{
        AuthorityIndex, BaseStatement, BlockReference, RoundNumber, StatementBlock, Transaction,
        TransactionLocator,
    },
};

use super::{
    block_store::{
        BlockStore, IndexEntry, OwnBlockData, WAL_ENTRY_BLOCK, WAL_ENTRY_COMMIT,
        WAL_ENTRY_OWN_BLOCK, WAL_ENTRY_PAYLOAD, WAL_ENTRY_STATE,
    },
    state::RecoveredStateBuilder,
    wal::{WalReader, WalWriter},
};

#[derive(Clone)]
pub struct BlockReader {
    inner: Arc<RwLock<BlockStore>>,
    wal_reader: Arc<WalReader>,
    metrics: Arc<Metrics>,
}

impl BlockReader {
    pub(super) fn open(
        authority: AuthorityIndex,
        wal_reader: WalReader,
        wal_writer: &WalWriter,
        metrics: Arc<Metrics>,
        committee: &Committee,
    ) -> (BlockReader, super::state::RecoveredState) {
        let last_seen_by_authority = committee.authorities().map(|_| 0).collect();
        let mut store = BlockStore {
            authority,
            last_seen_by_authority,
            ..Default::default()
        };
        let mut builder = RecoveredStateBuilder::new();
        let mut replay_started: Option<Instant> = None;
        let mut block_count = 0u64;
        for (pos, (tag, data)) in wal_reader.iter_until(wal_writer) {
            if replay_started.is_none() {
                replay_started = Some(Instant::now());
                tracing::info!("Wal is not empty, starting replay");
            }
            let block = match tag {
                WAL_ENTRY_BLOCK => {
                    let block = Data::<StatementBlock>::from_bytes(data)
                        .expect("Failed to deserialize data from wal");
                    builder.block(pos, &block);
                    block
                }
                WAL_ENTRY_PAYLOAD => {
                    builder.payload(pos, data);
                    continue;
                }
                WAL_ENTRY_OWN_BLOCK => {
                    let (own_block_data, own_block) = OwnBlockData::from_bytes(data)
                        .expect("Failed to deserialized own block data from wal");
                    builder.own_block(own_block_data);
                    own_block
                }
                WAL_ENTRY_STATE => {
                    builder.state(data);
                    continue;
                }
                WAL_ENTRY_COMMIT => {
                    let (commit_data, state) = bincode::deserialize(&data)
                        .expect("Failed to deserialized commit data from wal");
                    builder.commit_data(commit_data, state);
                    continue;
                }
                _ => {
                    panic!("Unknown wal tag {tag} at position {pos}")
                }
            };
            block_count += 1;
            store.add_unloaded(block.reference(), pos);
        }
        metrics.inc_block_store_entries_by(block_count);
        if let Some(replay_started) = replay_started {
            tracing::info!("Wal replay completed in {:?}", replay_started.elapsed());
        } else {
            tracing::info!("Wal is empty, will start from genesis");
        }
        let reader = Self {
            wal_reader: Arc::new(wal_reader),
            inner: Arc::new(RwLock::new(store)),
            metrics,
        };
        (reader, builder.build())
    }

    pub(super) fn write_inner(&self) -> RwLockWriteGuard<'_, BlockStore> {
        self.inner.write()
    }

    pub(super) fn inc_block_store_entries(&self) {
        self.metrics.inc_block_store_entries();
    }

    pub fn get_block(&self, reference: BlockReference) -> Option<Data<StatementBlock>> {
        let entry = self.inner.read().get_block(reference);
        entry.map(|pos| self.read_index(pos))
    }

    pub fn get_blocks_by_round(&self, round: RoundNumber) -> Vec<Data<StatementBlock>> {
        let entries = self.inner.read().get_blocks_by_round(round);
        self.read_index_vec(entries)
    }

    pub fn get_blocks_at_authority_round(
        &self,
        authority: AuthorityIndex,
        round: RoundNumber,
    ) -> Vec<Data<StatementBlock>> {
        let entries = self
            .inner
            .read()
            .get_blocks_at_authority_round(authority, round);
        self.read_index_vec(entries)
    }

    pub fn block_exists_at_authority_round(
        &self,
        authority: AuthorityIndex,
        round: RoundNumber,
    ) -> bool {
        self.inner
            .read()
            .block_exists_at_authority_round(authority, round)
    }

    pub fn all_blocks_exists_at_authority_round(
        &self,
        authorities: &[AuthorityIndex],
        round: RoundNumber,
    ) -> bool {
        self.inner
            .read()
            .all_blocks_exists_at_authority_round(authorities, round)
    }

    pub fn block_exists(&self, reference: BlockReference) -> bool {
        self.inner.read().block_exists(reference)
    }

    pub fn get_transaction(&self, locator: &TransactionLocator) -> Option<Transaction> {
        self.get_block(*locator.block()).and_then(|block| {
            block
                .statements()
                .get(locator.offset() as usize)
                .cloned()
                .map(|statement| {
                    let BaseStatement::Share(transaction) = statement;
                    transaction
                })
        })
    }

    #[cfg(test)]
    pub fn len_expensive(&self) -> usize {
        self.inner.read().len_expensive()
    }

    pub fn highest_round(&self) -> RoundNumber {
        self.inner.read().highest_round
    }

    pub fn cleanup(&self, threshold_round: RoundNumber) {
        if threshold_round == 0 {
            return;
        }
        let _timer = self.metrics.block_store_cleanup_utilization_timer();
        let unloaded = self.inner.write().unload_below_round(threshold_round);
        self.metrics
            .inc_block_store_unloaded_blocks_by(unloaded as u64);
        let retained_maps = self.wal_reader.cleanup();
        self.metrics.set_wal_mappings(retained_maps as i64);
    }

    pub fn get_own_blocks(
        &self,
        from_excluded: RoundNumber,
        limit: usize,
    ) -> Vec<Data<StatementBlock>> {
        let entries = self.inner.read().get_own_blocks(from_excluded, limit);
        self.read_index_vec(entries)
    }

    pub fn get_others_blocks(
        &self,
        from_excluded: RoundNumber,
        authority: AuthorityIndex,
        limit: usize,
    ) -> Vec<Data<StatementBlock>> {
        let entries = self
            .inner
            .read()
            .get_others_blocks(from_excluded, authority, limit);
        self.read_index_vec(entries)
    }

    pub fn last_seen_by_authority(&self, authority: AuthorityIndex) -> RoundNumber {
        self.inner.read().last_seen_by_authority(authority)
    }

    pub fn last_own_block_ref(&self) -> Option<BlockReference> {
        self.inner.read().last_own_block()
    }

    /// Check whether `earlier_block` is an ancestor of `later_block`.
    pub fn linked(
        &self,
        later_block: &Data<StatementBlock>,
        earlier_block: &Data<StatementBlock>,
    ) -> bool {
        let mut parents = vec![later_block.clone()];
        for r in (earlier_block.round()..later_block.round()).rev() {
            parents = self
                .get_blocks_by_round(r)
                .into_iter()
                .filter(|block| {
                    parents
                        .iter()
                        .any(|x| x.includes().contains(block.reference()))
                })
                .collect();
        }
        parents.contains(earlier_block)
    }

    fn read_index(&self, entry: IndexEntry) -> Data<StatementBlock> {
        match entry {
            IndexEntry::WalPosition(position) => {
                self.metrics.inc_block_store_loaded_blocks();
                let (tag, data) = self.wal_reader.read(position).expect("Failed to read wal");
                match tag {
                    WAL_ENTRY_BLOCK => {
                        Data::from_bytes(data).expect("Failed to deserialize data from wal")
                    }
                    WAL_ENTRY_OWN_BLOCK => {
                        OwnBlockData::from_bytes(data)
                            .expect("Failed to deserialized own block from wal")
                            .1
                    }
                    _ => {
                        panic!("Trying to load index entry at position {position}, found tag {tag}")
                    }
                }
            }
            IndexEntry::Loaded(_, block) => block,
        }
    }

    fn read_index_vec(&self, entries: Vec<IndexEntry>) -> Vec<Data<StatementBlock>> {
        entries
            .into_iter()
            .map(|pos| self.read_index(pos))
            .collect()
    }
}
