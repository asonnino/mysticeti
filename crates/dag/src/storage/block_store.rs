// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    cmp::max,
    collections::{BTreeMap, HashMap},
    io::IoSlice,
};

use minibytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    consensus::CommittedSubDag,
    data::Data,
    types::{Authority, BlockDigest, BlockReference, RoundNumber, StatementBlock},
    wal::{Tag, WalPosition, WalWriter},
};

pub use super::reader::BlockReader;

#[derive(Default)]
pub(super) struct BlockStore {
    pub(super) index: BTreeMap<RoundNumber, HashMap<(Authority, BlockDigest), IndexEntry>>,
    pub(super) own_blocks: BTreeMap<RoundNumber, BlockDigest>,
    pub(super) highest_round: RoundNumber,
    pub(super) authority: Authority,
    pub(super) last_seen_by_authority: Vec<RoundNumber>,
    pub(super) last_own_block: Option<BlockReference>,
}

#[derive(Clone)]
pub(super) enum IndexEntry {
    WalPosition(WalPosition),
    Loaded(WalPosition, Data<StatementBlock>),
}

impl BlockStore {
    pub(super) fn block_exists(&self, reference: BlockReference) -> bool {
        let Some(blocks) = self.index.get(&reference.round) else {
            return false;
        };
        blocks.contains_key(&(reference.authority, reference.digest))
    }

    pub(super) fn block_exists_at_authority_round(
        &self,
        authority: Authority,
        round: RoundNumber,
    ) -> bool {
        let Some(blocks) = self.index.get(&round) else {
            return false;
        };
        blocks
            .keys()
            .any(|(block_authority, _)| *block_authority == authority)
    }

    pub(super) fn all_blocks_exists_at_authority_round(
        &self,
        mut authorities: impl Iterator<Item = Authority>,
        round: RoundNumber,
    ) -> bool {
        let Some(blocks) = self.index.get(&round) else {
            return false;
        };
        authorities.all(|authority| {
            blocks
                .keys()
                .any(|(block_authority, _)| *block_authority == authority)
        })
    }

    pub(super) fn get_blocks_at_authority_round(
        &self,
        authority: Authority,
        round: RoundNumber,
    ) -> Vec<IndexEntry> {
        let Some(blocks) = self.index.get(&round) else {
            return vec![];
        };
        blocks
            .iter()
            .filter_map(|((a, _), entry)| {
                if *a == authority {
                    Some(entry.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub(super) fn get_blocks_by_round(&self, round: RoundNumber) -> Vec<IndexEntry> {
        let Some(blocks) = self.index.get(&round) else {
            return vec![];
        };
        blocks.values().cloned().collect()
    }

    pub(super) fn get_block(&self, reference: BlockReference) -> Option<IndexEntry> {
        self.index
            .get(&reference.round)?
            .get(&(reference.authority, reference.digest))
            .cloned()
    }

    pub(super) fn unload_below_round(&mut self, threshold_round: RoundNumber) -> usize {
        let mut unloaded = 0usize;
        for (round, map) in self.index.iter_mut() {
            if *round > threshold_round {
                continue;
            }
            for entry in map.values_mut() {
                match entry {
                    IndexEntry::WalPosition(_) => {}
                    IndexEntry::Loaded(position, _) => {
                        unloaded += 1;
                        *entry = IndexEntry::WalPosition(*position);
                    }
                }
            }
        }
        if unloaded > 0 {
            tracing::debug!("Unloaded {unloaded} entries from block store cache");
        }
        unloaded
    }

    pub(super) fn add_unloaded(&mut self, reference: &BlockReference, position: WalPosition) {
        self.highest_round = max(self.highest_round, reference.round());
        let map = self.index.entry(reference.round()).or_default();
        map.insert(reference.author_digest(), IndexEntry::WalPosition(position));
        self.add_own_index(reference);
        self.update_last_seen_by_authority(reference);
    }

    pub(super) fn add_loaded(&mut self, position: WalPosition, block: Data<StatementBlock>) {
        self.highest_round = max(self.highest_round, block.round());
        self.add_own_index(block.reference());
        self.update_last_seen_by_authority(block.reference());
        let map = self.index.entry(block.round()).or_default();
        map.insert(
            (block.author(), block.digest()),
            IndexEntry::Loaded(position, block),
        );
    }

    pub(super) fn last_seen_by_authority(&self, authority: Authority) -> RoundNumber {
        *self
            .last_seen_by_authority
            .get(authority.index())
            .expect("last_seen_by_authority not found")
    }

    fn update_last_seen_by_authority(&mut self, reference: &BlockReference) {
        let last_seen = self
            .last_seen_by_authority
            .get_mut(reference.authority.index())
            .expect("last_seen_by_authority not found");
        if reference.round() > *last_seen {
            *last_seen = reference.round();
        }
    }

    pub(super) fn get_own_blocks(
        &self,
        from_excluded: RoundNumber,
        limit: usize,
    ) -> Vec<IndexEntry> {
        self.own_blocks
            .range((from_excluded + 1)..)
            .take(limit)
            .map(|(round, digest)| {
                let reference = BlockReference {
                    authority: self.authority,
                    round: *round,
                    digest: *digest,
                };
                if let Some(block) = self.get_block(reference) {
                    block
                } else {
                    panic!(
                        "Own block index corrupted, not found: \
                        {reference}"
                    );
                }
            })
            .collect()
    }

    pub(super) fn get_others_blocks(
        &self,
        from_excluded: RoundNumber,
        authority: Authority,
        limit: usize,
    ) -> Vec<IndexEntry> {
        self.index
            .range((from_excluded + 1)..)
            .take(limit)
            .flat_map(|(round, map)| {
                map.keys()
                    .filter(|(a, _)| *a == authority)
                    .map(|(a, d)| BlockReference {
                        authority: *a,
                        round: *round,
                        digest: *d,
                    })
            })
            .map(|reference| {
                self.get_block(reference).unwrap_or_else(|| {
                    panic!(
                        "Block index corrupted, not found: \
                        {reference}"
                    )
                })
            })
            .collect()
    }

    fn add_own_index(&mut self, reference: &BlockReference) {
        if reference.authority != self.authority {
            return;
        }
        if reference.round > self.last_own_block.map(|r| r.round).unwrap_or_default() {
            self.last_own_block = Some(*reference);
        }
        assert!(
            self.own_blocks
                .insert(reference.round, reference.digest)
                .is_none()
        );
    }

    pub(super) fn last_own_block(&self) -> Option<BlockReference> {
        self.last_own_block
    }

    #[cfg(test)]
    pub(super) fn len_expensive(&self) -> usize {
        self.index.values().map(HashMap::len).sum()
    }
}

pub(super) const WAL_ENTRY_BLOCK: Tag = 1;
pub(super) const WAL_ENTRY_PAYLOAD: Tag = 2;
pub(super) const WAL_ENTRY_OWN_BLOCK: Tag = 3;
pub(super) const WAL_ENTRY_STATE: Tag = 4;
// Commit entry includes both commit interpreter incremental
// state and committed transactions aggregator.
// todo - They could be separated for better performance, but
// this will require catching up for committed transactions
// aggregator state.
pub(super) const WAL_ENTRY_COMMIT: Tag = 5;

// This data structure has a special serialization in/from
// Bytes, see OwnBlockData::from_bytes/write_to_wal
pub struct OwnBlockData {
    pub next_entry: WalPosition,
    pub block: Data<StatementBlock>,
}

const OWN_BLOCK_HEADER_SIZE: usize = 8;

impl OwnBlockData {
    pub(super) fn from_bytes(
        bytes: Bytes,
    ) -> bincode::Result<(OwnBlockData, Data<StatementBlock>)> {
        let next_entry = &bytes[..OWN_BLOCK_HEADER_SIZE];
        let next_entry: WalPosition = bincode::deserialize(next_entry)?;
        let block = bytes.slice(OWN_BLOCK_HEADER_SIZE..);
        let block = Data::<StatementBlock>::from_bytes(block)?;
        let own_block_data = OwnBlockData {
            next_entry,
            block: block.clone(),
        };
        Ok((own_block_data, block))
    }

    pub(super) fn write_to_wal(&self, writer: &mut WalWriter) -> WalPosition {
        let header = bincode::serialize(&self.next_entry).expect("Serialization failed");
        let header = IoSlice::new(&header);
        let block = IoSlice::new(self.block.serialized_bytes());
        writer
            .writev(WAL_ENTRY_OWN_BLOCK, &[header, block])
            .expect("Writing to wal failed")
    }
}

#[derive(Serialize, Deserialize)]
pub struct CommitData {
    pub leader: BlockReference,
    // All committed blocks, including the leader
    pub sub_dag: Vec<BlockReference>,
}

impl From<&CommittedSubDag> for CommitData {
    fn from(value: &CommittedSubDag) -> Self {
        let sub_dag = value.blocks.iter().map(|b| *b.reference()).collect();
        Self {
            leader: value.anchor,
            sub_dag,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn own_block_serialization_test() {
        let next_entry = WalPosition::default();
        let serialized = bincode::serialize(&next_entry).unwrap();
        assert_eq!(serialized.len(), OWN_BLOCK_HEADER_SIZE);
    }
}
