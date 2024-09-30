// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use crate::{
    aux_helper::aux_block_store::AuxiliaryBlockStore,
    block_store::{BlockStore, BlockWriter},
    committee::Committee,
    data::Data,
    types::{AuthorityIndex, BlockReference, StatementBlock},
    wal::WalPosition,
};

/// Block manager suspends incoming blocks until they are connected to the existing graph,
/// returning newly connected blocks
pub struct BlockManager {
    /// Keeps all pending blocks.
    blocks_pending: HashMap<BlockReference, Data<StatementBlock>>,
    /// Keeps all the blocks (`HashSet<BlockReference>`) waiting for `BlockReference` to be processed.
    block_references_waiting: HashMap<BlockReference, HashSet<BlockReference>>,
    /// Keeps all blocks that need to be synced in order to unblock the processing of other pending
    /// blocks. The indices of the vector correspond the authority indices.
    missing: Vec<HashSet<BlockReference>>,
    block_store: BlockStore,

    aux_missing: HashMap<AuthorityIndex, HashSet<BlockReference>>,
    aux_block_store: AuxiliaryBlockStore,
}

impl BlockManager {
    pub fn new(
        block_store: BlockStore,
        committee: &Arc<Committee>,
        aux_block_store: AuxiliaryBlockStore,
    ) -> Self {
        Self {
            blocks_pending: Default::default(),
            block_references_waiting: Default::default(),
            missing: (0..committee.len()).map(|_| HashSet::new()).collect(),
            block_store,
            aux_missing: Default::default(),
            aux_block_store,
        }
    }

    pub fn add_blocks(
        &mut self,
        blocks: Vec<Data<StatementBlock>>,
        block_writer: &mut impl BlockWriter,
    ) -> Vec<(WalPosition, Data<StatementBlock>)> {
        let mut blocks: VecDeque<Data<StatementBlock>> = blocks.into();
        let mut newly_blocks_processed: Vec<(WalPosition, Data<StatementBlock>)> = vec![];
        while let Some(block) = blocks.pop_front() {
            // Update the highest known round number.

            // check whether we have already processed this block and skip it if so.
            let block_reference = block.reference();
            if self.block_store.block_exists(*block_reference)
                || self.blocks_pending.contains_key(block_reference)
            {
                continue;
            }

            let mut processed = true;
            for included_reference in block.includes() {
                // If we are missing a reference then we insert into pending and update the waiting index
                if !self.block_store.block_exists(*included_reference) {
                    processed = false;
                    self.block_references_waiting
                        .entry(*included_reference)
                        .or_default()
                        .insert(*block_reference);
                    if !self.blocks_pending.contains_key(included_reference) {
                        self.missing[included_reference.authority as usize]
                            .insert(*included_reference);
                    }
                }
            }
            self.missing[block_reference.authority as usize].remove(block_reference);

            // // Check if the block has any weak links that are missing.
            // for aux_link in block.aux_includes() {
            //     if !self.aux_block_store.block_exists(aux_link) {
            //         processed = false;
            //         self.block_references_waiting
            //             .entry(*aux_link)
            //             .or_default()
            //             .insert(*block_reference);
            //         if !self.blocks_pending.contains_key(aux_link) {
            //             self.aux_missing
            //                 .entry(aux_link.authority)
            //                 .or_insert_with(HashSet::new)
            //                 .insert(*aux_link);
            //         }
            //     }
            // }
            // self.aux_missing
            //     .get_mut(&block_reference.authority)
            //     .map(|x| x.remove(block_reference));

            if !processed {
                self.blocks_pending.insert(*block_reference, block);
            } else {
                let block_reference = *block_reference;

                // Block can be processed. So need to update indexes etc
                let position = block_writer.insert_block(block.clone());
                newly_blocks_processed.push((position, block.clone()));

                // Now unlock any pending blocks, and process them if ready.
                if let Some(waiting_references) =
                    self.block_references_waiting.remove(&block_reference)
                {
                    // For each reference see if its unblocked.
                    for waiting_block_reference in waiting_references {
                        let block_pointer = self.blocks_pending.get(&waiting_block_reference).expect("Safe since we ensure the block waiting reference has a valid primary key.");

                        if block_pointer
                            .includes()
                            .iter()
                            .all(|item_ref| !self.block_references_waiting.contains_key(item_ref))
                        {
                            // No dependencies are left unprocessed, so remove from unprocessed list, and add to the
                            // blocks we are processing now.
                            let block = self.blocks_pending.remove(&waiting_block_reference).expect("Safe since we ensure the block waiting reference has a valid primary key.");
                            blocks.push_front(block);
                        }
                    }
                }
            }
        }

        newly_blocks_processed
    }

    pub fn missing_blocks(&self) -> &[HashSet<BlockReference>] {
        &self.missing
    }

    pub fn aux_missing_blocks(&self) -> &HashMap<AuthorityIndex, HashSet<BlockReference>> {
        &self.aux_missing
    }

    /// Unblock the processing of core blocks upon receiving a new auxiliary block (that was potentially used as a weak link).
    pub fn unlock_pending_block(
        &mut self,
        aux_block_reference: BlockReference,
        block_writer: &mut impl BlockWriter,
    ) {
        if let Some(waiting_references) = self.block_references_waiting.remove(&aux_block_reference)
        {
            // For each reference see if its unblocked.
            for waiting_block_reference in waiting_references {
                let block_pointer = self.blocks_pending.get(&waiting_block_reference).expect(
                    "Safe since we ensure the block waiting reference has a valid primary key.",
                );

                if block_pointer
                    .aux_includes()
                    .iter()
                    .all(|item_ref| !self.block_references_waiting.contains_key(item_ref))
                {
                    // No dependencies are left unprocessed, so remove from unprocessed list, and add to the
                    // blocks we are processing now.
                    let block = self.blocks_pending.remove(&waiting_block_reference).expect(
                        "Safe since we ensure the block waiting reference has a valid primary key.",
                    );
                    self.add_blocks(vec![block], block_writer);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::{prelude::StdRng, SeedableRng};

    use super::*;
    use crate::{aux_node::aux_config::AuxiliaryCommittee, test_util::TestBlockWriter, types::Dag};

    #[test]
    fn test_block_manager_add_block() {
        let dag =
            Dag::draw("A1:[A0, B0]; B1:[A0, B0]; B2:[A0, B1]; A2:[A1, B2]").add_genesis_blocks();
        assert_eq!(dag.len(), 6); // 4 blocks in dag + 2 genesis

        let aux_committee = Arc::new(AuxiliaryCommittee::default());
        let aux_node_parameters = Default::default();
        let aux_block_store = AuxiliaryBlockStore::new(aux_committee, aux_node_parameters);

        for seed in 0..100u8 {
            let mut block_writer = TestBlockWriter::new(&dag.committee());
            println!("Seed {seed}");
            let iter = dag.random_iter(&mut rng(seed));
            let mut bm = BlockManager::new(
                block_writer.block_store(),
                &dag.committee(),
                aux_block_store.clone(),
            );
            let mut processed_blocks = HashSet::new();
            for block in iter {
                let processed = bm.add_blocks(vec![block.clone()], &mut block_writer);
                print!("Adding {:?}:", block.reference());
                for (_, p) in processed {
                    print!("{:?},", p.reference());
                    if !processed_blocks.insert(p.reference().clone()) {
                        panic!("Block {:?} processed twice", p.reference());
                    }
                }
                println!();
            }
            assert_eq!(bm.block_references_waiting.len(), 0);
            assert_eq!(bm.blocks_pending.len(), 0);
            assert_eq!(processed_blocks.len(), dag.len());
            assert_eq!(bm.block_store.len_expensive(), dag.len());
            println!("======");
        }
    }

    fn rng(s: u8) -> StdRng {
        let mut seed = [0; 32];
        seed[0] = s;
        StdRng::from_seed(seed)
    }
}
