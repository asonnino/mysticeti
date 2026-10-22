// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    sync::Arc,
};

use crate::{
    block::{Block, BlockReference},
    committee::Committee,
    data::Data,
    storage::BlockReader,
    storage::Storage,
    wal::WalPosition,
};

/// Block manager suspends incoming blocks until they are connected to the existing graph,
/// returning newly connected blocks
pub struct BlockManager {
    /// Keeps all pending blocks.
    blocks_pending: HashMap<BlockReference, Data<Block>>,
    /// Keeps all the blocks (`BTreeSet<BlockReference>`) waiting
    /// for `BlockReference` to be processed. Ordered so that blocks are unblocked in the same
    /// order on every run: iterating a `HashSet` would make the processing order, and with it the
    /// simulator, depend on the process-random hasher.
    block_references_waiting: HashMap<BlockReference, BTreeSet<BlockReference>>,
    /// Keeps all blocks that need to be synced in order to unblock the processing of other pending
    /// blocks. The indices of the vector correspond the authority indices.
    missing: Vec<HashSet<BlockReference>>,
    block_reader: BlockReader,
}

impl BlockManager {
    pub fn new(block_reader: BlockReader, committee: &Arc<Committee>) -> Self {
        Self {
            blocks_pending: Default::default(),
            block_references_waiting: Default::default(),
            missing: (0..committee.len()).map(|_| HashSet::new()).collect(),
            block_reader,
        }
    }

    pub fn add_blocks(
        &mut self,
        blocks: Vec<Data<Block>>,
        storage: &mut Storage,
    ) -> Vec<(WalPosition, Data<Block>)> {
        let mut blocks: VecDeque<Data<Block>> = blocks.into();
        let mut newly_blocks_processed: Vec<(WalPosition, Data<Block>)> = vec![];
        while let Some(block) = blocks.pop_front() {
            // Update the highest known round number.

            // check whether we have already processed this block and skip it if so.
            let block_reference = block.reference();
            if self.block_reader.block_exists(*block_reference)
                || self.blocks_pending.contains_key(block_reference)
            {
                continue;
            }

            let mut processed = true;
            for included_reference in block.includes() {
                // If we are missing a reference then we insert
                // into pending and update the waiting index
                if !self.block_reader.block_exists(*included_reference) {
                    processed = false;
                    self.block_references_waiting
                        .entry(*included_reference)
                        .or_default()
                        .insert(*block_reference);
                    if !self.blocks_pending.contains_key(included_reference) {
                        self.missing[included_reference.authority.index()]
                            .insert(*included_reference);
                    }
                }
            }
            self.missing[block_reference.authority.index()].remove(block_reference);

            if !processed {
                self.blocks_pending.insert(*block_reference, block);
            } else {
                let block_reference = *block_reference;

                // Block can be processed. So need to update indexes etc
                let position = storage.insert_block(block.clone());
                newly_blocks_processed.push((position, block.clone()));

                // Now unlock any pending blocks, and process them if ready.
                if let Some(waiting_references) =
                    self.block_references_waiting.remove(&block_reference)
                {
                    // For each reference see if its unblocked.
                    for waiting_block_reference in waiting_references {
                        let block_pointer = self
                            .blocks_pending
                            .get(&waiting_block_reference)
                            .expect("Block waiting ref must exist");

                        if block_pointer
                            .includes()
                            .iter()
                            .all(|item_ref| !self.block_references_waiting.contains_key(item_ref))
                        {
                            // No dependencies left, move to
                            // processing queue.
                            let block = self
                                .blocks_pending
                                .remove(&waiting_block_reference)
                                .expect("Block waiting ref must exist");
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
}

#[cfg(test)]
mod tests {
    use rand::{SeedableRng, prelude::StdRng};

    use super::*;
    use crate::block::test::Dag;

    #[test]
    fn test_block_manager_add_block() {
        let dag =
            Dag::draw("A1:[A0, B0]; B1:[A0, B0]; B2:[A0, B1]; A2:[A1, B2]").add_genesis_blocks();
        assert_eq!(dag.len(), 6); // 4 blocks in dag + 2 genesis
        for seed in 0..100u8 {
            let mut storage = Storage::new_for_test(&dag.committee());
            println!("Seed {seed}");
            let iter = dag.random_iter(&mut rng(seed));
            let mut bm = BlockManager::new(storage.block_reader().clone(), &dag.committee());
            let mut processed_blocks = HashSet::new();
            for block in iter {
                let processed = bm.add_blocks(vec![block.clone()], &mut storage);
                print!("Adding {:?}:", block.reference());
                for (_, p) in processed {
                    print!("{:?},", p.reference());
                    if !processed_blocks.insert(*p.reference()) {
                        panic!("Block {:?} processed twice", p.reference());
                    }
                }
                println!();
            }
            assert_eq!(bm.block_references_waiting.len(), 0);
            assert_eq!(bm.blocks_pending.len(), 0);
            assert_eq!(processed_blocks.len(), dag.len());
            assert_eq!(bm.block_reader.len_expensive(), dag.len());
            println!("======");
        }
    }

    #[test]
    fn test_block_manager_unblocks_waiting_blocks_in_a_fixed_order() {
        // Five blocks wait on A1. Which of them is processed first must not depend on the
        // hasher's random keys, so a fresh manager must give the same order every time.
        let dag = "A0:[]; A1:[A0]; B2:[A1]; C2:[A1]; D2:[A1]; E2:[A1]; F2:[A1]";
        let blocks: Vec<_> = dag
            .split(';')
            .map(|block| Data::new(Dag::draw_block(block)))
            .collect();
        let committee = Committee::new_test(vec![1; 6]);

        let mut orders = HashSet::new();
        for _ in 0..32 {
            let mut storage = Storage::new_for_test(&committee);
            let mut bm = BlockManager::new(storage.block_reader().clone(), &committee);
            bm.add_blocks(vec![blocks[0].clone()], &mut storage);
            bm.add_blocks(blocks[2..].to_vec(), &mut storage);
            let order: Vec<_> = bm
                .add_blocks(vec![blocks[1].clone()], &mut storage)
                .iter()
                .map(|(_, block)| *block.reference())
                .collect();
            assert_eq!(order.len(), 6);
            orders.insert(order);
        }
        assert_eq!(orders.len(), 1, "unblock order changed between runs");
    }

    fn rng(s: u8) -> StdRng {
        let mut seed = [0; 32];
        seed[0] = s;
        StdRng::from_seed(seed)
    }
}
