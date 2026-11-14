// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Seeded random DAGs for tests that compare two ways of computing the same answer.

use dag::{
    block::{Block, RoundNumber},
    committee::Committee,
    crypto::{BLOCK_DIGEST_SIZE, BlockDigest},
    data::Data,
    storage::Storage,
    test_util::{build_dag, insert_test_block},
};

/// A small deterministic generator (an LCG), so the tests need no extra dependency.
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0xd1b5_4a32_d192_ed03)
    }

    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }

    /// A number in `0..bound`.
    pub(crate) fn below(&mut self, bound: usize) -> usize {
        self.next() as usize % bound
    }

    pub(crate) fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }

    /// Up to `count` distinct items of `items`, in random order.
    fn sample<T: Copy>(&mut self, items: &[T], count: usize) -> Vec<T> {
        let mut pool = items.to_vec();
        let mut chosen = Vec::new();
        while chosen.len() < count && !pool.is_empty() {
            let index = self.below(pool.len());
            chosen.push(pool.swap_remove(index));
        }
        chosen
    }
}

/// How the random DAG departs from a fully connected one.
#[derive(Clone, Copy)]
pub(crate) struct Shape {
    /// Chance, in percent, that an authority proposes nothing in a round.
    pub(crate) silent: u64,
    /// Chance, in percent, that an authority proposes two different blocks in a round.
    pub(crate) equivocate: u64,
    /// Chance, in percent, that a block also links to older blocks.
    pub(crate) weak_links: u64,
    /// Fewest blocks of the previous round a block links to, as a percentage of them.
    pub(crate) min_links: usize,
}

/// Builds a random DAG round by round and calls `after_insert` once per inserted block, in
/// a random arrival order within each round. Every block links only to blocks already stored.
pub(crate) fn build_random_dag(
    committee: &Committee,
    storage: &mut Storage,
    seed: u64,
    rounds: RoundNumber,
    shape: Shape,
    mut after_insert: impl FnMut(&Storage, &Data<Block>),
) {
    let mut rng = Rng::new(seed);
    let mut layers = vec![build_dag(committee, storage, None, 0)];
    let mut twins = 0u64;
    for round in 1..=rounds {
        let previous = layers[round as usize - 1].clone();
        let mut proposals = Vec::new();
        for authority in committee.authorities() {
            if rng.chance(shape.silent) {
                continue;
            }
            let copies = if rng.chance(shape.equivocate) { 2 } else { 1 };
            for copy in 0..copies {
                let fewest = (previous.len() * shape.min_links).div_ceil(100).max(1);
                let count = fewest + rng.below(previous.len() - fewest + 1);
                let mut includes = rng.sample(&previous, count);
                if round >= 3 && rng.chance(shape.weak_links) {
                    for _ in 0..1 + rng.below(2) {
                        let older = &layers[rng.below(round as usize - 1)];
                        let reference = older[rng.below(older.len())];
                        if !includes.contains(&reference) {
                            includes.push(reference);
                        }
                    }
                }
                let mut block = Block::new_for_test(authority, round, includes);
                if copy == 1 {
                    twins += 1;
                    let mut digest = [0u8; BLOCK_DIGEST_SIZE];
                    digest[..8].copy_from_slice(&twins.to_le_bytes());
                    digest[BLOCK_DIGEST_SIZE - 1] = 0xee;
                    block = block.with_digest(BlockDigest::from(digest));
                }
                proposals.push(block);
            }
        }
        for index in (1..proposals.len()).rev() {
            proposals.swap(index, rng.below(index + 1));
        }
        let mut layer = Vec::new();
        for block in proposals {
            let reference = insert_test_block(block, storage);
            layer.push(reference);
            let block = storage
                .block_reader()
                .get_block(reference)
                .expect("the block was just inserted");
            after_insert(storage, &block);
        }
        if layer.is_empty() {
            // Nobody proposed: carry the previous layer so the next round has something to link.
            layer = previous;
        }
        layers.push(layer);
    }
}

/// Shapes used by the differential tests: a dense DAG, a sparse one, and one with Byzantine
/// authors.
pub(crate) fn shapes() -> [Shape; 3] {
    [
        Shape {
            silent: 0,
            equivocate: 0,
            weak_links: 0,
            min_links: 100,
        },
        Shape {
            silent: 10,
            equivocate: 0,
            weak_links: 20,
            min_links: 67,
        },
        Shape {
            silent: 10,
            equivocate: 6,
            weak_links: 20,
            min_links: 75,
        },
    ]
}
