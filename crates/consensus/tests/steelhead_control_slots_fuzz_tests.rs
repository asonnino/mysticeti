// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Differential fuzzing of adaptive Steelhead's control slots. A control slot
//! is a round that carries a coin, defined by rule: the async rounds of the
//! interval under scan, then the multiples of `max_period` above its boundary.
//! The set therefore changes from one scan to the next, so the period schedule
//! must not depend on how a committer came to see the DAG. Each run builds a
//! random DAG (with starved leaders and dropped references) and feeds it to
//! committers all at once, block by block, in shuffled arrival order with
//! irregular `try_commit` calls, and as a partial sub-DAG; all must agree on
//! the output verdicts, the agreed output, the per-round wavelengths, and the
//! stall fallbacks.

use std::{
    collections::HashSet,
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
};

use consensus::{
    committer::Committer,
    protocol::{AdaptiveConfig, ConsensusProtocol, SteelheadPair},
};
use dag::{
    authority::Authority,
    block::{Block, BlockReference, RoundNumber},
    committee::Committee,
    consensus::LeaderStatus,
    data::Data,
    storage::Storage,
    test_util::{committee, insert_test_block},
};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

type Verdict = (RoundNumber, Authority, Option<BlockReference>);

fn verdict(status: &LeaderStatus) -> Verdict {
    let committed = match status {
        LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) => {
            Some(*block.reference())
        }
        _ => None,
    };
    (status.round(), status.authority(), committed)
}

fn spec(
    interval: u64,
    max_period: u64,
    async_wave_length: u64,
    leader_count: usize,
) -> ConsensusProtocol {
    ConsensusProtocol::Steelhead {
        pair: SteelheadPair::MysticetiMahiMahi,
        period: None,
        async_wave_length,
        adaptive: Some(AdaptiveConfig {
            interval,
            max_period: NonZeroU64::new(max_period).unwrap(),
            epsilon_percent: 10,
        }),
        canary: NonZeroU64::new(1),
        leader_count: NonZeroUsize::new(leader_count).unwrap(),
    }
}

/// A random DAG: every round keeps at least n - f blocks; every block
/// references at least n - f blocks of the previous round. `hostility` is the
/// percentage of "bad" choices (missing blocks, dropped references).
fn random_dag(
    committee: &Arc<Committee>,
    depth: RoundNumber,
    hostility: u64,
    rng: &mut Rng,
) -> (Vec<Data<Block>>, Vec<Data<Block>>) {
    let n = committee.len();
    let f = (n - 1) / 3;
    let genesis: Vec<Data<Block>> = committee.authorities().map(Block::genesis).collect();
    let mut previous: Vec<BlockReference> = genesis.iter().map(|b| *b.reference()).collect();
    let mut blocks = Vec::new();
    for round in 1..=depth {
        // Up to f authorities miss the round.
        let mut missing = HashSet::new();
        for _ in 0..f {
            if rng.chance(hostility) {
                missing.insert(rng.below(n as u64));
            }
        }
        // A per-round victim whose previous-round block tends to be dropped.
        let victim = rng.below(n as u64);
        let mut current = Vec::new();
        for authority in committee.authorities() {
            if missing.contains(&(authority.as_u64_for_scratch())) {
                continue;
            }
            let mut parents: Vec<BlockReference> = previous.clone();
            // Drop up to |previous| - (n - f) references.
            let mut droppable = parents.len().saturating_sub(n - f);
            if droppable > 0
                && rng.chance(hostility * 2)
                && let Some(position) = parents
                    .iter()
                    .position(|r| r.authority.as_u64_for_scratch() == victim)
            {
                parents.remove(position);
                droppable -= 1;
            }
            while droppable > 0 && rng.chance(hostility) {
                let position = rng.below(parents.len() as u64) as usize;
                parents.remove(position);
                droppable -= 1;
            }
            // Shuffle the include order a little (find_support is order-sensitive).
            if rng.chance(30) && parents.len() > 1 {
                let a = rng.below(parents.len() as u64) as usize;
                let b = rng.below(parents.len() as u64) as usize;
                parents.swap(a, b);
            }
            let block = Data::new(Block::new_for_test(authority, round, parents));
            current.push(*block.reference());
            blocks.push(block);
        }
        previous = current;
    }
    (genesis, blocks)
}

trait ScratchAuthority {
    fn as_u64_for_scratch(&self) -> u64;
}
impl ScratchAuthority for Authority {
    fn as_u64_for_scratch(&self) -> u64 {
        // Authority displays as a letter/number; recover the index by search.
        (0..1024u64)
            .find(|index| Authority::new(*index) == *self)
            .expect("authority index")
    }
}

struct View {
    storage: Storage,
    committer: Committer,
    output: Vec<Verdict>,
    last_decided: Option<(RoundNumber, Authority)>,
}

impl View {
    fn new(committee: &Arc<Committee>, spec: &ConsensusProtocol, genesis: &[Data<Block>]) -> Self {
        let mut storage = Storage::new_for_test(committee);
        for block in genesis {
            insert_test_block(Block::clone(block), &mut storage);
        }
        let committer = Committer::new_for_test(committee, &storage, spec);
        Self {
            storage,
            committer,
            output: Vec::new(),
            last_decided: None,
        }
    }

    fn step(&mut self, chunk: usize) {
        loop {
            let step: Vec<_> = self
                .committer
                .try_commit(self.last_decided)
                .take(chunk)
                .collect();
            let Some(last) = step.last() else {
                return;
            };
            self.last_decided = Some((last.round(), last.authority()));
            self.output.extend(step.iter().map(verdict));
            if chunk == usize::MAX {
                return;
            }
        }
    }
}

fn assert_consistent(tag: &str, a: &View, b: &View) {
    let common = a.output.len().min(b.output.len());
    assert_eq!(
        &a.output[..common],
        &b.output[..common],
        "{tag}: output diverges"
    );
    let agreed_a: Vec<_> = a.committer.agreed_output().iter().map(verdict).collect();
    let agreed_b: Vec<_> = b.committer.agreed_output().iter().map(verdict).collect();
    let common_agreed = agreed_a.len().min(agreed_b.len());
    assert_eq!(
        &agreed_a[..common_agreed],
        &agreed_b[..common_agreed],
        "{tag}: agreed output diverges"
    );
    // The agreed output is a prefix-consistent shadow of the output.
    for (view, agreed) in [(a, &agreed_a), (b, &agreed_b)] {
        let shared = agreed.len().min(view.output.len());
        assert_eq!(
            &agreed[..shared],
            &view.output[..shared],
            "{tag}: agreed vs output"
        );
    }
    // Wavelengths (hence periods) agree on every round both have output.
    let top = a
        .output
        .last()
        .map_or(0, |v| v.0)
        .min(b.output.last().map_or(0, |v| v.0));
    for round in 1..=top {
        assert_eq!(
            a.committer.wave_length_at(round),
            b.committer.wave_length_at(round),
            "{tag}: wavelength diverges at round {round}"
        );
    }
}

fn fuzz_one(seed: u64, n: usize, interval: u64, max_period: u64, wave: u64, leaders: usize) {
    let mut rng = Rng(seed);
    let committee = committee(n);
    let spec = spec(interval, max_period, wave, leaders);
    let depth = 3 * interval + 40;
    let hostility = [0, 10, 25, 40, 60][(seed % 5) as usize];
    let (genesis, blocks) = random_dag(&committee, depth, hostility, &mut rng);
    let tag = format!(
        "seed {seed} n {n} interval {interval} max {max_period} wave {wave} leaders {leaders}"
    );

    // View A: the whole DAG at once.
    let mut full = View::new(&committee, &spec, &genesis);
    for block in &blocks {
        insert_test_block(Block::clone(block), &mut full.storage);
    }
    full.step(usize::MAX);

    // View B: round by round, committing after each block.
    let mut incremental = View::new(&committee, &spec, &genesis);
    for block in &blocks {
        insert_test_block(Block::clone(block), &mut incremental.storage);
        incremental.step(usize::MAX);
    }

    // View C: a different causal arrival order, random cadence and chunks.
    let mut shuffled = View::new(&committee, &spec, &genesis);
    let mut inserted: HashSet<BlockReference> = genesis.iter().map(|b| *b.reference()).collect();
    let mut pending: Vec<Data<Block>> = blocks.clone();
    while !pending.is_empty() {
        // Pick a random insertable block among the first few rounds pending.
        let candidates: Vec<usize> = pending
            .iter()
            .enumerate()
            .filter(|(_, b)| b.includes().iter().all(|r| inserted.contains(r)))
            .map(|(index, _)| index)
            .take(3 * n)
            .collect();
        let pick = candidates[rng.below(candidates.len() as u64) as usize];
        let block = pending.remove(pick);
        inserted.insert(*block.reference());
        insert_test_block(Block::clone(&block), &mut shuffled.storage);
        if rng.chance(15) {
            let chunk = [1usize, 2, 5, usize::MAX][rng.below(4) as usize];
            shuffled.step(chunk);
            assert_consistent(&format!("{tag} (mid)"), &full, &shuffled);
        }
    }
    shuffled.step(usize::MAX);

    // View D: a causally closed strict sub-DAG (drops the top rounds of some authors).
    let cut = depth - rng.below(interval) - 1;
    let mut partial = View::new(&committee, &spec, &genesis);
    let lagging = rng.below(n as u64);
    let mut have: HashSet<BlockReference> = genesis.iter().map(|b| *b.reference()).collect();
    for block in &blocks {
        let lag = block.author().as_u64_for_scratch() == lagging && block.round() + 6 > cut;
        if block.round() > cut || lag || !block.includes().iter().all(|r| have.contains(r)) {
            continue;
        }
        have.insert(*block.reference());
        insert_test_block(Block::clone(block), &mut partial.storage);
        if rng.chance(5) {
            partial.step(usize::MAX);
        }
    }
    partial.step(3);

    assert_consistent(&tag, &full, &incremental);
    assert_consistent(&tag, &full, &shuffled);
    assert_consistent(&tag, &full, &partial);
    assert_eq!(
        full.output, incremental.output,
        "{tag}: same DAG, same output"
    );
    assert_eq!(full.output, shuffled.output, "{tag}: same DAG, same output");
    assert_eq!(
        full.committer.stall_fallbacks(),
        incremental.committer.stall_fallbacks(),
        "{tag}"
    );
    assert_eq!(
        full.committer.stall_fallbacks(),
        shuffled.committer.stall_fallbacks(),
        "{tag}"
    );
}

#[test]
fn differential_fuzz() {
    let configs: [(usize, u64, u64, u64, usize); 9] = [
        (4, 16, 8, 5, 1),
        (4, 17, 8, 5, 1),
        (4, 8, 4, 4, 1),
        (4, 9, 4, 5, 2),
        (4, 4, 2, 5, 1),
        (4, 2, 1, 5, 1),
        (7, 16, 8, 4, 1),
        (7, 11, 4, 5, 2),
        (4, 32, 16, 5, 1),
    ];
    let mut runs = 0;
    for (n, interval, max_period, wave, leaders) in configs {
        for seed in 0..8u64 {
            fuzz_one(
                seed * 7919 + interval,
                n,
                interval,
                max_period,
                wave,
                leaders,
            );
            runs += 1;
        }
    }
    eprintln!("differential fuzz: {runs} runs without divergence");
}
