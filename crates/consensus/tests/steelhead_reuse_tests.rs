// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Adaptive Steelhead reuses cached direct verdicts when extending its agreed
//! output; the reuse must be exact. Random DAGs with equivocating Byzantine
//! authorities (at most f), leader omissions, dropped parents and late delivery
//! reach a replica block by block, so verdicts are often cached before their
//! rounds are complete. Its agreed output must match a fresh drain of the final
//! DAG, which caches nothing before its scans.

use std::{
    collections::HashSet,
    num::{NonZeroU64, NonZeroUsize},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use consensus::{
    committer::Committer,
    protocol::{AdaptiveConfig, ConsensusProtocol, SteelheadPair},
};
use dag::{
    authority::Authority,
    block::{Block, BlockReference, RoundNumber},
    consensus::LeaderStatus,
    crypto::BlockDigest,
    storage::Storage,
    test_util::{build_dag, committee, insert_test_block},
};

const MAX_PERIOD: u64 = 4;
const DEPTH: RoundNumber = 64;

/// A slot's verdict: its round, its leader, and the committed block if any.
type Verdict = (RoundNumber, Authority, Option<BlockReference>);

/// SplitMix64: deterministic randomness without dependencies.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    fn digest(&mut self) -> BlockDigest {
        let mut bytes = [0u8; 32];
        for chunk in bytes.chunks_mut(8) {
            chunk.copy_from_slice(&self.next().to_le_bytes());
        }
        BlockDigest::from(bytes)
    }
}

#[derive(Debug)]
struct Scenario {
    n: usize,
    pair: SteelheadPair,
    leader_count: usize,
    interval: u64,
    /// The last `byzantine` authorities equivocate (never authority 0, the
    /// storage's own authority).
    byzantine: u64,
    seed: u64,
}

impl Scenario {
    fn spec(&self) -> ConsensusProtocol {
        let async_wave_length = match self.pair {
            SteelheadPair::MysticetiMahiMahi => 5,
            SteelheadPair::BlueBottle => 3,
        };
        ConsensusProtocol::Steelhead {
            pair: self.pair,
            period: None,
            async_wave_length,
            adaptive: Some(AdaptiveConfig {
                interval: self.interval,
                max_period: NonZeroU64::new(MAX_PERIOD).unwrap(),
                epsilon_percent: 10,
            }),
            canary: NonZeroU64::new(1),
            leader_count: NonZeroUsize::new(self.leader_count).unwrap(),
        }
    }

    /// Distinct parent authors every block needs: the pair's quorum.
    fn quorum(&self) -> usize {
        match self.pair {
            SteelheadPair::MysticetiMahiMahi => 2 * self.n / 3 + 1,
            SteelheadPair::BlueBottle => 4 * self.n / 5 + 1,
        }
    }
}

/// A block and the round at which it reaches the replica.
struct Planned {
    block: Block,
    arrival: RoundNumber,
}

/// Plan a random DAG: each round may starve one leader candidate of the
/// previous round, Byzantine authorities sometimes publish a second, differently
/// linked block, and a quarter of the blocks arrive up to six rounds late.
fn plan(scenario: &Scenario) -> Vec<Planned> {
    let mut random = Random(scenario.seed);
    let n = scenario.n as u64;
    let mut previous: Vec<BlockReference> = (0..n)
        .map(|author| *Block::genesis(Authority::new(author)).reference())
        .collect();
    let mut planned = Vec::new();
    for round in 1..=DEPTH {
        // The previous round's round-robin or coin leader (shifted by 5).
        let victim = random
            .chance(50)
            .then(|| (round - 1 + 5 * random.below(2)) % n);
        let mut current = Vec::new();
        for author in 0..n {
            let byzantine = author >= n - scenario.byzantine;
            let copies = if byzantine && random.chance(40) { 2 } else { 1 };
            for copy in 0..copies {
                // The twin takes the opposite stance on the victim.
                let omits_victim = random.chance(70) != (copy == 1);
                let victim = victim.filter(|_| omits_victim);
                let parents = choose_parents(&mut random, &previous, author, victim, scenario);
                let mut block = Block::new_for_test(Authority::new(author), round, parents);
                if copy == 1 {
                    block = block.with_digest(random.digest());
                }
                let arrival = if random.chance(25) {
                    round + 1 + random.below(6)
                } else {
                    round
                };
                current.push(*block.reference());
                planned.push(Planned { block, arrival });
            }
        }
        previous = current;
    }
    planned
}

/// Every own block, never the victim's, the others kept at random; missing
/// authors are restored until the parents reach a quorum.
fn choose_parents(
    random: &mut Random,
    previous: &[BlockReference],
    author: u64,
    victim: Option<u64>,
    scenario: &Scenario,
) -> Vec<BlockReference> {
    let (mut parents, mut missing): (Vec<_>, Vec<_>) =
        previous.iter().copied().partition(|reference| {
            let parent = reference.authority.as_u64();
            parent == author || (Some(parent) != victim && !random.chance(15))
        });
    let authors = |parents: &[BlockReference]| {
        parents
            .iter()
            .map(|reference| reference.authority)
            .collect::<HashSet<_>>()
            .len()
    };
    while authors(&parents) < scenario.quorum() {
        let index = random.below(missing.len() as u64) as usize;
        parents.push(missing.swap_remove(index));
    }
    parents
}

fn verdict(status: &LeaderStatus) -> Verdict {
    let committed = match status {
        LeaderStatus::DirectCommit(block) | LeaderStatus::IndirectCommit(block) => {
            Some(*block.reference())
        }
        _ => None,
    };
    (status.round(), status.authority(), committed)
}

/// Deliver the planned DAG block by block (parents first, late blocks from
/// their arrival round), running the replica after every block; then drain
/// the final DAG with a fresh committer and compare.
fn exercise(scenario: &Scenario) {
    let committee = committee(scenario.n);
    let spec = scenario.spec();
    let mut storage = Storage::new_for_test(&committee);
    build_dag(&committee, &mut storage, None, 0);
    let replica_cell = Arc::new(AtomicU64::new(MAX_PERIOD));
    let mut replica =
        Committer::new_for_test(&committee, &storage, &spec).with_period_cell(replica_cell.clone());

    let mut pending = plan(scenario);
    let mut random = Random(!scenario.seed);
    let mut inserted: HashSet<BlockReference> = (0..scenario.n as u64)
        .map(|author| *Block::genesis(Authority::new(author)).reference())
        .collect();
    let mut output: Vec<Verdict> = Vec::new();
    let mut last_decided = None;
    let mut round = 1;
    while !pending.is_empty() {
        loop {
            let ready: Vec<usize> = (0..pending.len())
                .filter(|&index| {
                    let planned = &pending[index];
                    planned.arrival <= round
                        && planned
                            .block
                            .includes()
                            .iter()
                            .all(|parent| inserted.contains(parent))
                })
                .collect();
            if ready.is_empty() {
                break;
            }
            let index = ready[random.below(ready.len() as u64) as usize];
            let planned = pending.swap_remove(index);
            inserted.insert(insert_test_block(planned.block, &mut storage));
            for status in replica.try_commit(last_decided) {
                last_decided = Some((status.round(), status.authority()));
                output.push(verdict(&status));
            }
        }
        round += 1;
    }

    let fresh_cell = Arc::new(AtomicU64::new(MAX_PERIOD));
    let mut fresh =
        Committer::new_for_test(&committee, &storage, &spec).with_period_cell(fresh_cell.clone());
    let fresh_output: Vec<Verdict> = fresh
        .try_commit(None)
        .map(|status| verdict(&status))
        .collect();

    let case = format!("{scenario:?}");
    let agreed = |committer: &Committer| -> Vec<Verdict> {
        committer.agreed_output().iter().map(verdict).collect()
    };
    assert_eq!(agreed(&replica), agreed(&fresh), "agreed output: {case}");
    assert_eq!(replica.stall_fallbacks(), fresh.stall_fallbacks(), "{case}");
    assert_eq!(
        replica_cell.load(Ordering::Relaxed),
        fresh_cell.load(Ordering::Relaxed),
        "period: {case}"
    );
    assert_eq!(output, fresh_output, "output: {case}");
}

#[test]
fn verdict_reuse_is_exact_under_equivocation() {
    let shapes = [
        (4, SteelheadPair::MysticetiMahiMahi, 1, 1),
        (7, SteelheadPair::MysticetiMahiMahi, 1, 2),
        (7, SteelheadPair::MysticetiMahiMahi, 2, 2),
        (7, SteelheadPair::BlueBottle, 1, 1),
    ];
    for (n, pair, leader_count, byzantine) in shapes {
        for interval in [8, 16] {
            for seed in 0..3 {
                exercise(&Scenario {
                    n,
                    pair,
                    leader_count,
                    interval,
                    byzantine,
                    seed: seed * 1_000_003 + n as u64 * 31 + interval,
                });
            }
        }
    }
}
