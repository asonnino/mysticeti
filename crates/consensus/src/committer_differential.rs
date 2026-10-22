// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A committer that keeps state between calls must return what a fresh committer returns on
//! the same blocks. Each test feeds random DAGs to both, block by block.

use std::{
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
};

use dag::{consensus::LeaderStatus, storage::Storage, test_util::committee};

use crate::{
    committer::Committer,
    protocol::ConsensusProtocol,
    random_dag::{build_random_dag, shapes},
};

fn specs() -> Vec<ConsensusProtocol> {
    let one = NonZeroUsize::new(1).unwrap();
    let two = NonZeroUsize::new(2).unwrap();
    vec![
        ConsensusProtocol::Mysticeti { leader_count: one },
        ConsensusProtocol::Mysticeti { leader_count: two },
        ConsensusProtocol::CordialMinersPartiallySynchronous,
        ConsensusProtocol::CordialMinersAsynchronous,
        ConsensusProtocol::BlueBottlePartiallySynchronous { leader_count: one },
        ConsensusProtocol::BlueBottleAsynchronous { leader_count: two },
        ConsensusProtocol::Orcaella {
            leader_count: two,
            f: 1,
            c: 0,
        },
        ConsensusProtocol::MahiMahi {
            leader_count: two,
            wave_length: 4,
        },
        ConsensusProtocol::MahiMahi {
            leader_count: two,
            wave_length: 5,
        },
        ConsensusProtocol::NemoNemo { leader_count: two },
        ConsensusProtocol::Hydrozoan {
            leader_count: two,
            f: 1,
            c: 0,
            k: 0,
        },
    ]
}

/// Runs the long-lived committer and a fresh one over the same random DAG and returns how
/// many steps were compared. A DAG with enough Byzantine blocks can certify two leader blocks,
/// which the rule treats as a safety violation by panicking; the run stops at that point.
fn compare(
    spec: &ConsensusProtocol,
    n: usize,
    seed: u64,
    shape: crate::random_dag::Shape,
) -> Option<usize> {
    let committee = committee(n);
    // Some protocols need a larger committee for their fault budget.
    spec.to_protocol(&committee).ok()?;
    let mut storage = Storage::new_for_test(&committee);
    let mut incremental = Committer::new_for_test(&committee, &storage, spec);
    let mut last_decided = None;
    let mut steps = 0;
    let mut violated = false;
    build_random_dag(
        &committee,
        &mut storage,
        seed,
        24,
        shape,
        |storage, _block| {
            if violated {
                return;
            }
            let mut fresh = Committer::new_for_test(&committee, storage, spec);
            let want = catch_unwind(AssertUnwindSafe(|| {
                fresh
                    .try_commit(last_decided)
                    .collect::<Vec<LeaderStatus>>()
            }));
            let Ok(want) = want else {
                violated = true;
                return;
            };
            let got: Vec<_> = incremental.try_commit(last_decided).collect();
            assert_eq!(
                got, want,
                "{spec:?} n={n} seed={seed} step={steps} last_decided={last_decided:?}"
            );
            if let Some(status) = got.last() {
                last_decided = Some((status.round(), status.authority()));
            }
            // Asking again from the start must also agree: nothing is lost by the cache.
            if steps % 17 == 0 {
                let mut fresh = Committer::new_for_test(&committee, storage, spec);
                let want: Vec<_> = fresh.try_commit(None).collect();
                let got: Vec<_> = incremental.try_commit(None).collect();
                assert_eq!(
                    got, want,
                    "{spec:?} n={n} seed={seed} step={steps} from the start"
                );
            }
            steps += 1;
        },
    );
    Some(steps)
}

#[test]
fn incremental_committer_matches_a_fresh_committer_on_random_dags() {
    let mut compared = 0;
    let mut runs = 0;
    for spec in specs() {
        for n in [4, 7] {
            for (shape_index, shape) in shapes().into_iter().enumerate() {
                for seed in 0..6 {
                    let seed = seed * 10 + shape_index as u64;
                    if let Some(steps) = compare(&spec, n, seed, shape) {
                        compared += steps;
                        runs += 1;
                    }
                }
            }
        }
    }
    // Most runs must get through the whole DAG, or the comparison proves little.
    assert!(
        compared > runs * 40,
        "only {compared} steps over {runs} runs"
    );
}
