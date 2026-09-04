// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Steelhead conservativity: with a constant period, the composed protocol must
//! reproduce the exact verdict sequence of the base rule it degenerates to —
//! period ∞ is the pure sync rule, period 1 the pure async rule.

use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
};

use consensus::{
    committer::Committer,
    leader::LeaderElector,
    protocol::{ConsensusProtocol, SteelheadPair},
};
use dag::{
    committee::Committee,
    storage::Storage,
    test_util::{build_dag, committee, drop_leader},
};

/// The steelhead spec and the base rule it must be indistinguishable from.
fn conservative_pairs(leader_count: NonZeroUsize) -> Vec<(ConsensusProtocol, ConsensusProtocol)> {
    let pure_sync = None;
    let pure_async = NonZeroU64::new(1);
    vec![
        (
            ConsensusProtocol::Steelhead {
                pair: SteelheadPair::MysticetiMahiMahi,
                period: pure_sync,
                async_wave_length: 5,
                adaptive: None,
                canary: NonZeroU64::new(1),
                leader_count,
            },
            ConsensusProtocol::Mysticeti { leader_count },
        ),
        (
            ConsensusProtocol::Steelhead {
                pair: SteelheadPair::MysticetiMahiMahi,
                period: pure_async,
                async_wave_length: 4,
                adaptive: None,
                canary: NonZeroU64::new(1),
                leader_count,
            },
            ConsensusProtocol::MahiMahi {
                leader_count,
                wave_length: 4,
            },
        ),
        (
            ConsensusProtocol::Steelhead {
                pair: SteelheadPair::MysticetiMahiMahi,
                period: pure_async,
                async_wave_length: 5,
                adaptive: None,
                canary: NonZeroU64::new(1),
                leader_count,
            },
            ConsensusProtocol::MahiMahi {
                leader_count,
                wave_length: 5,
            },
        ),
        (
            ConsensusProtocol::Steelhead {
                pair: SteelheadPair::BlueBottle,
                period: pure_sync,
                async_wave_length: 3,
                adaptive: None,
                canary: NonZeroU64::new(1),
                leader_count,
            },
            ConsensusProtocol::BlueBottlePartiallySynchronous { leader_count },
        ),
        (
            ConsensusProtocol::Steelhead {
                pair: SteelheadPair::BlueBottle,
                period: pure_async,
                async_wave_length: 3,
                adaptive: None,
                canary: NonZeroU64::new(1),
                leader_count,
            },
            ConsensusProtocol::BlueBottleAsynchronous { leader_count },
        ),
    ]
}

/// A fully-connected DAG deep enough to decide several waves of every rule.
fn full_dag(committee: &Arc<Committee>) -> Storage {
    let mut storage = Storage::new_for_test(committee);
    build_dag(committee, &mut storage, None, 12);
    storage
}

/// A DAG where the round-3 leader block is never referenced, driving skips.
fn dropped_leader_dag(committee: &Arc<Committee>) -> Storage {
    let mut storage = Storage::new_for_test(committee);
    let references = build_dag(committee, &mut storage, None, 3);
    let leader = LeaderElector::new(committee.len()).elect_leader(3);
    let references_without_leader = drop_leader(&references, leader);
    build_dag(committee, &mut storage, Some(references_without_leader), 12);
    storage
}

/// A shallow DAG whose top slots are still undecided.
fn partial_dag(committee: &Arc<Committee>) -> Storage {
    let mut storage = Storage::new_for_test(committee);
    build_dag(committee, &mut storage, None, 5);
    storage
}

fn run_for_size(n: usize) {
    let committee = committee(n);
    for leader_count in [1, 2] {
        let leader_count = NonZeroUsize::new(leader_count).unwrap();
        for (steelhead, baseline) in conservative_pairs(leader_count) {
            for (fixture_name, storage) in [
                ("full", full_dag(&committee)),
                ("dropped-leader", dropped_leader_dag(&committee)),
                ("partial", partial_dag(&committee)),
            ] {
                let mut steelhead_committer =
                    Committer::new_for_test(&committee, &storage, &steelhead);
                let mut baseline_committer =
                    Committer::new_for_test(&committee, &storage, &baseline);

                let steelhead_sequence: Vec<_> = steelhead_committer.try_commit(None).collect();
                let baseline_sequence: Vec<_> = baseline_committer.try_commit(None).collect();
                assert_eq!(
                    steelhead_sequence, baseline_sequence,
                    "[{steelhead:?} vs {baseline:?}] diverged on the {fixture_name} DAG"
                );
                if fixture_name != "partial" {
                    assert!(
                        !baseline_sequence.is_empty(),
                        "[{baseline:?}] vacuous conservativity check on the {fixture_name} DAG"
                    );
                }
            }
        }
    }
}

#[test]
#[tracing_test::traced_test]
fn steelhead_conservativity_n4() {
    run_for_size(4);
}

#[test]
#[tracing_test::traced_test]
fn steelhead_conservativity_n20() {
    run_for_size(20);
}
