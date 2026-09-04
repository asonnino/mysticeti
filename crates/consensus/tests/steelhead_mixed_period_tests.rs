// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Steelhead with a finite period on a healthy DAG: every slot direct-commits
//! under its own wavelength, and the output stays in strict round order.

use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
};

use consensus::{
    committer::Committer,
    protocol::{ConsensusProtocol, SteelheadPair},
};
use dag::{
    committee::Committee,
    consensus::LeaderStatus,
    storage::Storage,
    test_util::{build_dag, committee},
};

const DAG_DEPTH: u64 = 14;

fn run(spec: &ConsensusProtocol, committee: &Arc<Committee>, leader_count: usize) {
    let mut storage = Storage::new_for_test(committee);
    build_dag(committee, &mut storage, None, DAG_DEPTH);
    let mut committer = Committer::new_for_test(committee, &storage, spec);

    // The output stops at the first slot whose decision round exceeds the DAG.
    let last_decidable_round = (1..)
        .find(|&round| committer.decision_round_for(round) > DAG_DEPTH)
        .unwrap()
        - 1;
    assert!(last_decidable_round >= 8, "fixture too shallow: {spec:?}");

    let sequence = committer.try_commit(None).collect::<Vec<_>>();
    assert_eq!(
        sequence.len(),
        (last_decidable_round as usize) * leader_count,
        "[{spec:?}] expected rounds 1..={last_decidable_round} with {leader_count} leaders: \
        {sequence:?}"
    );
    for (index, status) in sequence.iter().enumerate() {
        let round = index as u64 / leader_count as u64 + 1;
        let offset = index as u64 % leader_count as u64;
        match status {
            LeaderStatus::DirectCommit(block) => {
                assert_eq!(block.round(), round, "[{spec:?}] round order");
                assert_eq!(
                    block.author(),
                    committer.leader_at(round, offset),
                    "[{spec:?}] leader at round {round} offset {offset}"
                );
            }
            other => panic!("[{spec:?}] expected DirectCommit at round {round}, got {other:?}"),
        }
    }
}

fn run_for_size(n: usize) {
    let committee = committee(n);
    for leader_count in [1, 2] {
        for period in [2, 4] {
            run(
                &ConsensusProtocol::Steelhead {
                    pair: SteelheadPair::MysticetiMahiMahi,
                    period: NonZeroU64::new(period),
                    async_wave_length: 5,
                    adaptive: None,
                    canary: NonZeroU64::new(1),
                    leader_count: NonZeroUsize::new(leader_count).unwrap(),
                },
                &committee,
                leader_count,
            );
            run(
                &ConsensusProtocol::Steelhead {
                    pair: SteelheadPair::BlueBottle,
                    period: NonZeroU64::new(period),
                    async_wave_length: 3,
                    adaptive: None,
                    canary: NonZeroU64::new(1),
                    leader_count: NonZeroUsize::new(leader_count).unwrap(),
                },
                &committee,
                leader_count,
            );
        }
    }
}

#[test]
#[tracing_test::traced_test]
fn steelhead_mixed_period_n4() {
    run_for_size(4);
}

#[test]
#[tracing_test::traced_test]
fn steelhead_mixed_period_n20() {
    run_for_size(20);
}
