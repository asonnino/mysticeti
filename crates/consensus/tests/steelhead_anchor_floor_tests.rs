// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Steelhead anchor floor: an undecided async slot must not be decided by the
//! sync slots inside its own wave — its anchor starts at `r + w_async`, so it
//! holds back every decided slot above it until its wave completes.

use std::num::{NonZeroU64, NonZeroUsize};

use consensus::{
    committer::Committer,
    protocol::{ConsensusProtocol, SteelheadPair},
};
use dag::{
    consensus::LeaderStatus,
    storage::Storage,
    test_util::{build_dag, committee},
};

/// Period 4 with `w_async = 5`: the async slot at round 4 has its decision
/// round at 8. A full DAG truncated at round 7 leaves it undecided (its votes
/// exist but its decision round does not), while the sync slots at rounds
/// 1..=3 and 5 (decision rounds ≤ 7) are all directly committable.
#[test]
#[tracing_test::traced_test]
fn undecided_async_slot_holds_back_committed_sync_slots() {
    let committee = committee(4);
    let spec = ConsensusProtocol::Steelhead {
        pair: SteelheadPair::MysticetiMahiMahi,
        period: NonZeroU64::new(4),
        async_wave_length: 5,
        adaptive: None,
        canary: NonZeroU64::new(1),
        leader_count: NonZeroUsize::new(1).unwrap(),
    };

    let mut storage = Storage::new_for_test(&committee);
    let references = build_dag(&committee, &mut storage, None, 7);
    let mut committer = Committer::new_for_test(&committee, &storage, &spec);

    // The sync slot at round 5 is directly committed, but the slots at rounds
    // 1..=4 gate the output: the sequence must stop strictly below the async
    // slot, proving no slot inside its wave (rounds 5..=8) anchored it.
    let sequence = committer.try_commit(None).collect::<Vec<_>>();
    assert_eq!(
        sequence.len(),
        3,
        "expected exactly rounds 1..=3: {sequence:?}"
    );
    for (index, status) in sequence.iter().enumerate() {
        let round = index as u64 + 1;
        match status {
            LeaderStatus::DirectCommit(block) => assert_eq!(block.round(), round),
            other => panic!("expected DirectCommit at round {round}, got {other:?}"),
        }
    }

    // Completing the async slot's own wave decides it directly, releasing the
    // withheld slots in round order.
    build_dag(&committee, &mut storage, Some(references), 10);
    let sequence = committer.try_commit(None).collect::<Vec<_>>();
    let committed_rounds: Vec<_> = sequence
        .iter()
        .map(|status| match status {
            LeaderStatus::DirectCommit(block) => block.round(),
            other => panic!("expected DirectCommit, got {other:?}"),
        })
        .collect();
    assert!(
        committed_rounds.contains(&4),
        "async slot must be decided once its wave completes: {committed_rounds:?}"
    );
    assert!(
        committed_rounds.windows(2).all(|pair| pair[0] < pair[1]),
        "slots must be output in round order: {committed_rounds:?}"
    );
}
