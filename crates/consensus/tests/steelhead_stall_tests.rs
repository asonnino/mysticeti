// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The stall fallback of adaptive Steelhead. An adversary can hold known-leader
//! slots at f + 1 votes and f + 1 blames, so they are neither committed nor
//! skipped; the indirect rule stops at them and the output stalls while other
//! slots keep committing. The replay score caps "never output" at its window
//! top, so it can keep a dead period. The committer therefore tracks the agreed
//! output (the output rule over each scan anchor's causal history) and falls
//! back to period 1 when it commits nothing within an interval.

use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::{Arc, atomic::AtomicU64, atomic::Ordering},
};

use consensus::{
    committer::Committer,
    protocol::{AdaptiveConfig, ConsensusProtocol, SteelheadPair},
};
use dag::{
    authority::Authority,
    block::{BlockReference, RoundNumber},
    committee::Committee,
    consensus::LeaderStatus,
    storage::Storage,
    test_util::{build_dag, build_dag_layer, committee, drop_leader},
};

const DEPTH: RoundNumber = 256;
const MAX_PERIOD: u64 = 4;

/// A slot's verdict: its round, its leader, and the committed block if any.
type Verdict = (RoundNumber, Authority, Option<BlockReference>);

/// Whether an author omits the previous round's known-leader block.
type Omits = fn(RoundNumber, Authority) -> bool;

fn spec(interval: u64, epsilon_percent: u8) -> ConsensusProtocol {
    ConsensusProtocol::Steelhead {
        pair: SteelheadPair::MysticetiMahiMahi,
        period: None,
        async_wave_length: 5,
        adaptive: Some(AdaptiveConfig {
            interval,
            max_period: NonZeroU64::new(MAX_PERIOD).unwrap(),
            epsilon_percent,
        }),
        canary: NonZeroU64::new(1),
        leader_count: NonZeroUsize::new(1).unwrap(),
    }
}

/// Build `DEPTH` rounds in which `author` omits the previous round's
/// round-robin leader block iff `omits(previous_round, author)`; a leader never
/// omits its own block.
fn build_omitting(committee: &Arc<Committee>, storage: &mut Storage, omits: Omits) {
    let n = committee.len() as u64;
    let mut references = build_dag(committee, storage, None, 0);
    for round in 1..=DEPTH {
        let previous = round - 1;
        let leader = Authority::new(previous % n);
        let connections = committee
            .authorities()
            .map(|author| {
                let parents = if previous >= 1 && author != leader && omits(previous, author) {
                    drop_leader(&references, leader)
                } else {
                    references.clone()
                };
                (author, parents)
            })
            .collect();
        references = build_dag_layer(connections, storage);
    }
}

/// The authority that references every held leader block besides its author.
fn is_helper(author: Authority) -> bool {
    author == Authority::new(0)
}

/// Every known leader is held: only the helper references it besides itself.
fn held_known_leaders(_previous: RoundNumber, author: Authority) -> bool {
    !is_helper(author)
}

/// Slots at `r % 4 == 1` commit, `2` are directly skipped, `3` are held: slot 3
/// waits on 7 (6 is skipped), 7 on 11, and so on, while half the sync slots and
/// every async slot commit.
fn partial_commits(previous: RoundNumber, author: Authority) -> bool {
    match previous % 4 {
        2 => true,
        3 => !is_helper(author),
        _ => false,
    }
}

/// One Byzantine authority (3) splits its votes 2/2 every time it leads; the
/// network is otherwise synchronous.
fn byzantine_split_leader(previous: RoundNumber, author: Authority) -> bool {
    previous % 4 == 3 && !is_helper(author)
}

/// Drain a committer, consuming at most `chunk` decisions per call.
fn consume(committer: &mut Committer, chunk: usize) -> Vec<LeaderStatus> {
    let mut sequence: Vec<LeaderStatus> = Vec::new();
    let mut last_decided: Option<(RoundNumber, Authority)> = None;
    loop {
        let step: Vec<_> = committer.try_commit(last_decided).take(chunk).collect();
        let Some(last) = step.last() else {
            return sequence;
        };
        last_decided = Some((last.round(), last.authority()));
        sequence.extend(step);
        assert!(
            sequence.len() <= 2 * DEPTH as usize,
            "consumption must terminate"
        );
    }
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

/// The agreed output must be a prefix of the real output, verdict for verdict.
fn assert_agreed_prefix(agreed: &[Verdict], output: &[Verdict]) {
    assert!(agreed.len() <= output.len(), "agreed output runs ahead");
    assert_eq!(agreed, &output[..agreed.len()]);
}

struct Run {
    output: Vec<Verdict>,
    agreed: Vec<Verdict>,
    stall_fallbacks: u64,
    period: u64,
}

fn run(
    storage: &Storage,
    committee: &Arc<Committee>,
    spec: &ConsensusProtocol,
    chunk: usize,
) -> Run {
    let cell = Arc::new(AtomicU64::new(MAX_PERIOD));
    let mut committer =
        Committer::new_for_test(committee, storage, spec).with_period_cell(cell.clone());
    let output = consume(&mut committer, chunk).iter().map(verdict).collect();
    Run {
        output,
        agreed: committer.agreed_output().iter().map(verdict).collect(),
        stall_fallbacks: committer.stall_fallbacks(),
        period: cell.load(Ordering::Relaxed),
    }
}

/// Without the fallback these configurations stall at round 1 or 2 forever
/// (the replay keeps period 4); with it the output reaches the DAG's top.
#[test]
fn stalled_output_falls_back_to_period_one() {
    let cases: [(&str, Omits, u64, u8); 3] = [
        ("held known leaders", held_known_leaders, 8, 50),
        ("held known leaders", held_known_leaders, 32, 50),
        ("partial commits", partial_commits, 8, 10),
    ];
    for (name, omits, interval, epsilon_percent) in cases {
        let committee = committee(4);
        let mut storage = Storage::new_for_test(&committee);
        build_omitting(&committee, &mut storage, omits);
        let outcome = run(
            &storage,
            &committee,
            &spec(interval, epsilon_percent),
            usize::MAX,
        );

        let case = format!("{name}, interval {interval}, epsilon {epsilon_percent}%");
        assert!(outcome.stall_fallbacks > 0, "{case}: no fallback");
        assert_eq!(outcome.period, 1, "{case}");
        let top = outcome.output.last().map_or(0, |(round, ..)| *round);
        assert!(top + 8 >= DEPTH, "{case}: output stops at round {top}");
        assert_agreed_prefix(&outcome.agreed, &outcome.output);
    }
}

/// A healthy DAG, and a single Byzantine leader splitting its votes under
/// synchrony (its slots are settled by the next honest commit), never stall.
#[test]
fn flowing_output_never_falls_back() {
    for interval in [8, 16, 32] {
        let committee = committee(4);
        let mut storage = Storage::new_for_test(&committee);
        build_dag(&committee, &mut storage, None, DEPTH);
        let healthy = run(&storage, &committee, &spec(interval, 10), usize::MAX);
        assert_eq!(healthy.stall_fallbacks, 0, "healthy, interval {interval}");
        assert_agreed_prefix(&healthy.agreed, &healthy.output);

        let mut storage = Storage::new_for_test(&committee);
        build_omitting(&committee, &mut storage, byzantine_split_leader);
        let split = run(&storage, &committee, &spec(interval, 10), usize::MAX);
        assert_eq!(
            split.stall_fallbacks, 0,
            "split leader, interval {interval}"
        );
        assert_agreed_prefix(&split.agreed, &split.output);
    }
}

/// The agreed output and the fallbacks are functions of the DAG alone, not of
/// the consumer's cadence.
#[test]
fn fallback_is_cadence_independent() {
    let committee = committee(4);
    let mut storage = Storage::new_for_test(&committee);
    build_omitting(&committee, &mut storage, partial_commits);
    let spec = spec(8, 10);

    let reference = run(&storage, &committee, &spec, usize::MAX);
    for chunk in [3, 1] {
        let outcome = run(&storage, &committee, &spec, chunk);
        assert_eq!(
            outcome.stall_fallbacks, reference.stall_fallbacks,
            "chunk {chunk}"
        );
        assert_eq!(outcome.period, reference.period, "chunk {chunk}");
        assert_eq!(outcome.agreed, reference.agreed, "chunk {chunk}");
        assert_eq!(outcome.output, reference.output, "chunk {chunk}");
    }
}
