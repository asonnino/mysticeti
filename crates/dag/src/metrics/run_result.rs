// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Run-level results derived from per-replica observations.
//!
//! Holds the `Outcome` classification of a run and the cross-replica consistency check used
//! to derive it. `RunResult<C>` (added later) is the shared container that sim, testbed, and
//! smoke tests build from a set of `MetricsSnapshot`s.

use blake2::Blake2b;
use digest::{Digest, consts::U32};

use crate::{crypto::CryptoHash, storage::block_store::CommitData};

/// Verdict on a single run, derived from the committed-leader sequences across replicas.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Every replica's committed prefix agrees with the others', and at least one replica
    /// committed a leader.
    Pass,
    /// Every replica committed zero leaders. Expected under unrecoverable partitions.
    NoProgress,
    /// At least two replicas disagree on a commit within their shared prefix. Safety bug.
    Diverged,
}

type PrefixHasher = Blake2b<U32>;

/// Classify a run by streaming each replica's committed sub-dag sequence through a per-replica
/// rolling hash (leader + ordered sub-dag, via [`CryptoHash`]) and comparing cumulative hashes
/// in lock-step. Replicas that reach a shared index must produce identical cumulative hashes
/// there.
///
/// Hashing the full `CommitData` (not just the leader) also catches linearizer divergence:
/// two replicas agreeing on the leader sequence but ordering their sub-dag blocks differently
/// will show as `Diverged`.
///
/// Memory: `O(replicas)` regardless of commit count.
pub fn check_consistency<R>(streams: impl IntoIterator<Item = R>) -> Outcome
where
    R: IntoIterator<Item = CommitData>,
{
    let mut replicas: Vec<(PrefixHasher, R::IntoIter)> = streams
        .into_iter()
        .map(|s| (PrefixHasher::new(), s.into_iter()))
        .collect();

    if replicas.is_empty() {
        return Outcome::NoProgress;
    }

    let mut any_commit_seen = false;

    while !replicas.is_empty() {
        // Advance each replica by one commit, dropping any that have exhausted their stream.
        // `hashes_this_step` collects the cumulative hash at this prefix length for every
        // replica still alive after the advance — they must all agree.
        let mut hashes_this_step: Vec<[u8; 32]> = Vec::with_capacity(replicas.len());
        replicas.retain_mut(|(hasher, iter)| match iter.next() {
            Some(commit) => {
                commit.crypto_hash(hasher);
                hashes_this_step.push(hasher.clone().finalize().into());
                true
            }
            None => false,
        });

        if hashes_this_step.is_empty() {
            break;
        }
        any_commit_seen = true;

        let reference = hashes_this_step[0];
        if hashes_this_step[1..].iter().any(|h| *h != reference) {
            return Outcome::Diverged;
        }
    }

    if any_commit_seen {
        Outcome::Pass
    } else {
        Outcome::NoProgress
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        block::BlockReference,
        metrics::run_result::{Outcome, check_consistency},
        storage::block_store::CommitData,
    };

    fn commit(leader_authority: u64, round: u64) -> CommitData {
        CommitData {
            leader: BlockReference::new_test(leader_authority, round),
            sub_dag: vec![BlockReference::new_test(leader_authority, round)],
        }
    }

    fn sequence(leaders: &[(u64, u64)]) -> Vec<CommitData> {
        leaders.iter().map(|(a, r)| commit(*a, *r)).collect()
    }

    #[test]
    fn identical_sequences_pass() {
        let seq = sequence(&[(0, 1), (1, 2), (2, 3), (3, 4)]);
        let streams = vec![seq.clone(), seq.clone(), seq.clone(), seq];
        assert_eq!(check_consistency(streams), Outcome::Pass);
    }

    #[test]
    fn all_empty_is_no_progress() {
        let streams: Vec<Vec<CommitData>> = vec![vec![], vec![], vec![]];
        assert_eq!(check_consistency(streams), Outcome::NoProgress);
    }

    #[test]
    fn no_replicas_is_no_progress() {
        let streams: Vec<Vec<CommitData>> = vec![];
        assert_eq!(check_consistency(streams), Outcome::NoProgress);
    }

    #[test]
    fn diverging_tails_are_detected() {
        let a = sequence(&[(0, 1), (1, 2), (2, 3)]);
        let b = sequence(&[(0, 1), (1, 2), (3, 3)]);
        assert_eq!(check_consistency(vec![a, b]), Outcome::Diverged);
    }

    #[test]
    fn divergence_at_first_position_is_detected() {
        let a = sequence(&[(0, 1)]);
        let b = sequence(&[(1, 1)]);
        assert_eq!(check_consistency(vec![a, b]), Outcome::Diverged);
    }

    #[test]
    fn shorter_replica_on_shared_prefix_passes() {
        let full = sequence(&[(0, 1), (1, 2), (2, 3)]);
        let short = sequence(&[(0, 1), (1, 2)]);
        assert_eq!(check_consistency(vec![full, short]), Outcome::Pass);
    }

    #[test]
    fn divergence_between_two_replicas_when_third_is_shorter_is_detected() {
        // The shortest replica exhausts before the divergence; the longer two must still
        // be compared against each other beyond that length.
        let short = sequence(&[(0, 1), (1, 2)]);
        let a = sequence(&[(0, 1), (1, 2), (2, 3), (3, 4)]);
        let b = sequence(&[(0, 1), (1, 2), (2, 3), (0, 4)]);
        assert_eq!(check_consistency(vec![short, a, b]), Outcome::Diverged);
    }

    #[test]
    fn same_leader_different_sub_dag_is_divergence() {
        let leader = BlockReference::new_test(0, 1);
        let a = vec![CommitData {
            leader,
            sub_dag: vec![
                BlockReference::new_test(1, 1),
                BlockReference::new_test(2, 1),
            ],
        }];
        let b = vec![CommitData {
            leader,
            sub_dag: vec![
                BlockReference::new_test(2, 1),
                BlockReference::new_test(1, 1),
            ],
        }];
        assert_eq!(check_consistency(vec![a, b]), Outcome::Diverged);
    }

    #[test]
    fn memory_bounded_for_large_identical_streams() {
        let seq: Vec<CommitData> = (1..=10_000).map(|round| commit(0, round)).collect();
        let streams = vec![seq.clone(), seq];
        assert_eq!(check_consistency(streams), Outcome::Pass);
    }
}
