// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Run-level results derived from per-replica observations.
//!
//! `RunResult<C>` is the shared container that sim, testbed, and smoke tests build from a
//! set of `MetricsSnapshot`s plus per-replica storages. `Outcome` classifies the run; it is
//! computed at construction by a streaming consistency check over each storage's committed
//! sub-dag sequence (no commit history retained in memory).

use std::{io, time::Duration};

use blake2::Blake2b;
use digest::{Digest, consts::U32};
use serde::Serialize;

use crate::{
    authority::Authority, crypto::CryptoHash, metrics::MetricsSnapshot, storage::Storage,
    storage::block_store::CommitData,
};

type PrefixHasher = Blake2b<U32>;

#[derive(Serialize)]
struct CommitRecord<'a> {
    authority: Authority,
    commit: &'a CommitData,
}

/// Verdict on a single run, derived from the committed-leader sequences across replicas.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Outcome {
    /// Every replica's committed prefix agrees with the others', and at least one replica
    /// committed a leader.
    Pass,
    /// Every replica committed zero leaders. Expected under unrecoverable partitions.
    NoProgress,
    /// At least two replicas disagree on a commit within their shared prefix. Safety bug.
    Diverged,
}

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
impl<I, R> From<I> for Outcome
where
    I: IntoIterator<Item = R>,
    R: IntoIterator<Item = CommitData>,
{
    fn from(streams: I) -> Self {
        let mut replicas: Vec<(PrefixHasher, R::IntoIter)> = streams
            .into_iter()
            .map(|stream| (PrefixHasher::new(), stream.into_iter()))
            .collect();

        if replicas.is_empty() {
            return Outcome::NoProgress;
        }

        let mut any_commit_seen = false;

        while !replicas.is_empty() {
            // Advance each replica by one commit, dropping any that have exhausted their
            // stream. `hashes_this_step` collects the cumulative hash at this prefix length
            // for every replica still alive after the advance — they must all agree.
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
}

/// Cross-replica summary of a single run: per-replica metrics, the derived outcome, the
/// configuration that drove the run, and wall-clock duration.
///
/// Generic over `C` so simulator, testbed, and future smoke-test configs all share this
/// type. `outcome` is precomputed at construction from a streaming consistency check; the
/// commit history is never materialised in memory.
pub struct RunResult<C> {
    pub metrics: Vec<MetricsSnapshot>,
    pub outcome: Outcome,
    pub config: C,
    pub duration: Duration,
}

impl<C> RunResult<C> {
    /// Build a result from per-replica metrics and storages, running the consistency check
    /// to derive `Outcome`. Storages are iterated once; no commit is retained.
    pub fn new(
        metrics: Vec<MetricsSnapshot>,
        storages: &[Storage],
        config: C,
        duration: Duration,
    ) -> Self {
        let outcome = Outcome::from(storages.iter().map(Storage::iter_commits));
        Self {
            metrics,
            outcome,
            config,
            duration,
        }
    }

    /// Same as [`RunResult::new`], plus stream every committed sub-dag to `writer` as
    /// newline-delimited JSON (one object per line, `{"authority":…, "commit":…}`). The
    /// writer is driven in a separate pass over each storage before the consistency check,
    /// so memory stays bounded even for long runs.
    pub fn new_with_commit_log<W: io::Write>(
        metrics: Vec<MetricsSnapshot>,
        storages: &[Storage],
        config: C,
        duration: Duration,
        writer: &mut W,
    ) -> io::Result<Self> {
        for (index, storage) in storages.iter().enumerate() {
            let authority = Authority::from(index as u64);
            for commit in storage.iter_commits() {
                let record = CommitRecord {
                    authority,
                    commit: &commit,
                };
                serde_json::to_writer(&mut *writer, &record).map_err(io::Error::other)?;
                writeln!(writer)?;
            }
        }
        Ok(Self::new(metrics, storages, config, duration))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::{
        authority::Authority,
        block::BlockReference,
        committee::Committee,
        metrics::{Metrics, Outcome, RunResult},
        storage::{Storage, block_store::CommitData},
    };

    fn build_storages_with_commits(
        replica_count: usize,
        batch: &[CommitData],
    ) -> (Vec<Storage>, Vec<crate::metrics::MetricsSnapshot>) {
        let committee = Committee::new_test(vec![1; replica_count]);
        let mut storages = Vec::with_capacity(replica_count);
        let mut snapshots = Vec::with_capacity(replica_count);
        for index in 0..replica_count {
            let metrics = Metrics::new_for_test(replica_count);
            let (mut storage, _) =
                Storage::new_for_tests(Authority::from(index as u64), metrics.clone(), &committee);
            storage.write_commits(batch);
            snapshots.push(metrics.collect());
            storages.push(storage);
        }
        (storages, snapshots)
    }

    #[test]
    fn identical_sequences_pass() {
        let seq = CommitData::new_for_test(&[(0, 1), (1, 2), (2, 3), (3, 4)]);
        let streams = vec![seq.clone(), seq.clone(), seq.clone(), seq];
        assert_eq!(Outcome::from(streams), Outcome::Pass);
    }

    #[test]
    fn all_empty_is_no_progress() {
        let streams: Vec<Vec<CommitData>> = vec![vec![], vec![], vec![]];
        assert_eq!(Outcome::from(streams), Outcome::NoProgress);
    }

    #[test]
    fn no_replicas_is_no_progress() {
        let streams: Vec<Vec<CommitData>> = vec![];
        assert_eq!(Outcome::from(streams), Outcome::NoProgress);
    }

    #[test]
    fn diverging_tails_are_detected() {
        let a = CommitData::new_for_test(&[(0, 1), (1, 2), (2, 3)]);
        let b = CommitData::new_for_test(&[(0, 1), (1, 2), (3, 3)]);
        assert_eq!(Outcome::from(vec![a, b]), Outcome::Diverged);
    }

    #[test]
    fn divergence_at_first_position_is_detected() {
        let a = CommitData::new_for_test(&[(0, 1)]);
        let b = CommitData::new_for_test(&[(1, 1)]);
        assert_eq!(Outcome::from(vec![a, b]), Outcome::Diverged);
    }

    #[test]
    fn shorter_replica_on_shared_prefix_passes() {
        let full = CommitData::new_for_test(&[(0, 1), (1, 2), (2, 3)]);
        let short = CommitData::new_for_test(&[(0, 1), (1, 2)]);
        assert_eq!(Outcome::from(vec![full, short]), Outcome::Pass);
    }

    #[test]
    fn divergence_between_two_replicas_when_third_is_shorter_is_detected() {
        // The shortest replica exhausts before the divergence; the longer two must still
        // be compared against each other beyond that length.
        let short = CommitData::new_for_test(&[(0, 1), (1, 2)]);
        let a = CommitData::new_for_test(&[(0, 1), (1, 2), (2, 3), (3, 4)]);
        let b = CommitData::new_for_test(&[(0, 1), (1, 2), (2, 3), (0, 4)]);
        assert_eq!(Outcome::from(vec![short, a, b]), Outcome::Diverged);
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
        assert_eq!(Outcome::from(vec![a, b]), Outcome::Diverged);
    }

    #[test]
    fn memory_bounded_for_large_identical_streams() {
        let leaders: Vec<(u64, u64)> = (1..=10_000).map(|round| (0, round)).collect();
        let seq = CommitData::new_for_test(&leaders);
        let streams = vec![seq.clone(), seq];
        assert_eq!(Outcome::from(streams), Outcome::Pass);
    }

    #[test]
    fn run_result_new_classifies_consistent_run_as_pass() {
        let batch = CommitData::new_for_test(&[(0, 1), (1, 2), (2, 3)]);
        let (storages, snapshots) = build_storages_with_commits(3, &batch);

        let result: RunResult<()> =
            RunResult::new(snapshots, &storages, (), Duration::from_secs(30));

        assert_eq!(result.outcome, Outcome::Pass);
        assert_eq!(result.duration, Duration::from_secs(30));
        assert_eq!(result.metrics.len(), 3);
    }

    #[test]
    fn run_result_new_classifies_empty_run_as_no_progress() {
        let (storages, snapshots) = build_storages_with_commits(2, &[]);
        let result: RunResult<()> =
            RunResult::new(snapshots, &storages, (), Duration::from_secs(5));
        assert_eq!(result.outcome, Outcome::NoProgress);
    }

    #[test]
    fn run_result_new_with_commit_log_writes_one_ndjson_line_per_commit() {
        let batch = CommitData::new_for_test(&[(0, 1), (1, 2), (2, 3)]);
        let (storages, snapshots) = build_storages_with_commits(2, &batch);

        let mut buffer = Vec::new();
        let result: RunResult<()> = RunResult::new_with_commit_log(
            snapshots,
            &storages,
            (),
            Duration::from_secs(10),
            &mut buffer,
        )
        .expect("NDJSON write");

        assert_eq!(result.outcome, Outcome::Pass);

        let text = String::from_utf8(buffer).expect("NDJSON is UTF-8");
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 6, "2 replicas × 3 commits");
        for line in &lines {
            let value: serde_json::Value = serde_json::from_str(line).expect("valid JSON");
            assert!(value.get("authority").is_some());
            assert!(value.get("commit").is_some());
        }
    }

    #[test]
    fn run_result_constructors_agree_on_outcome() {
        let batch = CommitData::new_for_test(&[(0, 1), (1, 2)]);
        let (storages_a, snapshots_a) = build_storages_with_commits(3, &batch);
        let (storages_b, snapshots_b) = build_storages_with_commits(3, &batch);

        let plain: RunResult<()> =
            RunResult::new(snapshots_a, &storages_a, (), Duration::from_secs(1));
        let mut sink = Vec::new();
        let logged: RunResult<()> = RunResult::new_with_commit_log(
            snapshots_b,
            &storages_b,
            (),
            Duration::from_secs(1),
            &mut sink,
        )
        .expect("NDJSON write");

        assert_eq!(plain.outcome, logged.outcome);
    }
}
