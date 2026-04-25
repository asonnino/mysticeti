// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Run-level results derived from per-replica observations.
//!
//! `RunResult<C>` is the shared container that sim, testbed, and smoke tests build from a
//! set of `MetricsSnapshot`s plus per-replica storages. `Outcome` classifies the run; it is
//! computed at construction by a streaming consistency check over each storage's committed
//! sub-dag sequence (no commit history retained in memory).

use std::{borrow::Borrow, io, time::Duration};

use blake2::Blake2b;
use digest::{Digest, consts::U32};
use serde::Serialize;

use super::{MetricsSnapshot, names::LATENCY_S};
use crate::{
    authority::Authority, crypto::CryptoHash, storage::Storage, storage::block_store::CommitData,
};

type PrefixHasher = Blake2b<U32>;

/// One NDJSON line emitted by [`RunResultBuilder::with_dag_log`]. The JSON
/// shape comes from the [`serde::Serialize`] derive; field order here is the
/// on-disk order.
#[derive(Serialize)]
struct DagRecord<'a> {
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
/// Generic over `C` so simulator, testbed, and smoke-test configs all share this
/// type. `outcome` is precomputed at construction from a streaming consistency check; the
/// commit history is never materialised in memory.
pub struct RunResult<C> {
    pub metrics: Vec<MetricsSnapshot>,
    pub outcome: Outcome,
    pub config: C,
    pub duration: Duration,
}

impl<C> RunResult<C> {
    /// Start configuring a run-result. Holds the inputs; pass optional knobs via
    /// `with_*` methods, then call [`RunResultBuilder::collect`] to run the
    /// consistency check and produce the `RunResult<C>`.
    pub fn builder<S: Borrow<Storage>>(
        metrics: Vec<MetricsSnapshot>,
        storages: &[S],
        config: C,
        duration: Duration,
    ) -> RunResultBuilder<'_, C, S> {
        RunResultBuilder {
            metrics,
            storages,
            config,
            duration,
            dag_log: None,
        }
    }

    /// 50th-percentile end-to-end transaction latency (ms), averaged across replicas.
    pub fn p50_latency_ms(&self) -> Option<f64> {
        self.latency_percentile_ms(0.5)
    }

    /// 90th-percentile end-to-end transaction latency (ms), averaged across replicas.
    pub fn p90_latency_ms(&self) -> Option<f64> {
        self.latency_percentile_ms(0.9)
    }

    fn latency_percentile_ms(&self, p: f64) -> Option<f64> {
        let per_replica: Vec<f64> = self
            .metrics
            .iter()
            .filter_map(|s| s.latency_percentile_ms(p))
            .collect();
        if per_replica.is_empty() {
            None
        } else {
            Some(per_replica.iter().sum::<f64>() / per_replica.len() as f64)
        }
    }

    /// Per-replica count of committed leaders, in authority order. Derived from each
    /// snapshot's `committed_leaders_total` counter (summed across all leader authorities),
    /// so the values stay meaningful whether the result came from a sim, a local testbed,
    /// or a future smoke test.
    pub fn leaders_committed_per_replica(&self) -> Vec<usize> {
        self.metrics
            .iter()
            .map(|snapshot| snapshot.total_committed_leaders() as usize)
            .collect()
    }

    /// Mean committed-leader rate (leaders/s) across replicas. `None` when `duration` is
    /// zero or no replica committed anything.
    pub fn leaders_committed_per_second(&self) -> Option<f64> {
        if self.duration.is_zero() {
            return None;
        }
        let totals: Vec<u64> = self
            .metrics
            .iter()
            .map(MetricsSnapshot::total_committed_leaders)
            .collect();
        if totals.is_empty() {
            return None;
        }
        let mean = totals.iter().sum::<u64>() as f64 / totals.len() as f64;
        (mean > 0.0).then(|| mean / self.duration.as_secs_f64())
    }

    /// Mean committed-transaction rate (TPS) across replicas. Each replica's `latency_s`
    /// histogram sample-count equals the count of committed transactions it observed.
    /// `None` when no transactions committed or `duration` is zero.
    pub fn transactions_committed_per_second(&self) -> Option<f64> {
        if self.duration.is_zero() {
            return None;
        }
        let counts: Vec<u64> = self
            .metrics
            .iter()
            .filter_map(|s| s.histogram_sum_and_count(LATENCY_S))
            .map(|(_, count)| count)
            .collect();
        if counts.is_empty() {
            return None;
        }
        let mean = counts.iter().sum::<u64>() as f64 / counts.len() as f64;
        (mean > 0.0).then(|| mean / self.duration.as_secs_f64())
    }
}

/// Builder for [`RunResult`]. Configure optional knobs (e.g. a DAG-log writer) then
/// finalise via [`collect`](Self::collect), which runs the consistency check and
/// produces the `RunResult<C>`.
pub struct RunResultBuilder<'a, C, S: Borrow<Storage>> {
    metrics: Vec<MetricsSnapshot>,
    storages: &'a [S],
    config: C,
    duration: Duration,
    dag_log: Option<&'a mut dyn io::Write>,
}

impl<'a, C, S: Borrow<Storage>> RunResultBuilder<'a, C, S> {
    /// Stream every committed sub-DAG to `writer` as newline-delimited JSON (one
    /// object per line, `{"authority":…, "commit":…}`). The writer is driven in a
    /// separate pass over each storage before the consistency check, so memory stays
    /// bounded — but the WAL is now scanned twice per replica.
    pub fn with_dag_log(mut self, writer: &'a mut dyn io::Write) -> Self {
        self.dag_log = Some(writer);
        self
    }

    /// Run the consistency check and produce the `RunResult<C>`. Storages are streamed
    /// once (or twice when `with_dag_log` is set); no commit is retained in memory.
    ///
    /// Potentially heavy on long runs: each storage's WAL is streamed end-to-end.
    pub fn collect(self) -> io::Result<RunResult<C>> {
        let RunResultBuilder {
            metrics,
            storages,
            config,
            duration,
            dag_log,
        } = self;
        if let Some(writer) = dag_log {
            for (index, storage) in storages.iter().enumerate() {
                let authority = Authority::from(index as u64);
                for commit in storage.borrow().iter_commits() {
                    let record = DagRecord {
                        authority,
                        commit: &commit,
                    };
                    serde_json::to_writer(&mut *writer, &record).map_err(io::Error::other)?;
                    writeln!(writer)?;
                }
            }
        }
        let outcome = Outcome::from(storages.iter().map(|s| s.borrow().iter_commits()));
        Ok(RunResult {
            metrics,
            outcome,
            config,
            duration,
        })
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
    fn run_result_collect_classifies_consistent_run_as_pass() {
        let batch = CommitData::new_for_test(&[(0, 1), (1, 2), (2, 3)]);
        let (storages, snapshots) = build_storages_with_commits(3, &batch);

        let result: RunResult<()> =
            RunResult::builder(snapshots, &storages, (), Duration::from_secs(30))
                .collect()
                .expect("collect");

        assert_eq!(result.outcome, Outcome::Pass);
        assert_eq!(result.duration, Duration::from_secs(30));
        assert_eq!(result.metrics.len(), 3);
    }

    #[test]
    fn run_result_collect_classifies_empty_run_as_no_progress() {
        let (storages, snapshots) = build_storages_with_commits(2, &[]);
        let result: RunResult<()> =
            RunResult::builder(snapshots, &storages, (), Duration::from_secs(5))
                .collect()
                .expect("collect");
        assert_eq!(result.outcome, Outcome::NoProgress);
    }

    #[test]
    fn run_result_collect_with_dag_log_writes_one_ndjson_line_per_commit() {
        let batch = CommitData::new_for_test(&[(0, 1), (1, 2), (2, 3)]);
        let (storages, snapshots) = build_storages_with_commits(2, &batch);

        let mut buffer = Vec::new();
        let result: RunResult<()> =
            RunResult::builder(snapshots, &storages, (), Duration::from_secs(10))
                .with_dag_log(&mut buffer)
                .collect()
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
            RunResult::builder(snapshots_a, &storages_a, (), Duration::from_secs(1))
                .collect()
                .expect("collect");
        let mut sink = Vec::new();
        let logged: RunResult<()> =
            RunResult::builder(snapshots_b, &storages_b, (), Duration::from_secs(1))
                .with_dag_log(&mut sink)
                .collect()
                .expect("collect");

        assert_eq!(plain.outcome, logged.outcome);
    }
}
