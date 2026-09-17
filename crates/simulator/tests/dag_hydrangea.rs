// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! DagHydrangea simulation tests.
//!
//! Threshold cheat-sheet for the configurations used below
//! (n = 3f + 2c + k + 1, p = (c + k) / 2):
//!
//! | config     | n  | p | FAST=SKIP | W | CERT | SLOW | Q  |
//! | ---------- | -- | - | --------- | - | ---- | ---- | -- |
//! | (1, 0, 0)  | 4  | 0 | 4         | 2 | 3    | 3    | 3  |
//! | (0, 1, 1)  | 4  | 1 | 3         | 2 | 3    | 2    | 3  |
//! | (3, 4, 2)  | 20 | 3 | 17        | 7 | 12   | 11   | 13 |
//! | (0, 9, 1)  | 20 | 5 | 15        | 6 | 11   | 10   | 11 |
//! | (4, 1, 5)  | 20 | 3 | 17        | 8 | 13   | 10   | 15 |
//! | (2, 3, 7)  | 20 | 5 | 15        | 8 | 12   | 8    | 15 |

use std::num::NonZeroUsize;

use consensus::protocol::{ConsensusProtocol, Protocol, ProtocolError};
use dag::authority::Authority;
use dag::metrics::{BlockKind, MetricsSnapshot, SnapshotAggregate};
use replica::config::ReplicaParameters;
use replica::result::{Outcome, RunResult};
use simulator::{NetworkTopology, SimulationConfig, SimulationRunner};

fn dag_hydrangea(f: u64, c: u64, k: u64, leader_count: usize) -> ConsensusProtocol {
    ConsensusProtocol::DagHydrangea {
        leader_count: NonZeroUsize::new(leader_count).unwrap(),
        f,
        c,
        k,
    }
}

fn run(config: SimulationConfig) -> RunResult<SimulationConfig> {
    SimulationRunner::new(config).run().unwrap()
}

fn assert_progress(result: &RunResult<SimulationConfig>, min_commits: u64) {
    assert_eq!(result.outcome, Outcome::Pass);
    let commits = SnapshotAggregate::new(&result.metrics).max_committed_leaders();
    assert!(
        commits >= min_commits,
        "expected >= {min_commits} committed leaders, got {commits}"
    );
}

/// Largest per-replica count for one commit type, e.g.
/// `max_over_replicas(&result, MetricsSnapshot::direct_skips)`.
fn max_over_replicas(
    result: &RunResult<SimulationConfig>,
    accessor: fn(&MetricsSnapshot) -> u64,
) -> u64 {
    result.metrics.iter().map(accessor).max().unwrap_or(0)
}

#[test]
fn happy_path_bft_n4() {
    // n=4, f=1, c=0, k=0 → p=0: the fast path needs unanimity (FAST=4); the
    // certificate, slow-commit, and pacemaker quorums coincide at 3.
    assert_progress(
        &run(SimulationConfig {
            committee_size: 4,
            duration_secs: 30,
            replica_parameters: ReplicaParameters {
                consensus: dag_hydrangea(1, 0, 0, 2),
                ..Default::default()
            },
            ..Default::default()
        }),
        10,
    );
}

#[test]
fn fast_dominant_crash_slack_n4() {
    // n=4, f=0, c=1, k=1 → p=1: fast-dominant configuration (CERT = FAST = 3).
    // A certificate references >= CERT = FAST votes at the voting round, so the
    // fast rule fires before any certificate can: every direct commit is fast.
    let result = run(SimulationConfig {
        committee_size: 4,
        duration_secs: 30,
        replica_parameters: ReplicaParameters {
            consensus: dag_hydrangea(0, 1, 1, 2),
            ..Default::default()
        },
        ..Default::default()
    });
    assert_progress(&result, 10);
    assert!(
        max_over_replicas(&result, MetricsSnapshot::fast_commits) > 0,
        "fast-dominant configuration must commit through the fast path"
    );
    assert_eq!(
        max_over_replicas(&result, MetricsSnapshot::slow_commits),
        0,
        "the fast path fires before any certificate forms"
    );
}

#[test]
fn happy_path_mixed_n20() {
    // n=20, f=3, c=4, k=2: the certificate (12), slow-commit (11), and
    // pacemaker (13) quorums are all distinct.
    assert_progress(
        &run(SimulationConfig {
            committee_size: 20,
            duration_secs: 40,
            replica_parameters: ReplicaParameters {
                consensus: dag_hydrangea(3, 4, 2, 2),
                ..Default::default()
            },
            ..Default::default()
        }),
        15,
    );
}

#[test]
fn happy_path_wide_slack_n20() {
    // n=20, f=4, c=1, k=5: widest spread between the three quorums.
    assert_progress(
        &run(SimulationConfig {
            committee_size: 20,
            duration_secs: 40,
            replica_parameters: ReplicaParameters {
                consensus: dag_hydrangea(4, 1, 5, 2),
                ..Default::default()
            },
            ..Default::default()
        }),
        15,
    );
}

#[test]
fn crash_one_node_n4() {
    // n=4, f=0, c=1, k=1: 3 live nodes == FAST — fast commits at the exact
    // boundary, and the dead node's slots draw 3 blames == SKIP → direct skip.
    let result = run(SimulationConfig {
        committee_size: 4,
        topology: NetworkTopology::OneDown(0),
        duration_secs: 30,
        replica_parameters: ReplicaParameters {
            consensus: dag_hydrangea(0, 1, 1, 2),
            ..Default::default()
        },
        ..Default::default()
    });
    assert_progress(&result, 10);
    assert!(
        max_over_replicas(&result, MetricsSnapshot::fast_commits) > 0,
        "the live nodes must still reach the fast quorum"
    );
    assert!(
        max_over_replicas(&result, MetricsSnapshot::direct_skips) > 0,
        "the crashed node's slots must be directly skipped"
    );
}

#[test]
fn crash_one_node_n20() {
    // n=20, f=3, c=4, k=2: 19 live ≥ FAST=17 votes and ≥ SKIP=17 blames.
    let result = run(SimulationConfig {
        committee_size: 20,
        topology: NetworkTopology::OneDown(0),
        duration_secs: 40,
        replica_parameters: ReplicaParameters {
            consensus: dag_hydrangea(3, 4, 2, 2),
            ..Default::default()
        },
        ..Default::default()
    });
    assert_progress(&result, 15);
    assert!(
        max_over_replicas(&result, MetricsSnapshot::direct_skips) > 0,
        "the crashed node's slots must be directly skipped"
    );
}

/// Mean proposal-to-commit latency of leader blocks over all replicas, in seconds.
fn mean_leader_latency(result: &RunResult<SimulationConfig>) -> f64 {
    let (sum, count) = result
        .metrics
        .iter()
        .filter_map(|metrics| metrics.block_latency_sum_and_count(BlockKind::Leader))
        .fold((0.0, 0), |(sum, count), (s, c)| (sum + s, count + c));
    assert!(count > 0, "no committed leader blocks");
    sum / count as f64
}

/// A fast quorum above the threshold-clock quorum (4, 1, 5: FAST=17 > Q=15) must not be
/// slower than one equal to it (2, 3, 7: FAST=Q=15). Without the fast-commit trigger in
/// `Syncer::add_blocks` the former only commits at the next own proposal, a round late.
#[test]
fn fast_quorum_above_clock_quorum_commits_without_waiting_for_a_proposal_n20() {
    let run_with = |f, c, k| {
        let result = run(SimulationConfig {
            committee_size: 20,
            duration_secs: 30,
            replica_parameters: ReplicaParameters {
                consensus: dag_hydrangea(f, c, k, 2),
                ..Default::default()
            },
            ..Default::default()
        });
        assert_progress(&result, 50);
        assert_eq!(
            max_over_replicas(&result, MetricsSnapshot::slow_commits),
            0,
            "({f}, {c}, {k}) commits every leader through the fast path"
        );
        mean_leader_latency(&result)
    };
    let above_clock = run_with(4, 1, 5);
    let at_clock = run_with(2, 3, 7);
    println!("mean leader latency: (4, 1, 5) {above_clock:.3}s, (2, 3, 7) {at_clock:.3}s");
    assert!(
        above_clock <= at_clock * 1.15,
        "fast quorum above the clock quorum: {above_clock:.3}s, at the clock quorum: {at_clock:.3}s"
    );
}

#[test]
fn slow_path_partition_n20() {
    // n=20, f=0, c=9, k=1: the majority size pins every leader's support at 14.
    // - 14 ∈ [CERT=11, FAST=15): the fast path is unreachable, so every direct
    //   commit is a slow-path (certified) commit;
    // - the 6 isolated leaders draw 14 blames < SKIP=15: never directly
    //   skipped, resolved as indirect skips via later anchors;
    // - 14 < SKIP=15 also means no direct skip can occur at all;
    // - rounds advance (14 ≥ Q=11) while the isolated side stalls (6 < Q) and
    //   commits nothing — an empty sequence is a valid prefix, so the run
    //   still passes the consistency check.
    //
    // The assertions hold by construction (the topology pins the support
    // ceiling), not by timing luck — swept over seeds to prove it.
    for rng_seed in [0, 7, 42] {
        let result = run(SimulationConfig {
            committee_size: 20,
            topology: NetworkTopology::Partition(vec![(0..14).collect(), (14..20).collect()]),
            duration_secs: 40,
            rng_seed,
            replica_parameters: ReplicaParameters {
                consensus: dag_hydrangea(0, 9, 1, 2),
                ..Default::default()
            },
            ..Default::default()
        });
        assert_progress(&result, 10);
        assert!(
            max_over_replicas(&result, MetricsSnapshot::slow_commits) > 0,
            "[seed={rng_seed}] the majority must commit through the slow path"
        );
        assert_eq!(
            max_over_replicas(&result, MetricsSnapshot::fast_commits),
            0,
            "[seed={rng_seed}] the fast quorum (15) is unreachable with 14 connected nodes"
        );
        assert!(
            max_over_replicas(&result, MetricsSnapshot::indirect_skips) > 0,
            "[seed={rng_seed}] isolated leaders must be indirectly skipped"
        );
        assert_eq!(
            max_over_replicas(&result, MetricsSnapshot::direct_skips),
            0,
            "[seed={rng_seed}] the skip quorum (15) is unreachable with 14 connected nodes"
        );
    }
}

#[test]
fn partition_below_quorum_n20() {
    // n=20, f=0, c=9, k=1: a 10/10 split leaves both sides below the pacemaker
    // quorum Q=11 — the threshold clock stalls and neither side commits.
    let result = run(SimulationConfig {
        committee_size: 20,
        topology: NetworkTopology::Partition(vec![(0..10).collect(), (10..20).collect()]),
        duration_secs: 20,
        replica_parameters: ReplicaParameters {
            consensus: dag_hydrangea(0, 9, 1, 2),
            ..Default::default()
        },
        ..Default::default()
    });
    assert_eq!(result.outcome, Outcome::NoProgress);
}

#[test]
fn multi_leader_1_n20() {
    assert_progress(
        &run(SimulationConfig {
            committee_size: 20,
            duration_secs: 30,
            replica_parameters: ReplicaParameters {
                consensus: dag_hydrangea(3, 4, 2, 1),
                ..Default::default()
            },
            ..Default::default()
        }),
        10,
    );
}

#[test]
fn multi_leader_4_n20() {
    assert_progress(
        &run(SimulationConfig {
            committee_size: 20,
            duration_secs: 30,
            replica_parameters: ReplicaParameters {
                consensus: dag_hydrangea(3, 4, 2, 4),
                ..Default::default()
            },
            ..Default::default()
        }),
        10,
    );
}

#[test]
fn infeasible_params_rejected() {
    // n=4, f=1, c=0, k=1 needs n >= 3*1+0+1+1 = 5 — rejected.
    let result = Protocol::dag_hydrangea(4, 1, 0, 1, NonZeroUsize::new(1).unwrap());
    assert!(matches!(
        result,
        Err(ProtocolError::FaultBoundViolated {
            protocol: "DagHydrangea",
            n: 4,
            min_n: 5,
        })
    ));
    // n=19, f=3, c=4, k=2 needs n >= 9+8+2+1 = 20 — rejected.
    let result = Protocol::dag_hydrangea(19, 3, 4, 2, NonZeroUsize::new(1).unwrap());
    assert!(matches!(
        result,
        Err(ProtocolError::FaultBoundViolated {
            protocol: "DagHydrangea",
            n: 19,
            min_n: 20,
        })
    ));
}

#[test]
fn equivocating_leader_n20() {
    // n=20, f=3, c=4, k=2 with authority 3 equivocating in its leader rounds: the
    // 9 odd peers see the twin first, the 10 even peers and the leader itself the
    // original, so no twin reaches FAST=17 votes or CERT=12 certificates. Both
    // twins gather at least W=7 anchor-linked votes, so every equivocated slot is
    // decided by the weak-quorum rung with its digest tie-break, never by the fast
    // path and never by a skip (the equivocator stays live, its slots are never
    // empty). Observed: 42 equivocated slots, all weak-rung commits, on both seeds.
    for rng_seed in [0, 7] {
        let result = run(SimulationConfig {
            committee_size: 20,
            equivocating_leaders: vec![3],
            duration_secs: 40,
            rng_seed,
            replica_parameters: ReplicaParameters {
                consensus: dag_hydrangea(3, 4, 2, 2),
                ..Default::default()
            },
            ..Default::default()
        });
        assert_progress(&result, 15);
        let equivocator = Authority::from(3usize);
        for metrics in &result.metrics {
            assert_eq!(
                metrics.fast_commits_of(equivocator),
                0,
                "[seed={rng_seed}] an equivocated slot can never gather a fast quorum"
            );
        }
        let decided = result
            .metrics
            .iter()
            .map(|metrics| metrics.decided_leaders_of(equivocator))
            .max()
            .unwrap_or(0);
        assert!(
            decided > 0,
            "[seed={rng_seed}] equivocated slots must be decided"
        );
        assert!(
            max_over_replicas(&result, MetricsSnapshot::indirect_weak_commits) > 0,
            "[seed={rng_seed}] equivocated slots are committed by the weak-quorum rung"
        );
        assert_eq!(
            max_over_replicas(&result, MetricsSnapshot::direct_skips),
            0,
            "[seed={rng_seed}] the equivocator stays live, so none of its slots is skipped"
        );
        assert!(
            max_over_replicas(&result, MetricsSnapshot::fast_commits) > 0,
            "[seed={rng_seed}] the other slots keep committing through the fast path"
        );
    }
}
