// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Hybrid Mysticeti simulation tests.
//!
//! Threshold cheat-sheet for the configurations used below (n >= 3f + 2c + 1):
//!
//! | config | n  | COMMIT = 2f+c+1 | CERT = ⌈(n+f+1)/2⌉ | SKIP = ⌊(n+f+1)/2⌋ | Q = n-f-c |
//! | ------ | -- | --------------- | ------------------ | ------------------ | --------- |
//! | (1, 0) | 4  | 3               | 3                  | 3                  | 3         |
//! | (0, 1) | 4  | 2               | 3                  | 2                  | 3         |
//! | (3, 4) | 20 | 11              | 12                 | 12                 | 13        |
//! | (0, 9) | 20 | 10              | 11                 | 10                 | 11        |

use std::num::NonZeroUsize;

use consensus::protocol::{ConsensusProtocol, Protocol, ProtocolError};
use dag::authority::Authority;
use dag::metrics::{MetricsSnapshot, SnapshotAggregate};
use replica::config::ReplicaParameters;
use replica::result::{Outcome, RunResult};
use simulator::{NetworkTopology, SimulationConfig, SimulationRunner};

fn hybrid_mysticeti(f: u64, c: u64, leader_count: usize) -> ConsensusProtocol {
    ConsensusProtocol::HybridMysticeti {
        leader_count: NonZeroUsize::new(leader_count).unwrap(),
        f,
        c,
    }
}

fn run(
    committee_size: usize,
    topology: NetworkTopology,
    duration_secs: u64,
    consensus: ConsensusProtocol,
) -> RunResult<SimulationConfig> {
    let config = SimulationConfig {
        committee_size,
        topology,
        duration_secs,
        replica_parameters: ReplicaParameters {
            consensus,
            ..Default::default()
        },
        ..Default::default()
    };
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

/// Largest per-replica count for one commit type.
fn max_over_replicas(
    result: &RunResult<SimulationConfig>,
    accessor: fn(&MetricsSnapshot) -> u64,
) -> u64 {
    result.metrics.iter().map(accessor).max().unwrap_or(0)
}

#[test]
fn happy_path_bft_n4() {
    // n=4, f=1, c=0: every threshold equals Mysticeti's.
    let result = run(4, NetworkTopology::FullMesh, 30, hybrid_mysticeti(1, 0, 2));
    assert_progress(&result, 10);
}

#[test]
fn happy_path_crash_only_n4() {
    // n=4, f=0, c=1: the CFT endpoint, n = 2c + 2.
    let result = run(4, NetworkTopology::FullMesh, 30, hybrid_mysticeti(0, 1, 2));
    assert_progress(&result, 10);
}

#[test]
fn happy_path_mixed_n20() {
    // n=20, f=3, c=4: all four quorums are distinct.
    let result = run(20, NetworkTopology::FullMesh, 40, hybrid_mysticeti(3, 4, 2));
    assert_progress(&result, 15);
}

#[test]
fn crash_one_node_n4() {
    // n=4, f=0, c=1: 3 live nodes == CERT, and the dead node's slots draw 3 blames >= SKIP=2.
    let result = run(
        4,
        NetworkTopology::OneDown(0),
        30,
        hybrid_mysticeti(0, 1, 2),
    );
    assert_progress(&result, 10);
    assert!(
        max_over_replicas(&result, MetricsSnapshot::direct_skips) > 0,
        "the crashed node's slots must be directly skipped"
    );
}

#[test]
fn crash_one_node_n20() {
    // n=20, f=3, c=4: 19 live nodes, well above CERT=12 and SKIP=12.
    let result = run(
        20,
        NetworkTopology::OneDown(0),
        40,
        hybrid_mysticeti(3, 4, 2),
    );
    assert_progress(&result, 15);
    assert!(
        max_over_replicas(&result, MetricsSnapshot::direct_skips) > 0,
        "the crashed node's slots must be directly skipped"
    );
}

#[test]
fn crash_c_nodes_n20() {
    // n=20, f=0, c=9: cutting off c = 9 nodes leaves 11 == Q == CERT. The majority keeps
    // committing, and the isolated leaders draw 11 blames >= SKIP=10, so they are skipped
    // directly.
    let topology = NetworkTopology::Partition(vec![(0..11).collect(), (11..20).collect()]);
    let result = run(20, topology, 40, hybrid_mysticeti(0, 9, 2));
    assert_progress(&result, 10);
    assert!(
        max_over_replicas(&result, MetricsSnapshot::direct_skips) > 0,
        "the isolated leaders must be directly skipped"
    );
}

#[test]
fn partition_below_quorum_n20() {
    // n=20, f=0, c=9: a 10/10 split leaves both sides below Q=11, so neither side commits.
    let topology = NetworkTopology::Partition(vec![(0..10).collect(), (10..20).collect()]);
    let result = run(20, topology, 20, hybrid_mysticeti(0, 9, 2));
    assert_eq!(result.outcome, Outcome::NoProgress);
}

#[test]
fn multi_leader_1_n20() {
    let result = run(20, NetworkTopology::FullMesh, 30, hybrid_mysticeti(3, 4, 1));
    assert_progress(&result, 10);
}

#[test]
fn multi_leader_4_n20() {
    let result = run(20, NetworkTopology::FullMesh, 30, hybrid_mysticeti(3, 4, 4));
    assert_progress(&result, 10);
}

#[test]
fn equivocating_leader_n20() {
    // n=20, f=3, c=4 with authority 3 equivocating in its leader rounds: the 9 odd peers see
    // the twin first, the 10 even peers and the leader itself the original, so neither twin
    // reaches CERT=12 votes. Every voting block supports one of the twins, so the slot is never
    // blamed either: equivocated slots are decided by indirect skip, never by a direct skip.
    for rng_seed in [0, 7] {
        let config = SimulationConfig {
            committee_size: 20,
            equivocating_leaders: vec![3],
            duration_secs: 40,
            rng_seed,
            replica_parameters: ReplicaParameters {
                consensus: hybrid_mysticeti(3, 4, 2),
                ..Default::default()
            },
            ..Default::default()
        };
        let result = SimulationRunner::new(config).run().unwrap();
        assert_progress(&result, 15);
        let equivocator = Authority::from(3usize);
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
            max_over_replicas(&result, MetricsSnapshot::indirect_skips) > 0,
            "[seed={rng_seed}] equivocated slots are skipped through an anchor"
        );
        assert_eq!(
            max_over_replicas(&result, MetricsSnapshot::direct_skips),
            0,
            "[seed={rng_seed}] the equivocator stays live, so none of its slots is blamed"
        );
    }
}

#[test]
fn infeasible_params_rejected() {
    // n=5, f=1, c=1 needs n >= 3 + 2 + 1 = 6.
    let result = Protocol::hybrid_mysticeti(5, 1, 1, NonZeroUsize::new(1).unwrap());
    assert!(matches!(
        result,
        Err(ProtocolError::FaultBoundViolated {
            protocol: "Hybrid Mysticeti",
            n: 5,
            min_n: 6,
        })
    ));
}

#[test]
fn config_round_trip() {
    let yaml = "protocol: hybrid-mysticeti\nleader_count: 2\nf: 3\nc: 4\n";
    let consensus: ConsensusProtocol = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(format!("{consensus:?}"), "hybrid-mysticeti-l2-f3-c4");
}
