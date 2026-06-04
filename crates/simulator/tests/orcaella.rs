// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Orcaella simulation tests.
//!
//! Covers happy-path, crash faults (OneDown), partition boundary cases,
//! non-optimal (k>0) committee sizes, multi-leader, and feasibility errors.
//! Byzantine equivocation is not injectable with the current simulator and
//! is out of scope here.

use std::num::NonZeroUsize;

use consensus::protocol::{ConsensusProtocol, Protocol, ProtocolError};
use dag::metrics::SnapshotAggregate;
use replica::config::ReplicaParameters;
use replica::result::{Outcome, RunResult};
use simulator::{NetworkTopology, SimulationConfig, SimulationRunner};

fn orcaella(f: u64, c: u64, leader_count: usize) -> ConsensusProtocol {
    ConsensusProtocol::Orcaella {
        leader_count: NonZeroUsize::new(leader_count).unwrap(),
        f,
        c,
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

#[test]
fn happy_path_crash_only_n4() {
    // n=4, f=0, c=1 → commit=3, skip=3, anchor=2 (tight)
    assert_progress(
        &run(SimulationConfig {
            committee_size: 4,
            duration_secs: 30,
            replica_parameters: ReplicaParameters {
                consensus: orcaella(0, 1, 2),
                ..Default::default()
            },
            ..Default::default()
        }),
        10,
    );
}

#[test]
fn happy_path_bft_n6() {
    // n=6, f=1, c=0 → commit=5, skip=5, anchor=3 (tight)
    assert_progress(
        &run(SimulationConfig {
            committee_size: 6,
            duration_secs: 30,
            replica_parameters: ReplicaParameters {
                consensus: orcaella(1, 0, 2),
                ..Default::default()
            },
            ..Default::default()
        }),
        10,
    );
}

#[test]
fn happy_path_mixed_n9() {
    // n=9, f=1, c=1 → commit=7, skip=7, anchor=5 (tight)
    assert_progress(
        &run(SimulationConfig {
            committee_size: 9,
            duration_secs: 30,
            replica_parameters: ReplicaParameters {
                consensus: orcaella(1, 1, 2),
                ..Default::default()
            },
            ..Default::default()
        }),
        10,
    );
}

#[test]
fn crash_one_node_n4() {
    // commit=3, 3 active nodes — exactly at threshold
    assert_progress(
        &run(SimulationConfig {
            committee_size: 4,
            topology: NetworkTopology::OneDown(0),
            duration_secs: 30,
            replica_parameters: ReplicaParameters {
                consensus: orcaella(0, 1, 2),
                ..Default::default()
            },
            ..Default::default()
        }),
        10,
    );
}

#[test]
fn crash_one_node_n6() {
    // commit=5, 5 active nodes — exactly at threshold
    assert_progress(
        &run(SimulationConfig {
            committee_size: 6,
            topology: NetworkTopology::OneDown(0),
            duration_secs: 30,
            replica_parameters: ReplicaParameters {
                consensus: orcaella(1, 0, 2),
                ..Default::default()
            },
            ..Default::default()
        }),
        10,
    );
}

#[test]
fn crash_one_node_n9() {
    // commit=7, 8 active nodes — one node to spare
    assert_progress(
        &run(SimulationConfig {
            committee_size: 9,
            topology: NetworkTopology::OneDown(0),
            duration_secs: 30,
            replica_parameters: ReplicaParameters {
                consensus: orcaella(1, 1, 2),
                ..Default::default()
            },
            ..Default::default()
        }),
        10,
    );
}

#[test]
fn partition_at_quorum_n9() {
    // Majority has exactly 7 nodes == commit threshold — just barely makes progress.
    assert_progress(
        &run(SimulationConfig {
            committee_size: 9,
            topology: NetworkTopology::Partition(vec![vec![0, 1], vec![2, 3, 4, 5, 6, 7, 8]]),
            duration_secs: 40,
            replica_parameters: ReplicaParameters {
                consensus: orcaella(1, 1, 2),
                ..Default::default()
            },
            ..Default::default()
        }),
        10,
    );
}

#[test]
fn partition_below_quorum_n9() {
    // Majority has 6 nodes < commit=7 — neither side can commit.
    let result = run(SimulationConfig {
        committee_size: 9,
        topology: NetworkTopology::Partition(vec![vec![0, 1, 2], vec![3, 4, 5, 6, 7, 8]]),
        duration_secs: 20,
        replica_parameters: ReplicaParameters {
            consensus: orcaella(1, 1, 2),
            ..Default::default()
        },
        ..Default::default()
    });
    assert_eq!(result.outcome, Outcome::NoProgress);
}

#[test]
fn non_optimal_crash_only_n20() {
    // n=20, f=0, c=1 → commit=19, skip=3, anchor=18 (k=16)
    assert_progress(
        &run(SimulationConfig {
            committee_size: 20,
            duration_secs: 40,
            replica_parameters: ReplicaParameters {
                consensus: orcaella(0, 1, 2),
                ..Default::default()
            },
            ..Default::default()
        }),
        15,
    );
}

#[test]
fn non_optimal_bft_n20() {
    // n=20, f=1, c=0 → commit=19, skip=5, anchor=17 (k=14)
    assert_progress(
        &run(SimulationConfig {
            committee_size: 20,
            duration_secs: 40,
            replica_parameters: ReplicaParameters {
                consensus: orcaella(1, 0, 2),
                ..Default::default()
            },
            ..Default::default()
        }),
        15,
    );
}

#[test]
fn non_optimal_mixed_n20() {
    // n=20, f=1, c=1 → commit=18, skip=7, anchor=15 (k=11)
    assert_progress(
        &run(SimulationConfig {
            committee_size: 20,
            duration_secs: 40,
            replica_parameters: ReplicaParameters {
                consensus: orcaella(1, 1, 2),
                ..Default::default()
            },
            ..Default::default()
        }),
        15,
    );
}

#[test]
fn multi_leader_1_n9() {
    assert_progress(
        &run(SimulationConfig {
            committee_size: 9,
            duration_secs: 30,
            replica_parameters: ReplicaParameters {
                consensus: orcaella(1, 1, 1),
                ..Default::default()
            },
            ..Default::default()
        }),
        10,
    );
}

#[test]
fn multi_leader_4_n9() {
    assert_progress(
        &run(SimulationConfig {
            committee_size: 9,
            duration_secs: 30,
            replica_parameters: ReplicaParameters {
                consensus: orcaella(1, 1, 4),
                ..Default::default()
            },
            ..Default::default()
        }),
        10,
    );
}

#[test]
fn infeasible_params_rejected() {
    // n=4, f=1, c=0: 5*1 + 3*0 + 1 = 6 > 4 — must be rejected
    let result = Protocol::orcaella(4, 1, 0, NonZeroUsize::new(1).unwrap());
    assert!(matches!(
        result,
        Err(ProtocolError::OrcaellaFaultBoundViolated { n: 4, f: 1, c: 0 })
    ));
}
