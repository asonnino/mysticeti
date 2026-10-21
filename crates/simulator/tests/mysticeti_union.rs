// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Mysticeti with union certificates: simulation tests.
//!
//! The configurations of the stall use a link delay far above the leader timeout (600 to 1000 ms
//! against 100 ms), so that every validator proposes with a minimum quorum of references and no
//! single block references a quorum of votes.

use std::{num::NonZeroUsize, time::Duration};

use consensus::protocol::ConsensusProtocol;
use dag::{config::DagParameters, metrics::SnapshotAggregate};
use replica::{
    config::ReplicaParameters,
    result::{Outcome, RunResult},
};
use simulator::{
    LatencyModel, NetworkTopology, SimulationConfig, SimulationRunner, UniformLatency,
};

fn mysticeti_union() -> ConsensusProtocol {
    ConsensusProtocol::MysticetiUnion {
        leader_count: NonZeroUsize::new(1).unwrap(),
    }
}

fn mysticeti() -> ConsensusProtocol {
    ConsensusProtocol::Mysticeti {
        leader_count: NonZeroUsize::new(1).unwrap(),
    }
}

fn committed_leaders(result: &RunResult<SimulationConfig>) -> u64 {
    SnapshotAggregate::new(&result.metrics).max_committed_leaders()
}

fn assert_progress(result: &RunResult<SimulationConfig>, min_commits: u64) {
    assert_eq!(result.outcome, Outcome::Pass);
    let commits = committed_leaders(result);
    assert!(
        commits >= min_commits,
        "expected >= {min_commits} committed leaders, got {commits}"
    );
}

/// A run whose link delay is far above the leader timeout.
fn run_with_mistimed_timeout(
    committee_size: usize,
    consensus: ConsensusProtocol,
    equivocating_leaders: Vec<usize>,
) -> RunResult<SimulationConfig> {
    let config = SimulationConfig {
        committee_size,
        latency: LatencyModel::Uniform(UniformLatency {
            range_ms: 600..1000,
        }),
        duration_secs: 120,
        rng_seed: 1,
        replica_parameters: ReplicaParameters {
            dag: DagParameters {
                round_timeout: Some(Duration::from_millis(100)),
                ..Default::default()
            },
            consensus,
        },
        equivocating_leaders,
        ..Default::default()
    };
    SimulationRunner::new(config).run().unwrap()
}

fn run(committee_size: usize, duration_secs: u64) -> RunResult<SimulationConfig> {
    let config = SimulationConfig {
        committee_size,
        topology: NetworkTopology::FullMesh,
        duration_secs,
        replica_parameters: ReplicaParameters {
            consensus: mysticeti_union(),
            ..Default::default()
        },
        ..Default::default()
    };
    SimulationRunner::new(config).run().unwrap()
}

#[test]
fn happy_path_n4() {
    assert_progress(&run(4, 30), 10);
}

#[test]
fn happy_path_n20() {
    assert_progress(&run(20, 40), 15);
}

#[test]
fn commits_when_the_leader_timeout_is_too_short_n10() {
    // With a minimum quorum of references Mysticeti commits at most sporadically (0 here), while
    // the union of the votes held by the certify-round blocks still reaches a quorum.
    let union = run_with_mistimed_timeout(10, mysticeti_union(), vec![]);
    assert_progress(&union, 10);
    let plain = run_with_mistimed_timeout(10, mysticeti(), vec![]);
    assert!(
        committed_leaders(&union) > committed_leaders(&plain),
        "union {} vs mysticeti {}",
        committed_leaders(&union),
        committed_leaders(&plain)
    );
}

#[test]
fn equivocating_leaders_with_a_short_leader_timeout_n10() {
    // Up to f = 3 equivocating leaders: the replicas must never disagree on the committed
    // sequence. Progress is not asserted: the simulator is not bit-reproducible, and with this
    // many equivocators and a timeout this short the first commit can take minutes.
    let result = run_with_mistimed_timeout(10, mysticeti_union(), vec![2, 5, 8]);
    assert_ne!(result.outcome, Outcome::Diverged);
}

#[test]
fn config_round_trip() {
    let yaml = "protocol: mysticeti-union\nleader_count: 2\n";
    let consensus: ConsensusProtocol = serde_yaml::from_str(yaml).unwrap();
    assert_eq!(format!("{consensus:?}"), "mysticeti-union-l2");
}
