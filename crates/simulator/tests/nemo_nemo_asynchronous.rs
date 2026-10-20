// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Async Nemo-Nemo simulation tests, at both wave lengths.
//!
//! Nemo-Nemo's thresholds: COMMIT = CERT = Q = n/2 + 1, SKIP = n (no direct skip), LINK = 1.
//! Without a direct skip, a crashed leader is only skipped through an anchor, which the fake coin
//! keeps from looping back to the crashed node.

use std::num::NonZeroUsize;

use consensus::protocol::ConsensusProtocol;
use dag::metrics::{MetricsSnapshot, SnapshotAggregate};
use replica::config::ReplicaParameters;
use replica::result::{Outcome, RunResult};
use simulator::{NetworkTopology, SimulationConfig, SimulationRunner};

const WAVE_LENGTHS: [u64; 2] = [2, 3];

fn nemo_nemo_asynchronous(leader_count: usize, wave_length: u64) -> ConsensusProtocol {
    ConsensusProtocol::NemoNemoAsynchronous {
        leader_count: NonZeroUsize::new(leader_count).unwrap(),
        wave_length,
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
        "{:?}: expected >= {min_commits} committed leaders, got {commits}",
        result.config.replica_parameters.consensus
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
fn happy_path_n3() {
    // n=3: COMMIT = Q = 2, the CFT minimum.
    for wave_length in WAVE_LENGTHS {
        let result = run(
            3,
            NetworkTopology::FullMesh,
            30,
            nemo_nemo_asynchronous(2, wave_length),
        );
        assert_progress(&result, 10);
    }
}

#[test]
fn happy_path_n20() {
    for wave_length in WAVE_LENGTHS {
        let consensus = nemo_nemo_asynchronous(2, wave_length);
        let result = run(20, NetworkTopology::FullMesh, 30, consensus);
        assert_progress(&result, 10);
    }
}

#[test]
fn crash_one_node_n5() {
    // n=5: 4 live nodes >= COMMIT = Q = 3; the crashed leader's slots are skipped indirectly.
    for wave_length in WAVE_LENGTHS {
        let consensus = nemo_nemo_asynchronous(2, wave_length);
        let result = run(5, NetworkTopology::OneDown(0), 30, consensus);
        assert_progress(&result, 10);
        assert!(
            max_over_replicas(&result, MetricsSnapshot::indirect_skips) > 0,
            "[wave={wave_length}] the crashed node's slots must be skipped through an anchor"
        );
    }
}

#[test]
fn crash_one_node_small_committees() {
    // Round-robin leaders stalled at n=3, wave 3; the fake coin keeps every size live.
    for committee_size in [3, 4] {
        for wave_length in WAVE_LENGTHS {
            let consensus = nemo_nemo_asynchronous(2, wave_length);
            let result = run(committee_size, NetworkTopology::OneDown(0), 30, consensus);
            assert_progress(&result, 10);
        }
    }
}

#[test]
fn partition_majority_n5() {
    // n=5: the 3-node side still meets COMMIT = Q = 3 and commits; the 2-node side stalls.
    let topology = NetworkTopology::Partition(vec![vec![0, 1, 2], vec![3, 4]]);
    for wave_length in WAVE_LENGTHS {
        let result = run(
            5,
            topology.clone(),
            30,
            nemo_nemo_asynchronous(2, wave_length),
        );
        assert_progress(&result, 10);
    }
}

#[test]
fn partition_below_quorum_n4() {
    // n=4: a 2/2 split leaves both sides below Q = 3, so neither side commits.
    let topology = NetworkTopology::Partition(vec![vec![0, 1], vec![2, 3]]);
    for wave_length in WAVE_LENGTHS {
        let result = run(
            4,
            topology.clone(),
            20,
            nemo_nemo_asynchronous(2, wave_length),
        );
        assert_eq!(result.outcome, Outcome::NoProgress);
    }
}

#[test]
fn multi_leader_1_n5() {
    for wave_length in WAVE_LENGTHS {
        let result = run(
            5,
            NetworkTopology::FullMesh,
            30,
            nemo_nemo_asynchronous(1, wave_length),
        );
        assert_progress(&result, 10);
    }
}

#[test]
fn multi_leader_4_n5() {
    for wave_length in WAVE_LENGTHS {
        let result = run(
            5,
            NetworkTopology::FullMesh,
            30,
            nemo_nemo_asynchronous(4, wave_length),
        );
        assert_progress(&result, 10);
    }
}

#[test]
fn invalid_wave_length_rejected_in_config() {
    let yaml = "protocol: nemo-nemo-asynchronous\nwave_length: 4\n";
    let error = serde_yaml::from_str::<ConsensusProtocol>(yaml)
        .err()
        .unwrap();
    assert!(error.to_string().contains("must be 2 or 3"), "{error}");
}
