// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Async Orcaella simulation tests, at both wave lengths.

use std::num::NonZeroUsize;

use consensus::protocol::ConsensusProtocol;
use dag::metrics::SnapshotAggregate;
use replica::config::ReplicaParameters;
use replica::result::{Outcome, RunResult};
use simulator::{NetworkTopology, SimulationConfig, SimulationRunner};

const WAVE_LENGTHS: [u64; 2] = [3, 4];

fn orcaella_asynchronous(
    f: u64,
    c: u64,
    leader_count: usize,
    wave_length: u64,
) -> ConsensusProtocol {
    ConsensusProtocol::OrcaellaAsynchronous {
        leader_count: NonZeroUsize::new(leader_count).unwrap(),
        f,
        c,
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

/// Runs the scenario at every wave length and checks for progress.
fn assert_progress_all_waves(
    committee_size: usize,
    topology: NetworkTopology,
    (f, c, leader_count): (u64, u64, usize),
    min_commits: u64,
) {
    for wave_length in WAVE_LENGTHS {
        let consensus = orcaella_asynchronous(f, c, leader_count, wave_length);
        assert_progress(
            &run(committee_size, topology.clone(), 30, consensus),
            min_commits,
        );
    }
}

#[test]
fn happy_path_crash_only_n3() {
    // n=3, f=0, c=1 → commit=2, skip=3, anchor=1 — CFT minimum (n=2c+1)
    assert_progress_all_waves(3, NetworkTopology::FullMesh, (0, 1, 2), 10);
}

#[test]
fn happy_path_bft_n6() {
    // n=6, f=1, c=0 → commit=5, skip=5, anchor=3 (tight)
    assert_progress_all_waves(6, NetworkTopology::FullMesh, (1, 0, 2), 10);
}

#[test]
fn happy_path_mixed_n9() {
    // n=9, f=1, c=1 → commit=7, skip=7, anchor=4 (tight)
    assert_progress_all_waves(9, NetworkTopology::FullMesh, (1, 1, 2), 10);
}

#[test]
fn crash_one_node_n3() {
    // 2 active == commit threshold, and skip=3 leaves only the indirect rule. Wave 4 only:
    // without a common coin, round-robin leaders at wave 3 = n make the crashed node the
    // first anchor candidate of its own slots, so they are never decided.
    let consensus = orcaella_asynchronous(0, 1, 2, 4);
    assert_progress(&run(3, NetworkTopology::OneDown(0), 30, consensus), 10);
}

#[test]
fn crash_one_node_n5() {
    // n=5, f=0, c=2 → commit=3, skip=5, anchor=1: indirect skips only, no aliasing.
    assert_progress_all_waves(5, NetworkTopology::OneDown(0), (0, 2, 2), 10);
}

#[test]
fn crash_one_node_n6() {
    // commit=5, 5 active nodes — exactly at threshold
    assert_progress_all_waves(6, NetworkTopology::OneDown(0), (1, 0, 2), 10);
}

#[test]
fn crash_one_node_n9() {
    // commit=7, 8 active nodes — one node to spare
    assert_progress_all_waves(9, NetworkTopology::OneDown(0), (1, 1, 2), 10);
}

#[test]
fn partition_at_quorum_n9() {
    // Majority has exactly 7 nodes == commit threshold.
    let topology = NetworkTopology::Partition(vec![vec![0, 1], vec![2, 3, 4, 5, 6, 7, 8]]);
    assert_progress_all_waves(9, topology, (1, 1, 2), 10);
}

#[test]
fn partition_below_quorum_n9() {
    // Majority has 6 nodes < commit=7 — neither side can commit.
    for wave_length in WAVE_LENGTHS {
        let topology = NetworkTopology::Partition(vec![vec![0, 1, 2], vec![3, 4, 5, 6, 7, 8]]);
        let result = run(9, topology, 20, orcaella_asynchronous(1, 1, 2, wave_length));
        assert_eq!(result.outcome, Outcome::NoProgress);
    }
}

#[test]
fn multi_leader_1_n9() {
    assert_progress_all_waves(9, NetworkTopology::FullMesh, (1, 1, 1), 10);
}

#[test]
fn multi_leader_4_n9() {
    assert_progress_all_waves(9, NetworkTopology::FullMesh, (1, 1, 4), 10);
}

#[test]
fn invalid_wave_length_rejected_in_config() {
    let yaml = "protocol: orcaella-asynchronous\nf: 1\nc: 1\nwave_length: 2\n";
    let error = serde_yaml::from_str::<ConsensusProtocol>(yaml)
        .err()
        .unwrap();
    assert!(error.to_string().contains("must be 3 or 4"), "{error}");
}
