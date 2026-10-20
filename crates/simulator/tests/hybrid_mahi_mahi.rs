// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Hybrid Mahi-Mahi simulation tests, at both wave lengths. The thresholds are Hybrid
//! Mysticeti's (see `hybrid_mysticeti.rs` for the cheat-sheet).

use std::num::NonZeroUsize;

use consensus::protocol::{ConsensusProtocol, Protocol, ProtocolError};
use dag::authority::Authority;
use dag::metrics::{MetricsSnapshot, SnapshotAggregate};
use replica::config::ReplicaParameters;
use replica::result::{Outcome, RunResult};
use simulator::{NetworkTopology, SimulationConfig, SimulationRunner};

const WAVE_LENGTHS: [u64; 2] = [4, 5];

fn hybrid_mahi_mahi(f: u64, c: u64, leader_count: usize, wave_length: u64) -> ConsensusProtocol {
    ConsensusProtocol::HybridMahiMahi {
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

/// Largest per-replica count for one commit type.
fn max_over_replicas(
    result: &RunResult<SimulationConfig>,
    accessor: fn(&MetricsSnapshot) -> u64,
) -> u64 {
    result.metrics.iter().map(accessor).max().unwrap_or(0)
}

/// Runs the scenario at every wave length and returns the results.
fn run_all_waves(
    committee_size: usize,
    topology: NetworkTopology,
    (f, c, leader_count): (u64, u64, usize),
) -> Vec<RunResult<SimulationConfig>> {
    WAVE_LENGTHS
        .into_iter()
        .map(|wave_length| {
            let consensus = hybrid_mahi_mahi(f, c, leader_count, wave_length);
            run(committee_size, topology.clone(), 30, consensus)
        })
        .collect()
}

#[test]
fn happy_path_bft_n4() {
    // n=4, f=1, c=0: every threshold equals Mahi-Mahi's.
    for result in run_all_waves(4, NetworkTopology::FullMesh, (1, 0, 2)) {
        assert_progress(&result, 10);
    }
}

#[test]
fn happy_path_crash_only_n4() {
    // n=4, f=0, c=1: the tight endpoint, skip + cert = n + 1.
    for result in run_all_waves(4, NetworkTopology::FullMesh, (0, 1, 2)) {
        assert_progress(&result, 10);
    }
}

#[test]
fn happy_path_mixed_n20() {
    // n=20, f=3, c=4: all four quorums are distinct.
    for result in run_all_waves(20, NetworkTopology::FullMesh, (3, 4, 2)) {
        assert_progress(&result, 10);
    }
}

#[test]
fn crash_one_node_n4() {
    // n=4, f=0, c=1: 3 live nodes == CERT, and the dead node's slots draw 3 blames >= SKIP=2,
    // so they are skipped directly even when the wave length is a multiple of n.
    for result in run_all_waves(4, NetworkTopology::OneDown(0), (0, 1, 2)) {
        assert_progress(&result, 10);
        assert!(
            max_over_replicas(&result, MetricsSnapshot::direct_skips) > 0,
            "the crashed node's slots must be directly skipped"
        );
    }
}

#[test]
fn crash_one_node_n20() {
    // n=20, f=3, c=4: 19 live nodes, well above CERT=12 and SKIP=12.
    for result in run_all_waves(20, NetworkTopology::OneDown(0), (3, 4, 2)) {
        assert_progress(&result, 10);
        assert!(
            max_over_replicas(&result, MetricsSnapshot::direct_skips) > 0,
            "the crashed node's slots must be directly skipped"
        );
    }
}

#[test]
fn crash_c_nodes_n20() {
    // n=20, f=0, c=9: cutting off c = 9 nodes leaves 11 == Q == CERT, and the isolated
    // leaders draw 11 blames >= SKIP=10.
    let topology = NetworkTopology::Partition(vec![(0..11).collect(), (11..20).collect()]);
    for result in run_all_waves(20, topology, (0, 9, 2)) {
        assert_progress(&result, 10);
        assert!(
            max_over_replicas(&result, MetricsSnapshot::direct_skips) > 0,
            "the isolated leaders must be directly skipped"
        );
    }
}

#[test]
fn partition_below_quorum_n20() {
    // n=20, f=0, c=9: a 10/10 split leaves both sides below Q=11, so neither side commits.
    let topology = NetworkTopology::Partition(vec![(0..10).collect(), (10..20).collect()]);
    for wave_length in WAVE_LENGTHS {
        let result = run(
            20,
            topology.clone(),
            20,
            hybrid_mahi_mahi(0, 9, 2, wave_length),
        );
        assert_eq!(result.outcome, Outcome::NoProgress);
    }
}

#[test]
fn multi_leader_1_n20() {
    for result in run_all_waves(20, NetworkTopology::FullMesh, (3, 4, 1)) {
        assert_progress(&result, 10);
    }
}

#[test]
fn multi_leader_4_n20() {
    for result in run_all_waves(20, NetworkTopology::FullMesh, (3, 4, 4)) {
        assert_progress(&result, 10);
    }
}

#[test]
fn equivocating_leader_n20() {
    // n=20, f=3, c=4 with authority 3 equivocating: neither twin reaches CERT=12 votes, so its
    // slots are never committed and are decided by indirect skip.
    for wave_length in WAVE_LENGTHS {
        let config = SimulationConfig {
            committee_size: 20,
            equivocating_leaders: vec![3],
            duration_secs: 40,
            replica_parameters: ReplicaParameters {
                consensus: hybrid_mahi_mahi(3, 4, 2, wave_length),
                ..Default::default()
            },
            ..Default::default()
        };
        let result = SimulationRunner::new(config).run().unwrap();
        assert_progress(&result, 10);
        let equivocator = Authority::from(3usize);
        let decided = result
            .metrics
            .iter()
            .map(|metrics| metrics.decided_leaders_of(equivocator))
            .max()
            .unwrap_or(0);
        assert!(
            decided > 0,
            "[wave={wave_length}] equivocated slots must be decided"
        );
        assert!(
            max_over_replicas(&result, MetricsSnapshot::indirect_skips) > 0,
            "[wave={wave_length}] equivocated slots are skipped through an anchor"
        );
    }
}

#[test]
fn infeasible_params_rejected() {
    // n=5, f=1, c=1 needs n >= 3 + 2 + 1 = 6.
    let result = Protocol::hybrid_mahi_mahi(5, 1, 1, NonZeroUsize::new(1).unwrap(), 4);
    assert!(matches!(
        result,
        Err(ProtocolError::FaultBoundViolated {
            protocol: "Hybrid Mahi-Mahi",
            n: 5,
            min_n: 6,
        })
    ));
}

#[test]
fn invalid_wave_length_rejected_in_config() {
    let yaml = "protocol: hybrid-mahi-mahi\nf: 1\nc: 1\nwave_length: 3\n";
    let error = serde_yaml::from_str::<ConsensusProtocol>(yaml)
        .err()
        .unwrap();
    assert!(error.to_string().contains("must be 4 or 5"), "{error}");
}
