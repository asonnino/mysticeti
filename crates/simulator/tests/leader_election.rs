// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Liveness of the asynchronous protocols under crash faults with the fake coin.
//!
//! Without a direct skip, a crashed leader's slot is only decided through a later anchor. With
//! round-robin leaders the anchor search could loop back to a crashed node forever whenever
//! `n <= 2 * wave_length - 3` and the crashed nodes are consecutive in leader order; each case
//! below stalled before the asynchronous protocols switched to the fake coin.

use std::num::NonZeroUsize;

use consensus::protocol::ConsensusProtocol;
use dag::metrics::SnapshotAggregate;
use replica::config::ReplicaParameters;
use replica::result::Outcome;
use simulator::{NetworkTopology, SimulationConfig, SimulationRunner};

fn assert_live(committee_size: usize, topology: NetworkTopology, consensus: ConsensusProtocol) {
    let name = format!("{consensus:?}");
    let result = SimulationRunner::new(SimulationConfig {
        committee_size,
        topology,
        duration_secs: 30,
        replica_parameters: ReplicaParameters {
            consensus,
            ..Default::default()
        },
        ..Default::default()
    })
    .run()
    .unwrap();
    assert_eq!(result.outcome, Outcome::Pass, "{name}");
    let commits = SnapshotAggregate::new(&result.metrics).max_committed_leaders();
    assert!(
        commits >= 10,
        "{name}: expected >= 10 committed leaders, got {commits}"
    );
}

fn leader_count(count: usize) -> NonZeroUsize {
    NonZeroUsize::new(count).unwrap()
}

#[test]
fn cordial_miners_asynchronous_crash_n5() {
    // Wave 5 = n: the crashed node was the first anchor candidate of its own slots.
    assert_live(
        5,
        NetworkTopology::OneDown(0),
        ConsensusProtocol::CordialMinersAsynchronous,
    );
}

#[test]
fn orcaella_asynchronous_crash_n3() {
    // Wave 3 = n, f=0, c=1: skip=3 leaves only the indirect rule.
    let consensus = ConsensusProtocol::OrcaellaAsynchronous {
        leader_count: leader_count(2),
        f: 0,
        c: 1,
        wave_length: 3,
    };
    assert_live(3, NetworkTopology::OneDown(0), consensus);
}

#[test]
fn orcaella_asynchronous_adjacent_crashes_n5() {
    // Wave 4, f=0, c=2, with nodes 3 and 4 cut off: under round-robin, node 4's slots waited
    // on node 3's and then on node 4's own next slot, forever.
    for count in [1, 2] {
        let consensus = ConsensusProtocol::OrcaellaAsynchronous {
            leader_count: leader_count(count),
            f: 0,
            c: 2,
            wave_length: 4,
        };
        let topology = NetworkTopology::Partition(vec![vec![0, 1, 2], vec![3, 4]]);
        assert_live(5, topology, consensus);
    }
}
