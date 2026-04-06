// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dag::config::{ImportExport, NodeParameters};
use simulator::{NetworkTopology, SimulationConfig, SimulationRunner};

#[test]
fn full_mesh() {
    let config = SimulationConfig::default();
    let runner = SimulationRunner::new(config);
    let results = runner.run();

    assert!(results.commits_consistent);
    assert!(!results.committed_leaders.is_empty());
    assert!(!results.metrics.is_empty());
}

#[test]
fn one_down() {
    let config = SimulationConfig {
        topology: NetworkTopology::OneDown(0),
        duration_secs: 40,
        ..Default::default()
    };
    let runner = SimulationRunner::new(config);
    let results = runner.run();

    assert!(results.commits_consistent);
}

#[test]
fn config_yaml_round_trip() {
    let config = SimulationConfig {
        committee_size: 7,
        latency_min_ms: 10,
        latency_max_ms: 200,
        topology: NetworkTopology::Star(0),
        duration_secs: 30,
        rng_seed: 42,
        commit_period: 5,
        ..Default::default()
    };

    let yaml = serde_yaml::to_string(&config).unwrap();
    let restored: SimulationConfig = serde_yaml::from_str(&yaml).unwrap();

    assert_eq!(restored.committee_size, 7);
    assert_eq!(restored.latency_min_ms, 10);
    assert_eq!(restored.latency_max_ms, 200);
    assert_eq!(restored.duration_secs, 30);
    assert_eq!(restored.rng_seed, 42);
    assert_eq!(restored.commit_period, 5);
    assert!(matches!(restored.topology, NetworkTopology::Star(0)));
}

#[test]
fn from_yaml() {
    let config = SimulationConfig::default();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sim.yaml");

    config.print(&path).unwrap();

    let runner = SimulationRunner::from_yaml(&path).unwrap();
    assert_eq!(runner.config().committee_size, 10);
    assert_eq!(runner.config().duration_secs, 20);
}

#[test]
fn star_topology() {
    let config = SimulationConfig {
        topology: NetworkTopology::Star(0),
        duration_secs: 20,
        ..Default::default()
    };
    let runner = SimulationRunner::new(config);
    let results = runner.run();

    assert!(results.commits_consistent);
    assert!(!results.committed_leaders.is_empty());
}

#[test]
fn small_committee() {
    let config = SimulationConfig {
        committee_size: 4,
        duration_secs: 20,
        ..Default::default()
    };
    let runner = SimulationRunner::new(config);
    let results = runner.run();

    assert!(results.commits_consistent);
    assert!(!results.committed_leaders.is_empty());
}

#[test]
fn custom_node_parameters() {
    let config = SimulationConfig {
        node_parameters: NodeParameters {
            number_of_leaders: 1,
            wave_length: 4,
            ..Default::default()
        },
        duration_secs: 20,
        ..Default::default()
    };
    let runner = SimulationRunner::new(config);
    let results = runner.run();

    assert!(results.commits_consistent);
    assert!(!results.committed_leaders.is_empty());
}

#[test]
fn network_partition() {
    let config = SimulationConfig {
        topology: NetworkTopology::Partition(vec![vec![0, 1], vec![2, 3, 4, 5, 6, 7, 8, 9]]),
        duration_secs: 40,
        ..Default::default()
    };
    let runner = SimulationRunner::new(config);
    let results = runner.run();

    assert!(results.commits_consistent);
}
