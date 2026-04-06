// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::config::{NetworkTopology, SimulationConfig};
use crate::runner::SimulationRunner;

#[test]
fn test_runner_full_mesh() {
    let config = SimulationConfig::default();
    let runner = SimulationRunner::new(config);
    let results = runner.run();

    assert!(results.commits_consistent);
    assert!(!results.committed_leaders.is_empty());
    assert!(!results.metrics.is_empty());
}

#[test]
fn test_runner_one_down() {
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
fn test_config_yaml_round_trip() {
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
fn test_runner_from_yaml() {
    let config = SimulationConfig::default();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sim.yaml");

    use dag::config::ImportExport;
    config.print(&path).unwrap();

    let runner = SimulationRunner::from_yaml(&path).unwrap();
    assert_eq!(runner.config().committee_size, 10);
    assert_eq!(runner.config().duration_secs, 20);
}
