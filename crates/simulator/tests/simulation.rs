// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{num::NonZeroUsize, path::PathBuf};

use consensus::protocol::ConsensusProtocol;
use dag::{config::ImportExport, metrics::Outcome};
use indoc::indoc;
use replica::config::ReplicaParameters;
use simulator::{NetworkTopology, SimulationConfig, SimulationMode, SimulationRunner};

#[test]
fn full_mesh() {
    let config = SimulationConfig::default();
    let runner = SimulationRunner::new(config);
    let results = runner.run();

    assert_ne!(results.outcome, Outcome::Diverged);
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

    assert_ne!(results.outcome, Outcome::Diverged);
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
        ..Default::default()
    };

    let yaml = serde_yaml::to_string(&config).unwrap();
    let restored: SimulationConfig = serde_yaml::from_str(&yaml).unwrap();

    assert_eq!(restored.committee_size, 7);
    assert_eq!(restored.latency_min_ms, 10);
    assert_eq!(restored.latency_max_ms, 200);
    assert_eq!(restored.duration_secs, 30);
    assert_eq!(restored.rng_seed, 42);
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

    assert_ne!(results.outcome, Outcome::Diverged);
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

    assert_ne!(results.outcome, Outcome::Diverged);
}

#[test]
fn custom_node_parameters() {
    let config = SimulationConfig {
        replica_parameters: ReplicaParameters {
            consensus: ConsensusProtocol::MahiMahi {
                leader_count: NonZeroUsize::new(1).unwrap(),
                wave_length: 4,
            },
            ..Default::default()
        },
        duration_secs: 20,
        ..Default::default()
    };
    let runner = SimulationRunner::new(config);
    let results = runner.run();

    assert_ne!(results.outcome, Outcome::Diverged);
}

#[test]
fn from_example_config() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/single.yaml");
    let runner = SimulationRunner::from_yaml(&path).unwrap();
    let results = runner.run();

    assert_ne!(results.outcome, Outcome::Diverged);
}

#[test]
fn mode_parses_single_mapping() {
    let yaml = "committee_size: 7\nduration_secs: 30\n";
    let configs = serde_yaml::from_str::<SimulationMode>(yaml)
        .unwrap()
        .into_configs();
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].committee_size, 7);
    assert_eq!(configs[0].duration_secs, 30);
    assert!(configs[0].name.is_none());
}

#[test]
fn mode_parses_suite_sequence() {
    let yaml = indoc! {"
        -   name: baseline
            committee_size: 4
            duration_secs: 20
        -   name: one-down
            topology:
                oneDown: 0
            duration_secs: 40
    "};
    let configs = serde_yaml::from_str::<SimulationMode>(yaml)
        .unwrap()
        .into_configs();
    assert_eq!(configs.len(), 2);
    assert_eq!(configs[0].name.as_deref(), Some("baseline"));
    assert_eq!(configs[0].committee_size, 4);
    assert_eq!(configs[1].name.as_deref(), Some("one-down"));
    assert!(matches!(configs[1].topology, NetworkTopology::OneDown(0)));
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

    assert_ne!(results.outcome, Outcome::Diverged);
}
