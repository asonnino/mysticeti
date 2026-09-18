// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{num::NonZeroUsize, ops::Range, path::PathBuf};

use consensus::protocol::ConsensusProtocol;
use dag::authority::Authority;
use dag::config::ImportExport;
use dag::metrics::{BlockKind, MetricsSnapshot};
use dag::test_util::committee;
use indoc::indoc;
use replica::config::ReplicaParameters;
use replica::result::Outcome;
use simulator::{
    Geography, LatencyModel, NetworkTopology, SimulatedNetwork, SimulationConfig, SimulationMode,
    SimulationRunner, UniformLatency,
};

#[test]
fn full_mesh() {
    let config = SimulationConfig::default();
    let runner = SimulationRunner::new(config);
    let results = runner.run().unwrap();

    assert_ne!(results.outcome, Outcome::Diverged);
    assert!(!results.metrics.is_empty());
    // The default protocol (Mysticeti) has no fast path: every direct commit is slow.
    let slow_commits = results.metrics.iter().map(|m| m.slow_commits()).max();
    let fast_commits = results.metrics.iter().map(|m| m.fast_commits()).max();
    assert!(slow_commits.unwrap_or(0) > 0);
    assert_eq!(fast_commits, Some(0));
    // Every committed leader is observed once on the block latency histogram.
    for metrics in &results.metrics {
        let (sum, count) = metrics
            .block_latency_sum_and_count(BlockKind::Leader)
            .unwrap();
        assert_eq!(count, metrics.total_committed_leaders());
        assert!(sum > 0.0);
    }
}

#[test]
fn one_down() {
    let config = SimulationConfig {
        topology: NetworkTopology::OneDown(0),
        duration_secs: 40,
        ..Default::default()
    };
    let runner = SimulationRunner::new(config);
    let results = runner.run().unwrap();

    assert_ne!(results.outcome, Outcome::Diverged);
}

#[test]
fn config_yaml_round_trip() {
    let config = SimulationConfig {
        committee_size: 7,
        latency: LatencyModel::Uniform(UniformLatency { range_ms: 10..200 }),
        topology: NetworkTopology::Star(0),
        duration_secs: 30,
        rng_seed: 42,
        equivocating_leaders: vec![2, 5],
        ..Default::default()
    };

    let yaml = serde_yaml::to_string(&config).unwrap();
    let restored: SimulationConfig = serde_yaml::from_str(&yaml).unwrap();

    assert_eq!(restored.committee_size, 7);
    assert_eq!(restored.latency, config.latency);
    assert_eq!(restored.duration_secs, 30);
    assert_eq!(restored.rng_seed, 42);
    assert!(matches!(restored.topology, NetworkTopology::Star(0)));
    assert_eq!(restored.equivocating_leaders, vec![2, 5]);
}

#[test]
fn inverted_latency_range_is_rejected() {
    let range_ms = Range {
        start: 200,
        end: 100,
    };
    let config = SimulationConfig {
        latency: LatencyModel::Uniform(UniformLatency { range_ms }),
        ..Default::default()
    };
    let error = SimulationRunner::new(config).run().err().unwrap();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
#[should_panic(expected = "invalid latency model")]
fn network_rejects_an_invalid_latency_model() {
    let range_ms = Range {
        start: 200,
        end: 100,
    };
    let latency = LatencyModel::Uniform(UniformLatency { range_ms });
    SimulatedNetwork::new(&committee(4), latency);
}

#[test]
fn geography_yaml_round_trip() {
    let config = SimulationConfig {
        latency: LatencyModel::Geographic(Geography::new_for_test()),
        ..Default::default()
    };

    let yaml = serde_yaml::to_string(&config).unwrap();
    let restored: SimulationConfig = serde_yaml::from_str(&yaml).unwrap();
    let from_mode = serde_yaml::from_str::<SimulationMode>(&yaml)
        .unwrap()
        .into_configs();

    assert_eq!(restored.latency, config.latency);
    assert_eq!(from_mode[0].latency, config.latency);
}

#[test]
fn latency_parses_from_yaml() {
    let yaml = indoc! {"
        latency:
            geographic:
                regions: [near, far]
                rtt_ms:
                    near: {far: 200}
    "};
    let config: SimulationConfig = serde_yaml::from_str(yaml).unwrap();
    let LatencyModel::Geographic(geography) = config.latency else {
        panic!("expected a geographic latency model");
    };
    assert_eq!(geography.round_trip_ms("far", "near"), Some(200.0));
    assert_eq!(geography.extra_ms, 0.0..1.0);

    let config: SimulationConfig = serde_yaml::from_str("committee_size: 4").unwrap();
    assert_eq!(config.latency, LatencyModel::default());
}

#[test]
fn geography_example_is_valid() {
    // Loaded and validated only: fifty replicas are too slow to simulate in a debug build.
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/geography.yaml");
    let config = SimulationMode::load(&path)
        .unwrap()
        .into_configs()
        .remove(0);

    config.latency.validate().unwrap();
    let LatencyModel::Geographic(geography) = config.latency else {
        panic!("expected a geographic latency model");
    };
    let tokyo = (0..config.committee_size)
        .filter(|authority| geography.region_of(*authority) == Some("ap-northeast-1"))
        .count();
    assert_eq!(tokyo, 8);
    assert_eq!(
        geography.round_trip_ms("ap-northeast-1", "eu-central-1"),
        Some(238.3)
    );
}

#[test]
fn every_example_is_valid() {
    let examples = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples");
    for entry in std::fs::read_dir(examples).unwrap() {
        let path = entry.unwrap().path();
        let configs = SimulationMode::load(&path).unwrap().into_configs();
        assert!(!configs.is_empty(), "{} is empty", path.display());
        for config in configs {
            config.latency.validate().unwrap();
        }
    }
}

#[test]
fn invalid_geography_is_rejected() {
    let mut geography = Geography::new_for_test();
    geography.regions.push("unknown".to_string());
    let config = SimulationConfig {
        latency: LatencyModel::Geographic(geography),
        ..Default::default()
    };
    let error = SimulationRunner::new(config).run().err().unwrap();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn far_region_observes_commits_later() {
    // Replicas 4 and 9 sit 100 ms (one way) from the other eight, which hold a quorum
    // (7 of 10) among themselves: the far replicas learn of every commit that much later.
    let config = SimulationConfig {
        latency: LatencyModel::Geographic(Geography::new_for_test()),
        ..Default::default()
    };
    let results = SimulationRunner::new(config).run().unwrap();

    assert_eq!(results.outcome, Outcome::Pass);
    let mean_leader_latency_ms = |authority: usize| {
        let (sum, count) = results.metrics[authority]
            .block_latency_sum_and_count(BlockKind::Leader)
            .unwrap();
        1000.0 * sum / count as f64
    };
    let slowest_near = [0, 1, 2, 3, 5, 6, 7, 8]
        .map(mean_leader_latency_ms)
        .into_iter()
        .fold(0.0, f64::max);
    for far in [4, 9] {
        assert!(mean_leader_latency_ms(far) > slowest_near + 50.0);
    }
}

#[test]
fn geographic_run_has_no_spurious_timeouts() {
    // The testbed matrix at n = 10, where Tokyo's links are slower than a round. If a link
    // queued its messages, the validators furthest from Tokyo would fall rounds behind after
    // every Tokyo-led round and never propose their own leader round, which everyone else then
    // waits out: one leader timeout and one direct skip per ten rounds.
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/geography.yaml");
    let mut config = SimulationMode::load(&path)
        .unwrap()
        .into_configs()
        .remove(0);
    config.committee_size = 10;
    config.replica_parameters.consensus = ConsensusProtocol::Mysticeti {
        leader_count: NonZeroUsize::new(2).unwrap(),
    };
    let results = SimulationRunner::new(config).run().unwrap();

    assert_eq!(results.outcome, Outcome::Pass);
    for metrics in &results.metrics {
        assert!(metrics.total_committed_leaders() > 300);
        // One timeout at startup, before the first blocks arrive.
        assert!(metrics.leader_timeouts() <= 1);
        assert_eq!(metrics.direct_skips(), 0);
    }
}

#[test]
fn equivocating_leader() {
    // Mysticeti (n=10, no fast path) with authority 3 sending twin blocks in its
    // leader rounds. The twins split the votes, so no certificate forms and the
    // slot cannot be committed; every voter voted for one twin, so there are no
    // blames and no direct skip either. Every equivocated slot is therefore an
    // indirect skip (observed: 80 of 80), while the other slots slow-commit.
    let result = SimulationRunner::new(SimulationConfig {
        equivocating_leaders: vec![3],
        duration_secs: 40,
        ..Default::default()
    })
    .run()
    .unwrap();

    assert_eq!(result.outcome, Outcome::Pass);
    let equivocator = Authority::from(3usize);
    let decided = result
        .metrics
        .iter()
        .map(|metrics| metrics.decided_leaders_of(equivocator))
        .max();
    assert!(decided.unwrap_or(0) > 0);
    let max_over_replicas = |accessor: fn(&MetricsSnapshot) -> u64| {
        result.metrics.iter().map(accessor).max().unwrap_or(0)
    };
    assert!(max_over_replicas(MetricsSnapshot::indirect_skips) > 0);
    assert_eq!(max_over_replicas(MetricsSnapshot::direct_skips), 0);
    assert_eq!(max_over_replicas(MetricsSnapshot::fast_commits), 0);
    assert!(max_over_replicas(MetricsSnapshot::slow_commits) > 0);
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
    let results = runner.run().unwrap();

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
    let results = runner.run().unwrap();

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
    let results = runner.run().unwrap();

    assert_ne!(results.outcome, Outcome::Diverged);
}

#[test]
fn from_example_config() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/single.yaml");
    let runner = SimulationRunner::from_yaml(&path).unwrap();
    let results = runner.run().unwrap();

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
    let results = runner.run().unwrap();

    assert_ne!(results.outcome, Outcome::Diverged);
}
