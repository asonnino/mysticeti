// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{num::NonZeroUsize, path::PathBuf};

use consensus::protocol::ConsensusProtocol;
use dag::config::ImportExport;
use indoc::indoc;
use replica::config::ReplicaParameters;
use replica::result::Outcome;
use simulator::{NetworkTopology, SimulationConfig, SimulationMode, SimulationRunner};

#[test]
fn full_mesh() {
    let config = SimulationConfig::default();
    let runner = SimulationRunner::new(config);
    let results = runner.run().unwrap();

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
    let results = runner.run().unwrap();

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

/// The per-link model (stable symmetric base + jitter) commits consistently.
#[test]
fn per_link_latency_smoke() {
    let config = SimulationConfig {
        latency_min_ms: 50,
        latency_max_ms: 200,
        link_jitter_ms: Some(10),
        duration_secs: 10,
        ..Default::default()
    };
    let results = SimulationRunner::new(config).run().unwrap();

    assert_eq!(results.outcome, Outcome::Pass);
}

/// Identical seeds and configs reproduce identical per-link runs.
#[test]
fn per_link_latency_deterministic() {
    let config = || SimulationConfig {
        committee_size: 4,
        latency_min_ms: 50,
        latency_max_ms: 200,
        link_jitter_ms: Some(10),
        duration_secs: 10,
        rng_seed: 7,
        ..Default::default()
    };
    let first = SimulationRunner::new(config()).run().unwrap();
    let second = SimulationRunner::new(config()).run().unwrap();

    assert_ne!(first.outcome, Outcome::Diverged);
    assert_eq!(first.outcome, second.outcome);
    let committed_leaders = |results: &[dag::metrics::MetricsSnapshot]| {
        results
            .iter()
            .map(|snapshot| snapshot.total_committed_leaders())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        committed_leaders(&first.metrics),
        committed_leaders(&second.metrics)
    );
}

/// Targeted delay of the known leaders stalls the sync rule to timeout pace,
/// leaves the async rule (coin leaders, off the targeted cohort) unaffected,
/// and lets Steelhead keep committing through its async slots: the paper's
/// attack claim, pinned as a strict throughput ordering with wide margins.
#[test]
fn targeted_leader_delay_ordering() {
    let attacked = |consensus: &str| {
        let mut config = SimulationConfig {
            committee_size: 10,
            duration_secs: 20,
            conditions: serde_yaml::from_str(
                "[{ from_secs: 0, model: { kind: targeted-leader-delay, delay_ms: 2000 } }]",
            )
            .unwrap(),
            ..Default::default()
        };
        config.replica_parameters = ReplicaParameters {
            consensus: serde_yaml::from_str(consensus).unwrap(),
            ..Default::default()
        };
        let results = SimulationRunner::new(config).run().unwrap();
        assert_ne!(results.outcome, Outcome::Diverged);
        results
            .metrics
            .iter()
            .map(|snapshot| snapshot.total_committed_leaders())
            .max()
            .unwrap()
    };

    let mysticeti = attacked("{ protocol: mysticeti, leader_count: 2 }");
    let steelhead = attacked(indoc! {"
        protocol: steelhead
        pair: mysticeti-mahi-mahi
        period: 4
        async_wave_length: 5
        leader_count: 2
    "});
    let mahi_mahi = attacked("{ protocol: mahi-mahi, wave_length: 5, leader_count: 2 }");

    assert!(
        mysticeti * 3 < steelhead,
        "sync rule must stall under attack: mysticeti={mysticeti} steelhead={steelhead}"
    );
    assert!(
        steelhead < mahi_mahi,
        "async slots pay for stalled sync slots: steelhead={steelhead} mahi_mahi={mahi_mahi}"
    );
}

/// A replica crashed mid-run must not stop the committee: commits continue at
/// n-1 (the crashed authority's slots skip via blames), the run stays
/// consistent, and the crashed replica's final state still appears in results.
#[test]
fn crash_fault_mid_run() {
    let config = SimulationConfig {
        duration_secs: 30,
        crashes: serde_yaml::from_str("[{ replica: 3, at_secs: 10 }]").unwrap(),
        ..Default::default()
    };
    let committee_size = config.committee_size;
    let results = SimulationRunner::new(config).run().unwrap();

    assert_eq!(results.outcome, Outcome::Pass);
    assert_eq!(results.metrics.len(), committee_size);
    let survivor_commits = results
        .metrics
        .iter()
        .map(|snapshot| snapshot.total_committed_leaders())
        .max()
        .unwrap();
    assert!(
        survivor_commits > 300,
        "commits must continue after the crash: {survivor_commits}"
    );
}

/// The sampler emits one row per replica per tick, with cumulative counters.
#[test]
fn time_series_sampling() {
    let config = SimulationConfig {
        duration_secs: 10,
        sample_interval_secs: Some(2),
        ..Default::default()
    };
    let committee_size = config.committee_size;
    let results = SimulationRunner::new(config).run().unwrap();

    assert_ne!(results.outcome, Outcome::Diverged);
    let rows = &results.time_series;
    assert!(
        rows.len() >= 4 * committee_size && rows.len().is_multiple_of(committee_size),
        "expected full sampling ticks, got {} rows",
        rows.len()
    );
    let replica_zero_commits: Vec<_> = rows
        .iter()
        .filter(|row| row.replica == 0)
        .map(|row| row.direct_commits + row.indirect_commits)
        .collect();
    assert!(
        replica_zero_commits
            .windows(2)
            .all(|pair| pair[0] <= pair[1]),
        "cumulative counters must be non-decreasing: {replica_zero_commits:?}"
    );
    assert!(*replica_zero_commits.last().unwrap() > 0);
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
