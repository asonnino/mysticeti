// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt, ops::Range, time::Duration};

use serde::{Deserialize, Serialize};

use dag::config::ImportExport;
use replica::config::{LoadGeneratorConfig, ReplicaParameters};

/// Either a single simulation or a suite of simulations to run sequentially.
///
/// The untagged representation lets one YAML file be either a mapping (single
/// config, as before) or a top-level sequence of configs (suite).
#[derive(Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum SimulationMode {
    Suite(Vec<SimulationConfig>),
    Single(Box<SimulationConfig>),
}

impl SimulationMode {
    pub fn into_configs(self) -> Vec<SimulationConfig> {
        match self {
            SimulationMode::Single(config) => vec![*config],
            SimulationMode::Suite(configs) => configs,
        }
    }
}

impl ImportExport for SimulationMode {}

#[derive(Serialize, Deserialize, Clone)]
pub struct SimulationConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default = "defaults::committee_size")]
    pub committee_size: usize,
    #[serde(default = "defaults::latency_min_ms")]
    pub latency_min_ms: u64,
    #[serde(default = "defaults::latency_max_ms")]
    pub latency_max_ms: u64,
    /// Per-message jitter on top of a per-link base latency drawn once from
    /// the latency range; `None` draws every message independently instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link_jitter_ms: Option<u64>,
    #[serde(default)]
    pub topology: NetworkTopology,
    #[serde(default = "defaults::duration_secs")]
    pub duration_secs: u64,
    #[serde(default)]
    pub rng_seed: u64,
    #[serde(default)]
    pub replica_parameters: ReplicaParameters,
    #[serde(default = "defaults::load_generator")]
    pub load_generator: Option<LoadGeneratorConfig>,
    /// Timed network-condition schedule; empty means healthy throughout.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<ConditionPhase>,
    /// Replicas to crash mid-run (their final state still appears in results).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub crashes: Vec<CrashSpec>,
    /// Sample per-replica counters every this many simulated seconds into the
    /// result's time series; `None` disables sampling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_interval_secs: Option<u64>,
}

/// Crash the given replica at the given simulated time.
#[derive(Serialize, Deserialize, Clone, Copy)]
pub struct CrashSpec {
    pub replica: usize,
    pub at_secs: u64,
}

/// One phase of the network-condition schedule: the delay model in force from
/// `from_secs` until the next phase begins; no `model` means healthy.
#[derive(Serialize, Deserialize, Clone)]
pub struct ConditionPhase {
    pub from_secs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<DelayModel>,
}

/// A network-condition model, adversarial or stochastic. Models add delay on
/// top of the link latency and never drop messages (eventual delivery).
#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum DelayModel {
    /// Delay every message from and to the current known leaders (the
    /// round-robin cohort of the adversary's tracked round). Blind to the
    /// fake coin: async slots remain untargetable.
    TargetedLeaderDelay { delay_ms: u64 },
    /// Adversarial scheduler: hold every message until the next burst
    /// boundary, releasing traffic in bursts of maximal reordering.
    ScheduledAsynchrony { burst_ms: u64 },
    /// Stochastic model: each message independently suffers an extra delay
    /// from the range with the given probability (in percent).
    RandomLinkDelay {
        percent: u8,
        delay_min_ms: u64,
        delay_max_ms: u64,
    },
    /// Jitter model: each message suffers `base_ms` plus an exponentially
    /// distributed delay of mean `mean_ms` (a heavy right tail), truncated at
    /// `cap_ms`. Applies to `percent` of messages; most are small, a few are
    /// very large, as on an unstable network.
    ExponentialJitter {
        percent: u8,
        base_ms: u64,
        mean_ms: u64,
        cap_ms: u64,
    },
}

impl DelayModel {
    /// Panic on parameters the model cannot run with.
    fn validate(&self) {
        match self {
            Self::TargetedLeaderDelay { .. } => {}
            Self::ScheduledAsynchrony { burst_ms } => {
                assert!(*burst_ms > 0, "burst_ms must be positive");
            }
            Self::RandomLinkDelay {
                percent,
                delay_min_ms,
                delay_max_ms,
            } => {
                assert!(*percent <= 100, "percent ({percent}) must be at most 100");
                assert!(
                    delay_min_ms <= delay_max_ms,
                    "delay_min_ms ({delay_min_ms}) must not exceed delay_max_ms ({delay_max_ms})"
                );
            }
            Self::ExponentialJitter {
                percent,
                base_ms,
                mean_ms,
                cap_ms,
            } => {
                assert!(*percent <= 100, "percent ({percent}) must be at most 100");
                assert!(*mean_ms > 0, "mean_ms must be positive");
                assert!(
                    base_ms <= cap_ms,
                    "base_ms ({base_ms}) must not exceed cap_ms ({cap_ms})"
                );
            }
        }
    }
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            name: None,
            committee_size: defaults::committee_size(),
            latency_min_ms: defaults::latency_min_ms(),
            latency_max_ms: defaults::latency_max_ms(),
            link_jitter_ms: None,
            topology: NetworkTopology::default(),
            duration_secs: defaults::duration_secs(),
            rng_seed: 0,
            replica_parameters: ReplicaParameters::default(),
            load_generator: Some(LoadGeneratorConfig::new_for_test()),
            conditions: Vec::new(),
            crashes: Vec::new(),
            sample_interval_secs: None,
        }
    }
}

impl SimulationConfig {
    pub fn latency_range(&self) -> Range<Duration> {
        assert!(
            self.latency_min_ms <= self.latency_max_ms,
            "latency_min_ms ({}) must not exceed latency_max_ms ({})",
            self.latency_min_ms,
            self.latency_max_ms
        );
        let min = Duration::from_millis(self.latency_min_ms);
        let max = Duration::from_millis(self.latency_max_ms);
        min..max
    }

    pub fn duration(&self) -> Duration {
        Duration::from_secs(self.duration_secs)
    }

    pub fn link_jitter(&self) -> Option<Duration> {
        self.link_jitter_ms.map(Duration::from_millis)
    }

    /// The condition schedule as `(start, model)` pairs sorted by start time.
    pub fn condition_phases(&self) -> Vec<(Duration, Option<DelayModel>)> {
        let mut phases: Vec<_> = self
            .conditions
            .iter()
            .map(|phase| {
                if let Some(model) = &phase.model {
                    model.validate();
                }
                (Duration::from_secs(phase.from_secs), phase.model.clone())
            })
            .collect();
        phases.sort_by_key(|(start, _)| *start);
        phases
    }
}

impl ImportExport for SimulationConfig {}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub enum NetworkTopology {
    #[default]
    FullMesh,
    OneDown(usize),
    Partition(Vec<Vec<usize>>),
    Star(usize),
}

impl fmt::Display for NetworkTopology {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FullMesh => f.write_str("full mesh"),
            Self::OneDown(index) => write!(f, "one down ({index})"),
            Self::Star(center) => write!(f, "star (center={center})"),
            Self::Partition(groups) => {
                f.write_str("partition (")?;
                for (i, group) in groups.iter().enumerate() {
                    if i > 0 {
                        f.write_str(",")?;
                    }
                    f.write_str("[")?;
                    for (j, index) in group.iter().enumerate() {
                        if j > 0 {
                            f.write_str(",")?;
                        }
                        write!(f, "{index}")?;
                    }
                    f.write_str("]")?;
                }
                f.write_str(")")
            }
        }
    }
}

mod defaults {
    use replica::config::LoadGeneratorConfig;

    pub fn committee_size() -> usize {
        10
    }
    pub fn latency_min_ms() -> u64 {
        50
    }
    pub fn latency_max_ms() -> u64 {
        100
    }
    pub fn duration_secs() -> u64 {
        20
    }
    pub fn load_generator() -> Option<LoadGeneratorConfig> {
        Some(LoadGeneratorConfig::new_for_test())
    }
}
