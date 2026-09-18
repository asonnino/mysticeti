// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt, time::Duration};

use serde::{Deserialize, Deserializer, Serialize};

use crate::latency::LatencyModel;
use dag::config::ImportExport;
use replica::config::{LoadGeneratorConfig, ReplicaParameters};

/// Either a single simulation or a suite of simulations to run sequentially.
///
/// The untagged representation lets one YAML file be either a mapping (single
/// config, as before) or a top-level sequence of configs (suite).
#[derive(Serialize, Clone)]
#[serde(untagged)]
pub enum SimulationMode {
    Suite(Vec<SimulationConfig>),
    Single(Box<SimulationConfig>),
}

// Not derived: an untagged derive reports every error inside a config as "data did not match
// any variant", hiding the field at fault.
impl<'de> Deserialize<'de> for SimulationMode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_yaml::Value::deserialize(deserializer)?;
        let mode = if value.is_sequence() {
            serde_yaml::from_value(value).map(Self::Suite)
        } else {
            serde_yaml::from_value(value).map(|config| Self::Single(Box::new(config)))
        };
        mode.map_err(serde::de::Error::custom)
    }
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
    // `singleton_map` writes enum variants as `variant: value` rather than a YAML `!variant`
    // tag, which the untagged `SimulationMode` cannot read back.
    #[serde(default, with = "serde_yaml::with::singleton_map")]
    pub latency: LatencyModel,
    #[serde(default, with = "serde_yaml::with::singleton_map")]
    pub topology: NetworkTopology,
    #[serde(default = "defaults::duration_secs")]
    pub duration_secs: u64,
    #[serde(default)]
    pub rng_seed: u64,
    #[serde(default)]
    pub replica_parameters: ReplicaParameters,
    #[serde(default = "defaults::load_generator")]
    pub load_generator: Option<LoadGeneratorConfig>,
    /// Authority indices that send twin blocks in their leader rounds (see docs/simulator.md).
    #[serde(default)]
    pub equivocating_leaders: Vec<usize>,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            name: None,
            committee_size: defaults::committee_size(),
            latency: LatencyModel::default(),
            topology: NetworkTopology::default(),
            duration_secs: defaults::duration_secs(),
            rng_seed: 0,
            replica_parameters: ReplicaParameters::default(),
            load_generator: Some(LoadGeneratorConfig::new_for_test()),
            equivocating_leaders: Vec::new(),
        }
    }
}

impl SimulationConfig {
    pub fn duration(&self) -> Duration {
        Duration::from_secs(self.duration_secs)
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
    pub fn duration_secs() -> u64 {
        20
    }
    pub fn load_generator() -> Option<LoadGeneratorConfig> {
        Some(LoadGeneratorConfig::new_for_test())
    }
}
