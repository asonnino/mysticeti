// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{ops::Range, time::Duration};

use serde::{Deserialize, Serialize};

use dag::config::{ImportExport, NodeParameters};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SimulationConfig {
    #[serde(default = "defaults::committee_size")]
    pub committee_size: usize,

    #[serde(default = "defaults::latency_min_ms")]
    pub latency_min_ms: u64,

    #[serde(default = "defaults::latency_max_ms")]
    pub latency_max_ms: u64,

    #[serde(default)]
    pub topology: NetworkTopology,

    #[serde(default = "defaults::duration_secs")]
    pub duration_secs: u64,

    #[serde(default)]
    pub rng_seed: u64,

    #[serde(default = "defaults::commit_period")]
    pub commit_period: u64,

    #[serde(default)]
    pub node_parameters: NodeParameters,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            committee_size: defaults::committee_size(),
            latency_min_ms: defaults::latency_min_ms(),
            latency_max_ms: defaults::latency_max_ms(),
            topology: NetworkTopology::default(),
            duration_secs: defaults::duration_secs(),
            rng_seed: 0,
            commit_period: defaults::commit_period(),
            node_parameters: NodeParameters::default(),
        }
    }
}

impl SimulationConfig {
    pub fn latency_range(&self) -> Range<Duration> {
        let min = Duration::from_millis(self.latency_min_ms);
        let max = Duration::from_millis(self.latency_max_ms);
        min..max
    }
}

impl ImportExport for SimulationConfig {}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub enum NetworkTopology {
    #[default]
    FullMesh,
    OneDown(usize),
    Partition(Vec<Vec<usize>>),
    Star(usize),
}

mod defaults {
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
    pub fn commit_period() -> u64 {
        3
    }
}
