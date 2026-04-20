// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use consensus::protocol::ConsensusProtocol;
use dag::config::{DagParameters, ImportExport};
use serde::{Deserialize, Serialize};

/// Tunable knobs for a replica: dag-level + consensus variant.
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct ReplicaParameters {
    #[serde(default)]
    pub dag: DagParameters,
    #[serde(default)]
    pub consensus: ConsensusProtocol,
}

impl ReplicaParameters {
    pub const DEFAULT_FILENAME: &'static str = "replica-parameters.yaml";
}

impl ImportExport for ReplicaParameters {}
