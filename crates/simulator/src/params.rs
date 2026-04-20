// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// NOTE: mirrored from `replica::params::ReplicaParameters`. Unified
// in the next crate-split refactor (replica → lib + cli), which will
// let `simulator` depend on `replica` without a cycle. Keep the two
// definitions in lock-step until then.

use consensus::protocol::ConsensusProtocol;
use dag::config::{DagParameters, ImportExport};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct ReplicaParameters {
    #[serde(default)]
    pub dag: DagParameters,
    #[serde(default)]
    pub consensus: ConsensusProtocol,
}

impl ImportExport for ReplicaParameters {}
