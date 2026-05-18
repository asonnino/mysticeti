// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{Debug, Display};

use serde::{Deserialize, Serialize};

use crate::{
    protocol::{Protocol, ProtocolParameters},
    settings::Settings,
};

/// Shorthand for the benchmark-parameters type carried by `Orchestrator<P>` and
/// the protocol's `Monitor` helpers. Lifts noise out of method signatures
/// without changing the underlying type.
pub type Parameters<P> =
    BenchmarkParameters<<P as Protocol>::NodeParameters, <P as Protocol>::ClientParameters>;

/// The benchmark parameters for a run. These parameters are stored along with the performance data
/// and should be used to reproduce the results.
#[derive(Serialize, Deserialize, Clone)]
pub struct BenchmarkParameters<N, C> {
    /// The testbed settings.
    pub settings: Settings,
    /// The node's configuration parameters.
    pub node_parameters: N,
    /// The client's configuration parameters.
    pub client_parameters: C,
    /// The committee size.
    pub nodes: usize,
    /// The total load (tx/s) to submit to the system.
    pub load: usize,
}

impl<N: Debug, C: Debug> Debug for BenchmarkParameters<N, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}-{:?}-{:?}-{}-{}",
            self.node_parameters,
            self.client_parameters,
            self.settings.faults,
            self.nodes,
            self.load
        )
    }
}

impl<N, C> Display for BenchmarkParameters<N, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} nodes ({}) - {} tx/s",
            self.nodes, self.settings.faults, self.load
        )
    }
}

impl<N: ProtocolParameters, C: ProtocolParameters> BenchmarkParameters<N, C> {
    /// Make a new benchmark parameters.
    pub fn new_from_loads(
        settings: Settings,
        node_parameters: N,
        client_parameters: C,
        nodes: usize,
        loads: Vec<usize>,
    ) -> Vec<Self> {
        loads
            .into_iter()
            .map(|load| Self {
                settings: settings.clone(),
                node_parameters: node_parameters.clone(),
                client_parameters: client_parameters.clone(),
                nodes,
                load,
            })
            .collect()
    }
}
