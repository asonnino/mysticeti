// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod monitoring;
mod prepare;
mod run;

pub use monitoring::MonitoringReport;
pub use prepare::ConfigureReport;

use std::collections::{HashMap, VecDeque};

use crate::{
    benchmark::Parameters,
    ensure,
    error::{TestbedError, TestbedResult},
    protocol::{ProtocolCommands, ProtocolMetrics},
    provider::Instance,
    settings::Settings,
    ssh::SshConnectionManager,
};

/// An orchestrator to deploy nodes and run benchmarks on a testbed.
pub struct Orchestrator<P> {
    /// The testbed's settings.
    pub(super) settings: Settings,
    /// The state of the testbed (reflecting accurately the state of the machines).
    pub(super) instances: Vec<Instance>,
    /// Provider-specific commands to install on the instance.
    pub(super) instance_setup_commands: Vec<String>,
    /// Protocol-specific commands generator to generate the protocol configuration files,
    /// boot clients and nodes, etc.
    pub(super) protocol_commands: P,
    /// Handle ssh connections to instances.
    pub(super) ssh_manager: SshConnectionManager,
}

impl<P> Orchestrator<P> {
    /// Make a new orchestrator.
    pub fn new(
        settings: Settings,
        instances: Vec<Instance>,
        instance_setup_commands: Vec<String>,
        protocol_commands: P,
        ssh_manager: SshConnectionManager,
    ) -> Self {
        Self {
            settings,
            instances,
            instance_setup_commands,
            protocol_commands,
            ssh_manager,
        }
    }

    /// Borrow the protocol implementation the orchestrator is driving. Callers
    /// that need protocol-specific data (e.g. metric names for the PromQL
    /// collector) should ask the orchestrator rather than re-instantiating the
    /// protocol from `Settings`, so a non-default `P` is honored.
    pub fn protocol(&self) -> &P {
        &self.protocol_commands
    }
}

impl<P: ProtocolCommands + ProtocolMetrics> Orchestrator<P> {
    /// Returns the instances of the testbed on which to run the benchmarks.
    ///
    /// Returns `(clients, nodes, monitoring)`. Clients and nodes are selected
    /// round-robin across regions; the monitoring instance is taken from the
    /// first region when monitoring is enabled.
    pub fn select_instances(
        &self,
        parameters: &Parameters<P>,
    ) -> TestbedResult<(Vec<Instance>, Vec<Instance>, Option<Instance>)> {
        // Ensure there are enough active instances.
        let available_instances: Vec<_> = self.instances.iter().filter(|x| x.is_active()).collect();
        let minimum_instances = parameters.nodes
            + self.settings.dedicated_clients
            + if self.settings.monitoring { 1 } else { 0 };
        ensure!(
            available_instances.len() >= minimum_instances,
            TestbedError::InsufficientCapacity(minimum_instances - available_instances.len())
        );

        // Sort the instances by region. This step ensures that the instances are selected as
        // equally as possible from all regions.
        let mut instances_by_regions = HashMap::new();
        for instance in available_instances {
            instances_by_regions
                .entry(&instance.region)
                .or_insert_with(VecDeque::new)
                .push_back(instance);
        }

        // Select the instance to host the monitoring stack. If monitoring is
        // enabled but the first region has no active instance to spare, that's
        // a capacity error — silently dropping monitoring would surprise the
        // caller (who already saw the action banner and expects a Grafana URL).
        let mut monitoring_instance = None;
        if self.settings.monitoring {
            let region = &self.settings.regions[0];
            monitoring_instance = Some(
                instances_by_regions
                    .get_mut(region)
                    .and_then(|instances| instances.pop_front())
                    .ok_or(TestbedError::InsufficientCapacity(1))?
                    .clone(),
            );
        }

        // Select the instances to host exclusively load generators.
        let mut client_instances = Vec::new();
        for region in self.settings.regions.iter().cycle() {
            if client_instances.len() == self.settings.dedicated_clients {
                break;
            }
            if let Some(regional_instances) = instances_by_regions.get_mut(region)
                && let Some(instance) = regional_instances.pop_front()
            {
                client_instances.push(instance.clone());
            }
        }

        // Select the instances to host the nodes.
        let mut nodes_instances = Vec::new();
        for region in self.settings.regions.iter().cycle() {
            if nodes_instances.len() == parameters.nodes {
                break;
            }
            if let Some(regional_instances) = instances_by_regions.get_mut(region)
                && let Some(instance) = regional_instances.pop_front()
            {
                nodes_instances.push(instance.clone());
            }
        }

        // Spawn a load generator collocated with each node if there are no instances dedicated
        // to exclusively run load generators.
        if client_instances.is_empty() {
            client_instances.clone_from(&nodes_instances);
        }

        Ok((client_instances, nodes_instances, monitoring_instance))
    }
}
