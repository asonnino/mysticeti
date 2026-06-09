// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    benchmark::Parameters,
    error::TestbedResult,
    monitor::Monitor,
    protocol::{ProtocolCommands, ProtocolMetrics},
    report::MonitoringReport,
};

use super::Orchestrator;

impl<P: ProtocolCommands + ProtocolMetrics> Orchestrator<P> {
    /// Reload prometheus and grafana. Returns `Some(report)` when monitoring is
    /// enabled (the testbed has a dedicated monitor instance) and `None`
    /// otherwise.
    pub async fn start_monitoring(
        &self,
        parameters: &Parameters<P>,
    ) -> TestbedResult<Option<MonitoringReport>> {
        let (clients, nodes, instance) = self.select_instances(parameters)?;
        let Some(instance) = instance else {
            return Ok(None);
        };

        let monitor = Monitor::new(instance, clients, nodes, self.ssh_manager.clone());
        let commands = &self.protocol_commands;
        monitor.start_prometheus(commands, parameters).await?;
        monitor.start_grafana().await?;

        Ok(Some(MonitoringReport {
            grafana_address: monitor.grafana_address(),
            prometheus_address: monitor.prometheus_address(),
        }))
    }
}
