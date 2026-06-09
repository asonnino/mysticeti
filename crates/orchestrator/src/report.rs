// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, time::Duration};

use crate::{collector::LiveStats, faults::CrashRecoveryAction, provider::Instance};

/// Per-instance addresses produced by [`crate::orchestrator::Orchestrator::configure`].
/// The caller uses this to render the "Configuring instances" table;
/// `Orchestrator` itself never prints.
pub struct ConfigureReport {
    pub nodes: Vec<(usize, SocketAddr)>,
    pub clients: Vec<(usize, SocketAddr)>,
}

impl ConfigureReport {
    /// Snapshot the addresses of every node and client the orchestrator just
    /// configured.
    pub fn new(nodes: &[Instance], clients: &[Instance]) -> Self {
        let enumerate_addresses = |instances: &[Instance]| {
            instances
                .iter()
                .enumerate()
                .map(|(index, instance)| (index, instance.ssh_address()))
                .collect()
        };
        Self {
            nodes: enumerate_addresses(nodes),
            clients: enumerate_addresses(clients),
        }
    }
}

/// Outcome of a successful [`crate::orchestrator::Orchestrator::start_monitoring`]
/// when the testbed has a dedicated monitoring instance configured. `None` from
/// `start_monitoring` means monitoring is disabled in
/// [`crate::settings::Settings`].
pub struct MonitoringReport {
    pub grafana_address: String,
    /// PromQL endpoint that consumers can hand to [`crate::collector::Collector`]
    /// when they want lib-provided metric collection. Always populated when this
    /// report is returned — the same monitoring instance hosts both servers.
    pub prometheus_address: String,
}

/// Snapshot of log-analysis state — the data callers need to render or react to
/// a benchmark's log outcome. Produced by [`crate::orchestrator::Orchestrator::download_logs`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogsReport {
    pub node_panic: bool,
    pub client_panic: bool,
    pub node_errors: usize,
    pub client_errors: usize,
}

/// One iteration of the benchmark tick loop. Future return type of
/// `Orchestrator::tick()`.
pub enum TickReport {
    /// A metrics scrape interval fired. When a Prometheus collector is active,
    /// `results` carries the YAML-serialised snapshot of the accumulated
    /// `BenchmarkResults` (for on-disk persistence).
    MetricsTick {
        elapsed: Duration,
        results: Option<String>,
        stats: LiveStats,
    },
    /// A fault-schedule interval fired and the orchestrator killed or rebooted
    /// instances accordingly.
    FaultUpdate {
        elapsed: Duration,
        action: CrashRecoveryAction,
    },
}
