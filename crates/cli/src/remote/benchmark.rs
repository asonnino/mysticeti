// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use eyre::{Context, Result};
use orchestrator::{
    benchmark::{BenchmarkParameters, Parameters},
    error::MonitorError,
    orchestrator::{MonitoringReport, Orchestrator},
    protocol::{ProtocolCommands, ProtocolMetrics, ProtocolParameters as _},
    provider::Instance,
    report::TickReport,
    session::BenchmarkSession,
    settings::Settings,
};

use crate::{
    remote::protocol::{ClientParameters, NodeParameters, ReplicaProtocol},
    terminal::{BannerPrinter, Progress},
};

pub struct RemoteBenchmarkDriver {
    settings: Settings,
    username: String,
    progress: Progress,
}

impl RemoteBenchmarkDriver {
    pub fn new(settings: Settings, username: String, color: bool) -> Self {
        Self {
            settings,
            username,
            progress: Progress::new(color),
        }
    }

    pub async fn benchmark(
        mut self,
        instances: Vec<Instance>,
        setup_commands: Vec<String>,
        committee: usize,
        loads: Vec<usize>,
        skip_testbed_update: bool,
        skip_testbed_configuration: bool,
    ) -> Result<()> {
        let protocol_commands = ReplicaProtocol::new(&self.settings);
        let node_parameters = match &self.settings.node_parameters_path {
            Some(path) => {
                NodeParameters::load(path).wrap_err("Failed to load node's parameters")?
            }
            None => NodeParameters::default(),
        };
        let client_parameters = match &self.settings.client_parameters_path {
            Some(path) => {
                ClientParameters::load(path).wrap_err("Failed to load client's parameters")?
            }
            None => ClientParameters::default(),
        };

        // Apply the commit side-effect before settings are snapshotted into
        // per-benchmark parameters by new_from_loads.
        if skip_testbed_update {
            self.settings.repository.set_unknown_commit();
        }

        let loads_label = loads
            .iter()
            .map(|l| l.to_string())
            .collect::<Vec<_>>()
            .join(",");
        BannerPrinter::new(
            "Mysticeti remote benchmark",
            &[
                ("Committee", &committee.to_string()),
                ("Loads", &loads_label),
                ("Commit", &self.settings.repository.commit),
            ],
        )
        .print();

        if skip_testbed_update {
            eprintln!("WARNING: skipping testbed update! Use with care.");
        }
        if skip_testbed_configuration {
            eprintln!("WARNING: skipping testbed configuration! Use with care.");
        }

        let parameters_set = BenchmarkParameters::new_from_loads(
            self.settings.clone(),
            node_parameters,
            client_parameters,
            committee,
            loads,
        );
        let total = parameters_set.len();

        let orchestrator = Orchestrator::new(
            self.settings.clone(),
            instances,
            setup_commands,
            protocol_commands,
            &self.username,
        );

        self.progress
            .track("Cleaning up testbed", orchestrator.cleanup(true))
            .await
            .wrap_err("Cleanup failed")?;

        if !skip_testbed_update {
            self.progress
                .track(
                    "Installing dependencies on all machines",
                    orchestrator.install(),
                )
                .await
                .wrap_err("Install failed")?;
            self.progress
                .track("Updating all instances", orchestrator.update())
                .await
                .wrap_err("Update failed")?;
        }

        let mut latest_committee_size = 0;
        for (index, parameters) in parameters_set.into_iter().enumerate() {
            let benchmark_number = index + 1;
            eprintln!();
            eprintln!(
                "--- Benchmark {benchmark_number}/{total}: {} ---",
                parameters
            );
            eprintln!("Node parameters: {}", parameters.node_parameters);

            self.progress
                .track("Cleaning up testbed", orchestrator.cleanup(true))
                .await
                .wrap_err("Cleanup failed")?;

            // When monitoring is disabled, start_monitoring always returns None;
            // skip the call entirely rather than performing a no-op SSH round-trip.
            let monitoring = if self.settings.monitoring {
                let report = self
                    .progress
                    .track(
                        "Configuring monitoring instance",
                        orchestrator.start_monitoring(&parameters),
                    )
                    .await
                    .wrap_err("Monitoring setup failed")?;
                if let Some(r) = &report {
                    eprintln!("Grafana: {}", r.grafana_address);
                }
                report
            } else {
                None
            };

            self.run_one(
                &orchestrator,
                &parameters,
                monitoring.as_ref(),
                &mut latest_committee_size,
                skip_testbed_configuration,
            )
            .await?;

            self.progress
                .track("Cleaning up testbed", orchestrator.cleanup(false))
                .await
                .wrap_err("Cleanup failed")?;

            if self.settings.log_processing {
                let logs = self
                    .progress
                    .track("Downloading logs", orchestrator.download_logs(&parameters))
                    .await
                    .wrap_err("Failed to download logs")?;
                if logs.node_panic {
                    eprintln!("ERROR: node(s) panicked!");
                } else if logs.client_panic {
                    eprintln!("ERROR: client(s) panicked!");
                } else if logs.node_errors != 0 || logs.client_errors != 0 {
                    eprintln!(
                        "WARNING: logs contain errors (node: {}, client: {})",
                        logs.node_errors, logs.client_errors,
                    );
                }
            }
        }

        eprintln!();
        eprintln!("Benchmark completed");
        Ok(())
    }

    async fn run_one<P: ProtocolCommands + ProtocolMetrics>(
        &mut self,
        orchestrator: &Orchestrator<P>,
        parameters: &Parameters<P>,
        monitoring: Option<&MonitoringReport>,
        latest_committee_size: &mut usize,
        skip_testbed_configuration: bool,
    ) -> Result<()> {
        if !skip_testbed_configuration && *latest_committee_size != parameters.nodes {
            let configure_report = self
                .progress
                .track("Configuring instances", orchestrator.configure(parameters))
                .await
                .wrap_err("Configure failed")?;
            for (node_index, address) in &configure_report.nodes {
                eprintln!("  node {node_index}: {address}");
            }
            for (client_index, address) in &configure_report.clients {
                eprintln!("  client {client_index}: {address}");
            }
            *latest_committee_size = parameters.nodes;
        }

        self.progress
            .track("Deploying replicas", orchestrator.run_nodes(parameters))
            .await
            .wrap_err("Deploying replicas failed")?;

        if parameters.settings.benchmark_duration.as_secs() == 0 {
            return Ok(());
        }

        let load_label = if parameters.load == 0 {
            "Skipping load generators (load = 0)"
        } else {
            "Setting up load generators"
        };
        self.progress
            .track(load_label, orchestrator.run_clients(parameters))
            .await
            .wrap_err("Starting load generators failed")?;

        self.run_benchmark_loop(orchestrator, parameters, monitoring)
            .await
    }

    /// Drive the metrics + faults tick loop for a single benchmark run. Owns the
    /// `tokio::select!`, the fault schedule, and (when monitoring is deployed) a
    /// PromQL [`Collector`] that polls Prometheus on the metrics tick.
    /// Drive the metrics + faults tick loop for a single benchmark run.
    async fn run_benchmark_loop<P: ProtocolCommands + ProtocolMetrics>(
        &mut self,
        orchestrator: &Orchestrator<P>,
        parameters: &Parameters<P>,
        monitoring: Option<&MonitoringReport>,
    ) -> Result<()> {
        let benchmark_duration = parameters.settings.benchmark_duration;
        let label = if monitoring.is_some() {
            format!(
                "Scraping metrics (at least {}s)",
                benchmark_duration.as_secs()
            )
        } else {
            format!(
                "Running benchmark (at least {}s)",
                benchmark_duration.as_secs()
            )
        };

        let mut session = BenchmarkSession::new(orchestrator, parameters, monitoring)
            .await
            .wrap_err("Failed to start benchmark session")?;

        let results_path = self
            .settings
            .results_dir
            .join(format!("results-{}", self.settings.repository.commit));
        tokio::fs::create_dir_all(&results_path)
            .await
            .map_err(MonitorError::ResultsWriteError)
            .wrap_err("Failed to create results directory")?;

        self.progress.start(Some(benchmark_duration), &label);
        let outcome = loop {
            match session.tick(orchestrator, parameters).await {
                Err(e) => break Err(e).wrap_err("Benchmark tick failed"),
                Ok(TickReport::MetricsTick { elapsed, results }) => {
                    self.progress.set_elapsed(elapsed);
                    if let Some(yaml) = results {
                        let path = results_path.join(format!("measurements-{:?}.yaml", parameters));
                        if let Err(e) = tokio::fs::write(&path, &yaml).await {
                            break Err(MonitorError::ResultsWriteError(e))
                                .wrap_err("Failed to save benchmark results");
                        }
                    }
                    if elapsed > benchmark_duration {
                        break Ok(());
                    }
                }
                Ok(TickReport::FaultUpdate { elapsed, action }) => {
                    self.progress.set_elapsed(elapsed);
                    if !action.kill.is_empty() || !action.boot.is_empty() {
                        self.progress
                            .suspend(|| eprintln!("Testbed update: {action}"));
                    }
                }
            }
        };
        self.progress.stop();
        outcome
    }
}
