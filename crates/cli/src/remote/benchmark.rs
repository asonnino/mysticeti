// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use eyre::{Context, Result};
use orchestrator::{
    benchmark::{BenchmarkParameters, Parameters},
    orchestrator::{MonitoringReport, Orchestrator},
    protocol::{ProtocolCommands, ProtocolMetrics, ProtocolParameters as _},
    provider::Instance,
    report::{LogsReport, TickReport},
    session::BenchmarkSession,
    settings::Settings,
};
use replica::result::Outcome;

use crate::{
    exporter::Exporter,
    remote::protocol::{ClientParameters, NodeParameters, ReplicaProtocol},
    terminal::{BannerPrinter, OutcomeDisplay, ResultRender, Terminal, table::SuiteRow},
};

/// Final result of one remote benchmark run, as the terminal renders it: the
/// configured duration plus the log-analysis report (absent when log processing
/// is disabled). Perf numbers live in the saved measurement files; the rendered
/// block carries only the pass/warn/fail verdict derived from the logs.
struct RemoteResult {
    duration: Duration,
    logs: Option<LogsReport>,
}

impl RemoteResult {
    /// Classify the log report into a consensus-style [`Outcome`] for display.
    /// `None` when logs were not collected.
    fn outcome(&self) -> Option<Outcome> {
        let logs = self.logs.as_ref()?;
        Some(if logs.node_panic || logs.client_panic {
            Outcome::Diverged
        } else if logs.node_errors != 0 || logs.client_errors != 0 {
            Outcome::NoProgress
        } else {
            Outcome::Pass
        })
    }
}

impl ResultRender for RemoteResult {
    fn render_block(&self, color: bool) -> String {
        // Log analysis disabled: no verdict to derive, so just confirm completion.
        let (Some(outcome), Some(logs)) = (self.outcome(), self.logs.as_ref()) else {
            return "Benchmark complete (log analysis disabled)".to_string();
        };
        let mut out = outcome.badge(color);
        if logs.node_errors != 0 || logs.client_errors != 0 {
            out.push('\n');
            out.push_str(&format!(
                "Log errors — node: {}, client: {}",
                logs.node_errors, logs.client_errors
            ));
        }
        out
    }

    fn suite_row(&self, name: &str, nodes: usize) -> Option<SuiteRow> {
        let outcome = self.outcome()?;
        // Remote runs have no per-replica committed-leader counts to show.
        Some(SuiteRow::new(
            name,
            nodes,
            self.duration.as_secs(),
            outcome,
            &[],
        ))
    }
}

pub struct RemoteBenchmarkDriver {
    settings: Settings,
    username: String,
    terminal: Terminal,
}

impl RemoteBenchmarkDriver {
    /// `runs` is the number of benchmarks the suite will execute (one per load),
    /// used for the per-run heading (`[i/N]`) and the suite summary.
    pub fn new(settings: Settings, username: String, runs: usize) -> Self {
        Self {
            settings,
            username,
            terminal: Terminal::new(runs),
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
        let protocol = node_parameters.consensus.to_string();
        BannerPrinter::new(
            "Remote benchmark",
            &[
                ("Protocol", &protocol),
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

        let orchestrator = Orchestrator::new(
            self.settings.clone(),
            instances,
            setup_commands,
            protocol_commands,
            &self.username,
        );

        // Validate instance capacity up front.
        if let Some(parameters) = parameters_set.first() {
            orchestrator
                .select_instances(parameters)
                .wrap_err("Not enough instances for this benchmark")?;
        }

        let mut latest_committee_size = 0;
        for (index, parameters) in parameters_set.into_iter().enumerate() {
            let benchmark_number = index + 1;
            self.terminal.print_config(benchmark_number, &parameters);

            // Prepare the testbed once, after announcing the first run's config,
            // so the user sees what is about to run before the (slow) setup.
            if index == 0 {
                self.terminal
                    .track("Cleaning up testbed", orchestrator.cleanup(true))
                    .await
                    .wrap_err("Cleanup failed")?;
                if !skip_testbed_update {
                    self.terminal
                        .track(
                            "Installing dependencies on all machines",
                            orchestrator.install(),
                        )
                        .await
                        .wrap_err("Install failed")?;
                    self.terminal
                        .track("Updating all instances", orchestrator.update())
                        .await
                        .wrap_err("Update failed")?;
                }
            }

            self.terminal
                .track("Cleaning up testbed", orchestrator.cleanup(true))
                .await
                .wrap_err("Cleanup failed")?;

            // When monitoring is disabled, start_monitoring always returns None;
            // skip the call entirely rather than performing a no-op SSH round-trip.
            let monitoring = if self.settings.monitoring {
                let report = self
                    .terminal
                    .track(
                        "Configuring monitoring instance",
                        orchestrator.start_monitoring(&parameters),
                    )
                    .await
                    .wrap_err("Monitoring setup failed")?;
                if let Some(r) = &report {
                    self.terminal.print_details(
                        "Grafana",
                        &[
                            ("url", r.grafana_address.as_str()),
                            ("username", "admin"),
                            ("password", "admin"),
                        ],
                    );
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

            self.terminal
                .track("Cleaning up testbed", orchestrator.cleanup(false))
                .await
                .wrap_err("Cleanup failed")?;

            let logs = if self.settings.log_processing {
                Some(
                    self.terminal
                        .track("Downloading logs", orchestrator.download_logs(&parameters))
                        .await
                        .wrap_err("Failed to download logs")?,
                )
            } else {
                None
            };
            let result = RemoteResult {
                duration: parameters.settings.benchmark_duration,
                logs,
            };
            self.terminal.print_results(&parameters, &result);
        }

        self.terminal.print_summary();
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
            self.terminal
                .track("Configuring instances", orchestrator.configure(parameters))
                .await
                .wrap_err("Configure failed")?;
            *latest_committee_size = parameters.nodes;
        }

        self.terminal
            .track("Deploying replicas", orchestrator.run_nodes(parameters))
            .await
            .wrap_err("Deploying replicas failed")?;

        let load_label = if parameters.load == 0 {
            "Skipping load generators (load = 0)"
        } else {
            "Setting up load generators"
        };
        self.terminal
            .track(load_label, orchestrator.run_clients(parameters))
            .await
            .wrap_err("Starting load generators failed")?;

        self.run_benchmark_loop(orchestrator, parameters, monitoring)
            .await
    }

    /// Drive the metrics + faults tick loop for a single benchmark run: render a
    /// live heartbeat from the collector's scraped stats on each metrics tick,
    /// persist each scrape's YAML, and announce fault injections.
    async fn run_benchmark_loop<P: ProtocolCommands + ProtocolMetrics>(
        &mut self,
        orchestrator: &Orchestrator<P>,
        parameters: &Parameters<P>,
        monitoring: Option<&MonitoringReport>,
    ) -> Result<()> {
        let benchmark_duration = parameters.settings.benchmark_duration;
        // A zero duration means "run indefinitely" (until Ctrl-C); show a spinner
        // instead of a determinate bar and never break the loop on elapsed time.
        let indefinite = benchmark_duration.is_zero();
        let scope = if monitoring.is_some() {
            "Scraping metrics"
        } else {
            "Running benchmark"
        };
        let label = if indefinite {
            format!("{scope} (indefinitely, Ctrl-C to stop)")
        } else {
            format!("{scope} (at least {}s)", benchmark_duration.as_secs())
        };

        let mut session = BenchmarkSession::new(orchestrator, parameters, monitoring)
            .await
            .wrap_err("Failed to start benchmark session")?;

        let results_path = self
            .settings
            .results_dir
            .join(format!("results-{}", self.settings.repository.commit));
        let exporter =
            Exporter::new(results_path).wrap_err("Failed to create results directory")?;

        let total = (!indefinite).then_some(benchmark_duration);
        self.terminal.print_separator();
        self.terminal.start_progress_animation(total, &label);
        let outcome = loop {
            let tick = tokio::select! {
                biased;
                // Stop gracefully on Ctrl-C so the caller still runs cleanup and
                // tears down the remote replicas, rather than leaking them.
                _ = tokio::signal::ctrl_c() => {
                    self.terminal
                        .suspend(|| eprintln!("Interrupted, stopping the benchmark"));
                    break Ok(());
                }
                tick = session.tick(orchestrator, parameters) => tick,
            };
            match tick {
                Err(e) => break Err(e).wrap_err("Benchmark tick failed"),
                Ok(TickReport::MetricsTick {
                    elapsed,
                    results,
                    stats,
                }) => {
                    self.terminal.print_status(elapsed, &stats);
                    if let Some(yaml) = results {
                        let key = format!("{parameters:?}");
                        let exporter_clone = exporter.clone();
                        let result = tokio::task::spawn_blocking(move || {
                            exporter_clone.write_benchmark_result(&yaml, &key)
                        })
                        .await
                        .map_err(|e| eyre::eyre!("write_benchmark_result task panicked: {e}"))
                        .and_then(|r| r);
                        if let Err(e) = result {
                            break Err(e).wrap_err("Failed to save benchmark results");
                        }
                    }
                    if !indefinite && elapsed > benchmark_duration {
                        break Ok(());
                    }
                }
                Ok(TickReport::FaultUpdate { elapsed, action }) => {
                    self.terminal.set_elapsed(elapsed);
                    if !action.kill.is_empty() || !action.boot.is_empty() {
                        self.terminal
                            .suspend(|| eprintln!("Testbed update: {action}"));
                    }
                }
            }
        };
        self.terminal.stop_progress_animation();
        outcome
    }
}
