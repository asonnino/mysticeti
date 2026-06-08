// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt::Display, future::Future, path::PathBuf, time::Duration};

use eyre::{Context, Result};
use orchestrator::{
    benchmark::{BenchmarkParameters, Parameters},
    collector::Collector,
    error::MonitorError,
    faults::CrashRecoverySchedule,
    orchestrator::{MonitoringReport, Orchestrator},
    protocol::{ProtocolCommands, ProtocolMetrics, ProtocolParameters as _},
    provider::ServerProviderClient,
    settings::{CloudProvider, Settings},
    ssh::SshConnectionManager,
    testbed::{Testbed, TestbedStatus},
};
use tokio::time::{self, Instant};
use tracing_subscriber::filter::LevelFilter;

use crate::{
    args::{RemoteTestbedArgs, RemoteTestbedCommand},
    protocol::{ClientParameters, NodeParameters, ReplicaProtocol},
    terminal::{BOLD, BannerPrinter, Progress, RESET, stderr_supports_color},
    tracing::ReplicaTracing,
};

/// Run a single phase under an indeterminate spinner. Will eventually move to
/// `terminal.rs` when `Terminal` is unified to cover remote testbed runs
/// (see issue #170).
async fn phase<T, E, F>(progress: &mut Progress, label: &str, work: F) -> std::result::Result<T, E>
where
    F: Future<Output = std::result::Result<T, E>>,
{
    progress.start(None, label);
    let result = work.await;
    progress.stop();
    result
}

trait TestbedStatusRender {
    fn render(&self, color: bool) -> String;
}

impl TestbedStatusRender for TestbedStatus {
    fn render(&self, color: bool) -> String {
        let mut out = format!(
            "Client: {}\nRepo: {} ({})\nInstances active: {}\n",
            self.client_summary, self.repository_url, self.repository_commit, self.active_count
        );
        for region in &self.regions {
            out.push('\n');
            let heading = region.region.to_uppercase();
            if color {
                out.push_str(&format!("{BOLD}[{heading}]{RESET}\n"));
            } else {
                out.push_str(&format!("[{heading}]\n"));
            }
            if region.instances.is_empty() {
                out.push_str("  (no instances)\n");
                continue;
            }
            for (index, instance) in region.instances.iter().enumerate() {
                let marker = if instance.active { "●" } else { "○" };
                out.push_str(&format!(
                    "  {marker} {index:>2}  {}\n",
                    instance.connect_command
                ));
            }
        }
        out
    }
}

struct RemoteTestbedDriver<C> {
    testbed: Testbed<C>,
    settings: Settings,
    ssh_manager: SshConnectionManager,
    progress: Progress,
    color: bool,
}

impl<C: ServerProviderClient> RemoteTestbedDriver<C> {
    async fn new(settings: Settings, client: C, color: bool) -> Result<Self> {
        let testbed = Testbed::new(settings.clone(), client)
            .await
            .wrap_err("Failed to create testbed")?;
        let ssh_manager = SshConnectionManager::new(
            testbed.username().into(),
            settings.ssh_private_key_file.clone(),
        )
        .with_timeout(settings.ssh_timeout)
        .with_retries(settings.ssh_retries);
        Ok(Self {
            testbed,
            settings,
            ssh_manager,
            progress: Progress::new(color),
            color,
        })
    }

    async fn benchmark(
        &mut self,
        committee: usize,
        loads: Vec<usize>,
        skip_testbed_update: bool,
        skip_testbed_configuration: bool,
    ) -> Result<()> {
        let instances = self.testbed.instances();
        let setup_commands = self
            .testbed
            .setup_commands()
            .await
            .wrap_err("Failed to load testbed setup commands")?;

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
            self.ssh_manager.clone(),
        );

        phase(
            &mut self.progress,
            "Cleaning up testbed",
            orchestrator.cleanup(true),
        )
        .await
        .wrap_err("Cleanup failed")?;

        if !skip_testbed_update {
            phase(
                &mut self.progress,
                "Installing dependencies on all machines",
                orchestrator.install(),
            )
            .await
            .wrap_err("Install failed")?;
            phase(
                &mut self.progress,
                "Updating all instances",
                orchestrator.update(),
            )
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

            phase(
                &mut self.progress,
                "Cleaning up testbed",
                orchestrator.cleanup(true),
            )
            .await
            .wrap_err("Cleanup failed")?;

            // When monitoring is disabled, start_monitoring always returns None;
            // skip the call entirely rather than performing a no-op SSH round-trip.
            let monitoring = if self.settings.monitoring {
                let report = phase(
                    &mut self.progress,
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

            phase(
                &mut self.progress,
                "Cleaning up testbed",
                orchestrator.cleanup(false),
            )
            .await
            .wrap_err("Cleanup failed")?;

            if self.settings.log_processing {
                let logs = phase(
                    &mut self.progress,
                    "Downloading logs",
                    orchestrator.download_logs(&parameters),
                )
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
            let configure_report = phase(
                &mut self.progress,
                "Configuring instances",
                orchestrator.configure(parameters),
            )
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

        phase(
            &mut self.progress,
            "Deploying replicas",
            orchestrator.run_nodes(parameters),
        )
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
        phase(
            &mut self.progress,
            load_label,
            orchestrator.run_clients(parameters),
        )
        .await
        .wrap_err("Starting load generators failed")?;

        self.run_benchmark_loop(orchestrator, parameters, monitoring)
            .await
    }

    /// Drive the metrics + faults tick loop for a single benchmark run. Owns the
    /// `tokio::select!`, the fault schedule, and (when monitoring is deployed) a
    /// PromQL [`Collector`] that polls Prometheus on the metrics tick.
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

        let (_, nodes, _) = orchestrator
            .select_instances(parameters)
            .wrap_err("Failed to select instances for benchmark loop")?;
        let mut schedule = CrashRecoverySchedule::new(parameters.settings.faults.clone(), nodes);

        let mut collector = monitoring
            .map(|report| {
                Collector::new(
                    &report.prometheus_address,
                    parameters.clone(),
                    orchestrator.protocol().metrics(),
                )
            })
            .transpose()
            .wrap_err("Failed to set up the metrics collector")?;

        let mut metrics_interval = time::interval(parameters.settings.scrape_interval);
        metrics_interval.tick().await;
        let mut faults_interval = time::interval(parameters.settings.faults.crash_interval());
        faults_interval.tick().await;

        let results_path = self
            .settings
            .results_dir
            .join(format!("results-{}", self.settings.repository.commit));
        std::fs::create_dir_all(&results_path)
            .map_err(MonitorError::ResultsWriteError)
            .wrap_err("Failed to create results directory")?;

        self.progress.start(Some(benchmark_duration), &label);
        let start = Instant::now();
        let outcome = loop {
            tokio::select! {
                now = metrics_interval.tick() => {
                    let elapsed = now.duration_since(start);
                    self.progress.set_elapsed(elapsed);
                    if let Some(collector) = collector.as_mut() {
                        if let Err(error) = collector.collect().await {
                            break Err(error);
                        }
                        if let Err(error) = collector.save(&results_path) {
                            break Err(error);
                        }
                    }
                    if elapsed > benchmark_duration {
                        break Ok(());
                    }
                },
                _ = faults_interval.tick() => {
                    let action = match orchestrator
                        .apply_faults_step(parameters, &mut schedule)
                        .await
                    {
                        Ok(action) => action,
                        Err(error) => {
                            self.progress.stop();
                            return Err(error).wrap_err("Failed to apply fault schedule");
                        }
                    };
                    if !action.kill.is_empty() || !action.boot.is_empty() {
                        self.progress.suspend(|| eprintln!("Testbed update: {action}"));
                    }
                }
            }
            self.progress.set_elapsed(
                start
                    .elapsed()
                    .min(benchmark_duration + Duration::from_secs(1)),
            );
        };
        self.progress.stop();
        outcome.wrap_err("Metrics collection failed")
    }
}

impl<C: ServerProviderClient + Display> RemoteTestbedDriver<C> {
    pub async fn run(mut self, command: RemoteTestbedCommand) -> Result<()> {
        match command {
            RemoteTestbedCommand::Status => {
                eprint!("{}", self.testbed.status().render(self.color));
                Ok(())
            }
            RemoteTestbedCommand::Create { instances, region } => {
                let label = format!("Creating instances ({instances} per region)");
                phase(
                    &mut self.progress,
                    &label,
                    self.testbed.create(instances, region),
                )
                .await
                .wrap_err("Failed to create testbed")
            }
            RemoteTestbedCommand::Start { instances } => phase(
                &mut self.progress,
                "Booting instances",
                self.testbed.start(instances),
            )
            .await
            .wrap_err("Failed to start testbed"),
            RemoteTestbedCommand::Stop => phase(
                &mut self.progress,
                "Stopping instances",
                self.testbed.stop(),
            )
            .await
            .wrap_err("Failed to stop testbed"),
            RemoteTestbedCommand::Destroy => phase(
                &mut self.progress,
                "Destroying testbed",
                self.testbed.destroy(),
            )
            .await
            .wrap_err("Failed to destroy testbed"),
            RemoteTestbedCommand::Benchmark {
                committee,
                loads,
                skip_testbed_update,
                skip_testbed_configuration,
            } => {
                self.benchmark(
                    committee,
                    loads,
                    skip_testbed_update,
                    skip_testbed_configuration,
                )
                .await
            }
        }
    }
}

pub async fn remote_testbed(
    args: RemoteTestbedArgs,
    log_level: Option<LevelFilter>,
    log_file: Option<PathBuf>,
) -> Result<()> {
    // Match the local-testbed default: route INFO chatter to the log file when
    // one is configured, but keep stderr quiet (WARN) by default so the banner
    // and progress lines stay readable.
    let default_level = if log_file.is_some() {
        LevelFilter::INFO
    } else {
        LevelFilter::WARN
    };
    let level = log_level.unwrap_or(default_level);
    let _guard = ReplicaTracing::new(level).with_log_file(log_file).setup()?;

    let color = stderr_supports_color();
    let settings_path = args.settings_path.display().to_string();
    let settings = Settings::load(&settings_path).wrap_err("Failed to load settings")?;

    match &settings.cloud_provider {
        CloudProvider::Aws(_) => {
            use orchestrator::provider::aws::AwsClient;
            let client = AwsClient::new(settings.clone()).await;
            RemoteTestbedDriver::new(settings, client, color)
                .await?
                .run(args.command)
                .await
        }
        CloudProvider::Custom(_) => {
            use orchestrator::provider::custom::CustomClient;
            let client = CustomClient::new(settings.clone());
            RemoteTestbedDriver::new(settings, client, color)
                .await?
                .run(args.command)
                .await
        }
    }
}
