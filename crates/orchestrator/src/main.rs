// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Orchestrator entry point.

use benchmark::BenchmarkParameters;
use clap::Parser;
use client::{ServerProviderClient, aws::AwsClient, vultr::VultrClient};
use collector::Collector;
use error::TestbedResult;
use eyre::Context;
use faults::CrashRecoverySchedule;
use orchestrator::Orchestrator;
use protocol::{ProtocolCommands, ProtocolMetrics, ProtocolParameters};
use settings::{CloudProvider, Settings};
use ssh::SshConnectionManager;
use testbed::Testbed;
use tokio::time::{self, Instant};

mod benchmark;
mod client;
mod collector;
mod display;
mod error;
mod faults;
mod logs;
mod monitor;
mod orchestrator;
mod protocol;
mod settings;
mod ssh;
mod testbed;

/// NOTE: Link these types to the correct protocol.
type Protocol = protocol::mysticeti::MysticetiProtocol;
type NodeParameters = protocol::mysticeti::MysticetiNodeParameters;
type ClientParameters = protocol::mysticeti::MysticetiClientParameters;

/// The orchestrator command line options.
#[derive(Parser, Debug)]
#[command(author, version, about = "Testbed orchestrator", long_about = None)]
#[clap(rename_all = "kebab-case")]
pub struct Opts {
    /// The path to the settings file. This file contains basic information to deploy testbeds
    /// and run benchmarks such as the url of the git repo, the commit to deploy, etc.
    #[clap(
        long,
        value_name = "FILE",
        default_value = "crates/orchestrator/assets/settings.yml",
        global = true
    )]
    settings_path: String,

    /// The type of operation to run.
    #[clap(subcommand)]
    operation: Operation,
}

/// The type of operation to run.
#[derive(Parser, Debug)]
#[clap(rename_all = "kebab-case")]
pub enum Operation {
    /// Read or modify the status of the testbed.
    Testbed {
        /// The action to perform on the testbed.
        #[clap(subcommand)]
        action: TestbedAction,
    },
    /// Deploy nodes and run a benchmark on the specified testbed.
    Benchmark {
        /// The committee size to deploy.
        #[clap(long, value_name = "INT", default_value_t = 4, global = true)]
        committee: usize,

        /// The set of loads to submit to the system (tx/s). Each load triggers a separate
        /// benchmark run. Setting a load to zero will not deploy any benchmark clients
        /// (useful to boot testbeds designed to run with external clients and load generators).
        #[clap(long, value_name = "[INT]", default_value = "200", global = true)]
        loads: Vec<usize>,

        /// Whether to skip testbed updates before running benchmarks. This is a dangerous
        /// operation as it may lead to running benchmarks on outdated nodes. It is however
        /// useful when debugging in some specific scenarios.
        #[clap(long, action, default_value_t = false, global = true)]
        skip_testbed_update: bool,

        /// Whether to skip testbed configuration before running benchmarks. This is a dangerous
        /// operation as it may lead to running benchmarks on misconfigured nodes. It is however
        /// useful when debugging in some specific scenarios.
        #[clap(long, action, default_value_t = false, global = true)]
        skip_testbed_configuration: bool,
    },
}

/// The action to perform on the testbed.
#[derive(Parser, Debug)]
#[clap(rename_all = "kebab-case")]
pub enum TestbedAction {
    /// Display the testbed status.
    Status,

    /// Deploy the specified number of instances in all regions specified by in the setting file.
    Deploy {
        /// Number of instances to deploy.
        #[clap(long)]
        instances: usize,

        /// The region where to deploy the instances. If this parameter is not specified, the
        /// command deploys the specified number of instances in all regions listed in the
        /// setting file.
        #[clap(long)]
        region: Option<String>,
    },

    /// Start at most the specified number of instances per region on an existing testbed.
    Start {
        /// Number of instances to deploy.
        #[clap(long, default_value_t = 10)]
        instances: usize,
    },

    /// Stop an existing testbed (without destroying the instances).
    Stop,

    /// Destroy the testbed and terminate all instances.
    Destroy,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    color_eyre::install()?;
    let opts: Opts = Opts::parse();

    // Load the settings files.
    let settings = Settings::load(&opts.settings_path).wrap_err("Failed to load settings")?;

    match &settings.cloud_provider {
        CloudProvider::Aws => {
            // Create the client for the cloud provider.
            let client = AwsClient::new(settings.clone()).await;

            // Execute the command.
            run(settings, client, opts).await
        }
        CloudProvider::Vultr => {
            // Create the client for the cloud provider.
            let token = settings
                .load_token()
                .wrap_err("Failed to load cloud provider's token")?;
            let client = VultrClient::new(token, settings.clone());

            // Execute the command.
            run(settings, client, opts).await
        }
    }
}

async fn run<C: ServerProviderClient>(
    settings: Settings,
    client: C,
    opts: Opts,
) -> eyre::Result<()> {
    // Create a new testbed.
    let mut testbed = Testbed::new(settings.clone(), client)
        .await
        .wrap_err("Failed to crate testbed")?;

    match opts.operation {
        Operation::Testbed { action } => match action {
            // Display the current status of the testbed.
            TestbedAction::Status => testbed.status(),

            // Deploy the specified number of instances on the testbed.
            TestbedAction::Deploy { instances, region } => testbed
                .deploy(instances, region)
                .await
                .wrap_err("Failed to deploy testbed")?,

            // Start the specified number of instances on an existing testbed.
            TestbedAction::Start { instances } => testbed
                .start(instances)
                .await
                .wrap_err("Failed to start testbed")?,

            // Stop an existing testbed.
            TestbedAction::Stop => testbed.stop().await.wrap_err("Failed to stop testbed")?,

            // Destroy the testbed and terminal all instances.
            TestbedAction::Destroy => testbed
                .destroy()
                .await
                .wrap_err("Failed to destroy testbed")?,
        },

        // Run benchmarks.
        Operation::Benchmark {
            committee,
            loads,
            skip_testbed_update,
            skip_testbed_configuration,
        } => {
            // Create a new orchestrator to instruct the testbed.
            let username = testbed.username();
            let private_key_file = settings.ssh_private_key_file.clone();
            let ssh_manager = SshConnectionManager::new(username.into(), private_key_file)
                .with_timeout(settings.ssh_timeout)
                .with_retries(settings.ssh_retries);

            let instances = testbed.instances();

            let setup_commands = testbed
                .setup_commands()
                .await
                .wrap_err("Failed to load testbed setup commands")?;

            let protocol_commands = Protocol::new(&settings);
            let node_parameters = match &settings.node_parameters_path {
                Some(path) => {
                    NodeParameters::load(path).wrap_err("Failed to load node's parameters")?
                }
                None => NodeParameters::default(),
            };
            let client_parameters = match &settings.client_parameters_path {
                Some(path) => {
                    ClientParameters::load(path).wrap_err("Failed to load client's parameters")?
                }
                None => ClientParameters::default(),
            };

            // Skip-flag side effects live in the caller now: warn the user, and
            // pin `unknown_commit` to the repository before the orchestrator
            // snapshots the settings.
            let mut settings = settings;
            if skip_testbed_update {
                display::warn("Skipping testbed update! Use with care!");
                settings.repository.set_unknown_commit();
            }
            if skip_testbed_configuration {
                display::warn("Skipping testbed configuration! Use with care!");
            }

            let set_of_benchmark_parameters = BenchmarkParameters::new_from_loads(
                settings.clone(),
                node_parameters,
                client_parameters,
                committee,
                loads,
            );

            let orchestrator = Orchestrator::new(
                settings.clone(),
                instances,
                setup_commands,
                protocol_commands,
                ssh_manager,
            );

            run_benchmarks(
                &orchestrator,
                &settings,
                set_of_benchmark_parameters,
                skip_testbed_update,
                skip_testbed_configuration,
            )
            .await
            .wrap_err("Failed to run benchmarks")?;
        }
    }
    Ok(())
}

/// Drive the full benchmark sweep: prepare the testbed once, then run each
/// configured load through the orchestrator's phased API while rendering every
/// banner, action, and config row through [`display`]. The orchestrator never
/// touches stdout — this function is where all human-visible output happens.
async fn run_benchmarks<P: ProtocolCommands + ProtocolMetrics>(
    orchestrator: &Orchestrator<P>,
    settings: &Settings,
    parameters_set: Vec<BenchmarkParameters>,
    skip_testbed_update: bool,
    skip_testbed_configuration: bool,
) -> TestbedResult<()> {
    display::header("Preparing testbed");
    display::config("Commit", format!("'{}'", &settings.repository.commit));
    display::newline();

    display::action("Cleaning up testbed");
    orchestrator.cleanup(true).await?;
    display::done();

    if !skip_testbed_update {
        display::action("Installing dependencies on all machines");
        orchestrator.install().await?;
        display::done();

        display::action("Updating all instances");
        orchestrator.update().await?;
        display::done();
    }

    let mut latest_committee_size = 0;
    for (index, parameters) in parameters_set.into_iter().enumerate() {
        let benchmark_number = index + 1;
        display::header(format!("Starting benchmark {benchmark_number}"));
        display::config("Node Parameters", &parameters.node_parameters);
        display::config("Benchmark Parameters", &parameters);
        display::newline();

        display::action("Cleaning up testbed");
        orchestrator.cleanup(true).await?;
        display::done();

        // Emit the action banner before the slow SSH work so that long setup
        // times (or errors) aren't silent. The banner is gated on
        // `settings.monitoring` rather than on `start_monitoring`'s return value
        // because we need to commit to printing before awaiting the call.
        if settings.monitoring {
            display::action("Configuring monitoring instance");
        }
        let monitoring = orchestrator.start_monitoring(&parameters).await?;
        if settings.monitoring {
            display::done();
        }
        if let Some(report) = &monitoring {
            display::config("Grafana address", &report.grafana_address);
            display::newline();
        }

        if !skip_testbed_configuration && latest_committee_size != parameters.nodes {
            display::action("Configuring instances");
            let configure_report = orchestrator.configure(&parameters).await?;
            display::done();
            for (node_index, address) in &configure_report.nodes {
                display::config(format!("  - node {node_index}"), address);
            }
            for (client_index, address) in &configure_report.clients {
                display::config(format!("  - client {client_index}"), address);
            }
            latest_committee_size = parameters.nodes;
        }

        display::action("\nDeploying replicas");
        orchestrator.run_nodes(&parameters).await?;
        display::done();
        if parameters.settings.benchmark_duration.as_secs() == 0 {
            return Ok(());
        }

        // Decide the banner from the input rather than the report so the user
        // sees what's about to happen, not what already happened.
        display::action(if parameters.load == 0 {
            "Skipping load generators deployment (load = 0)"
        } else {
            "Setting up load generators"
        });
        orchestrator.run_clients(&parameters).await?;
        display::done();

        run_benchmark_loop(orchestrator, settings, &parameters, monitoring.as_ref()).await?;

        display::action("Cleaning up testbed");
        orchestrator.cleanup(false).await?;
        display::done();

        if settings.log_processing {
            display::action("Downloading logs");
            let logs = orchestrator.download_logs(&parameters).await?;
            display::done();
            if logs.node_panic {
                display::error("Node(s) panicked!");
            } else if logs.client_panic {
                display::error("Client(s) panicked!");
            } else if logs.node_errors != 0 || logs.client_errors != 0 {
                display::newline();
                display::warn(format!(
                    "Logs contain errors (node: {}, client: {})",
                    logs.node_errors, logs.client_errors,
                ));
            }
        }
    }

    display::header("Benchmark completed");
    Ok(())
}

/// Drive the metrics/fault tick loop for a single benchmark run. Owns the
/// `tokio::select!`, the fault schedule, and (when monitoring is deployed) a
/// PromQL [`Collector`] that polls Prometheus on the metrics tick. The
/// orchestrator never touches stdout or local files — that is all caller
/// policy here.
async fn run_benchmark_loop<P: ProtocolCommands + ProtocolMetrics>(
    orchestrator: &Orchestrator<P>,
    settings: &Settings,
    parameters: &BenchmarkParameters,
    monitoring: Option<&orchestrator::MonitoringReport>,
) -> TestbedResult<()> {
    // The banner copy depends on whether metrics will actually be collected:
    // without monitoring deployed there's no collector, so "Scraping metrics"
    // would be misleading.
    display::action(if monitoring.is_some() {
        format!(
            "Scraping metrics (at least {}s)",
            settings.benchmark_duration.as_secs()
        )
    } else {
        format!(
            "Running benchmark (at least {}s)",
            settings.benchmark_duration.as_secs()
        )
    });

    let (_, nodes, _) = orchestrator.select_instances(parameters)?;
    let mut schedule = CrashRecoverySchedule::new(parameters.settings.faults.clone(), nodes);

    let mut collector = monitoring
        .map(|report| {
            Collector::new(
                &report.prometheus_address,
                parameters.clone(),
                Protocol::new(settings).metric_names(),
            )
        })
        .transpose()?;

    let mut metrics_interval = time::interval(settings.scrape_interval);
    metrics_interval.tick().await; // The first tick returns immediately.
    let mut faults_interval = time::interval(settings.faults.crash_interval());
    faults_interval.tick().await; // The first tick returns immediately.

    // Per-commit subdirectory so that running the same benchmark parameters
    // against different commits doesn't overwrite previous results.
    let results_path = settings
        .results_dir
        .join(format!("results-{}", settings.repository.commit));
    std::fs::create_dir_all(&results_path).map_err(error::MonitorError::ResultsWriteError)?;

    let start = Instant::now();
    loop {
        tokio::select! {
            now = metrics_interval.tick() => {
                let elapsed = now.duration_since(start).as_secs_f64().ceil() as u64;
                display::status(format!("{elapsed}s"));
                if let Some(collector) = collector.as_mut() {
                    collector.collect().await?;
                    collector.save(&results_path)?;
                }
                if elapsed > parameters.settings.benchmark_duration.as_secs() {
                    break;
                }
            },
            _ = faults_interval.tick() => {
                let action = orchestrator
                    .apply_faults_step(parameters, &mut schedule)
                    .await?;
                if !action.kill.is_empty() || !action.boot.is_empty() {
                    display::newline();
                    display::config("Testbed update", action);
                }
            }
        }
    }

    display::done();
    Ok(())
}
