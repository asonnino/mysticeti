// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{future::Future, path::PathBuf, time::Duration};

use eyre::{Context, Result};
use orchestrator::{
    benchmark::{BenchmarkParameters, Parameters},
    collector::Collector,
    error::MonitorError,
    faults::CrashRecoverySchedule,
    orchestrator::{MonitoringReport, Orchestrator},
    protocol::{ProtocolCommands, ProtocolMetrics, ProtocolParameters},
    provider::{ServerProviderClient, aws::AwsClient, custom::CustomClient},
    settings::{CloudProvider, Settings},
    ssh::SshConnectionManager,
    testbed::{Testbed, TestbedStatus},
};
use tokio::time::{self, Instant};
use tracing_subscriber::filter::LevelFilter;

use crate::{
    args::{RemoteTestbedArgs, RemoteTestbedCommand},
    protocol::{MysticetiClientParameters, MysticetiNodeParameters, MysticetiProtocol},
    terminal::{BannerPrinter, Progress, stderr_supports_color},
    tracing::ReplicaTracing,
};

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

    let settings_path = args.settings_path.display().to_string();
    let settings = Settings::load(&settings_path).wrap_err("Failed to load settings")?;

    match &settings.cloud_provider {
        CloudProvider::Aws(_) => {
            let client = AwsClient::new(settings.clone()).await;
            dispatch(settings, client, args.command).await
        }
        CloudProvider::Custom(_) => {
            let client = CustomClient::new(settings.clone());
            dispatch(settings, client, args.command).await
        }
    }
}

/// Run a single phase under an indeterminate spinner so the user sees activity
/// during the long SSH waits the orchestrator performs internally.
async fn with_spinner<T, E, F>(label: &str, work: F) -> std::result::Result<T, E>
where
    F: Future<Output = std::result::Result<T, E>>,
{
    let mut progress = Progress::new(stderr_supports_color());
    progress.start(None, label);
    let result = work.await;
    progress.stop();
    result
}

async fn dispatch<C: ServerProviderClient>(
    settings: Settings,
    client: C,
    command: RemoteTestbedCommand,
) -> Result<()> {
    let mut testbed = Testbed::new(settings.clone(), client)
        .await
        .wrap_err("Failed to create testbed")?;

    match command {
        RemoteTestbedCommand::Status => {
            print_status(&testbed.status());
            Ok(())
        }
        RemoteTestbedCommand::Create { instances, region } => {
            let label = format!("Creating instances ({instances} per region)");
            with_spinner(&label, testbed.create(instances, region))
                .await
                .wrap_err("Failed to create testbed")
        }
        RemoteTestbedCommand::Start { instances } => {
            with_spinner("Booting instances", testbed.start(instances))
                .await
                .wrap_err("Failed to start testbed")
        }
        RemoteTestbedCommand::Stop => with_spinner("Stopping instances", testbed.stop())
            .await
            .wrap_err("Failed to stop testbed"),
        RemoteTestbedCommand::Destroy => with_spinner("Destroying testbed", testbed.destroy())
            .await
            .wrap_err("Failed to destroy testbed"),
        RemoteTestbedCommand::Benchmark {
            committee,
            loads,
            skip_testbed_update,
            skip_testbed_configuration,
        } => {
            benchmark(
                testbed,
                settings,
                committee,
                loads,
                skip_testbed_update,
                skip_testbed_configuration,
            )
            .await
        }
    }
}

fn print_status(status: &TestbedStatus) {
    eprintln!("Client: {}", status.client_summary);
    eprintln!(
        "Repo: {} ({})",
        status.repository_url, status.repository_commit
    );
    eprintln!("Instances active: {}", status.active_count);
    for region in &status.regions {
        eprintln!();
        eprintln!("[{}]", region.region.to_uppercase());
        if region.instances.is_empty() {
            eprintln!("  (no instances)");
            continue;
        }
        for (index, instance) in region.instances.iter().enumerate() {
            let marker = if instance.active { "●" } else { "○" };
            eprintln!("  {marker} {index:>2}  {}", instance.connect_command);
        }
    }
}

async fn benchmark<C: ServerProviderClient>(
    testbed: Testbed<C>,
    settings: Settings,
    committee: usize,
    loads: Vec<usize>,
    skip_testbed_update: bool,
    skip_testbed_configuration: bool,
) -> Result<()> {
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

    let protocol_commands = MysticetiProtocol::new(&settings);
    let node_parameters = match &settings.node_parameters_path {
        Some(path) => {
            MysticetiNodeParameters::load(path).wrap_err("Failed to load node's parameters")?
        }
        None => MysticetiNodeParameters::default(),
    };
    let client_parameters = match &settings.client_parameters_path {
        Some(path) => {
            MysticetiClientParameters::load(path).wrap_err("Failed to load client's parameters")?
        }
        None => MysticetiClientParameters::default(),
    };

    // Skip-flag side effects live in the caller: warn the user, and pin
    // `unknown_commit` to the repository before the orchestrator snapshots the
    // settings into per-benchmark parameters.
    let mut settings = settings;
    if skip_testbed_update {
        eprintln!("WARNING: skipping testbed update! Use with care.");
        settings.repository.set_unknown_commit();
    }
    if skip_testbed_configuration {
        eprintln!("WARNING: skipping testbed configuration! Use with care.");
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
            ("Commit", &settings.repository.commit),
        ],
    )
    .print();

    let parameters_set = BenchmarkParameters::new_from_loads(
        settings.clone(),
        node_parameters,
        client_parameters,
        committee,
        loads,
    );
    let total = parameters_set.len();

    let orchestrator = Orchestrator::new(
        settings.clone(),
        instances,
        setup_commands,
        protocol_commands,
        ssh_manager,
    );

    with_spinner("Cleaning up testbed", orchestrator.cleanup(true))
        .await
        .wrap_err("Cleanup failed")?;

    if !skip_testbed_update {
        with_spinner(
            "Installing dependencies on all machines",
            orchestrator.install(),
        )
        .await
        .wrap_err("Install failed")?;
        with_spinner("Updating all instances", orchestrator.update())
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

        with_spinner("Cleaning up testbed", orchestrator.cleanup(true))
            .await
            .wrap_err("Cleanup failed")?;

        if settings.monitoring {
            let monitoring = with_spinner(
                "Configuring monitoring instance",
                orchestrator.start_monitoring(&parameters),
            )
            .await
            .wrap_err("Monitoring setup failed")?;
            if let Some(report) = &monitoring {
                eprintln!("Grafana: {}", report.grafana_address);
            }
            run_one(
                &orchestrator,
                &settings,
                &parameters,
                monitoring.as_ref(),
                &mut latest_committee_size,
                skip_testbed_configuration,
            )
            .await?;
        } else {
            // Skip the spinner entirely when monitoring is disabled: the call
            // is a no-op SSH-wise but still needs to run to keep the rest of
            // the loop uniform.
            let monitoring = orchestrator
                .start_monitoring(&parameters)
                .await
                .wrap_err("Monitoring setup failed")?;
            run_one(
                &orchestrator,
                &settings,
                &parameters,
                monitoring.as_ref(),
                &mut latest_committee_size,
                skip_testbed_configuration,
            )
            .await?;
        }

        with_spinner("Cleaning up testbed", orchestrator.cleanup(false))
            .await
            .wrap_err("Cleanup failed")?;

        if settings.log_processing {
            let logs = with_spinner("Downloading logs", orchestrator.download_logs(&parameters))
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

#[allow(clippy::too_many_arguments)]
async fn run_one<P: ProtocolCommands + ProtocolMetrics>(
    orchestrator: &Orchestrator<P>,
    settings: &Settings,
    parameters: &Parameters<P>,
    monitoring: Option<&MonitoringReport>,
    latest_committee_size: &mut usize,
    skip_testbed_configuration: bool,
) -> Result<()> {
    if !skip_testbed_configuration && *latest_committee_size != parameters.nodes {
        let configure_report =
            with_spinner("Configuring instances", orchestrator.configure(parameters))
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

    with_spinner("Deploying replicas", orchestrator.run_nodes(parameters))
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
    with_spinner(load_label, orchestrator.run_clients(parameters))
        .await
        .wrap_err("Starting load generators failed")?;

    run_benchmark_loop(orchestrator, settings, parameters, monitoring).await
}

/// Drive the metrics + faults tick loop for a single benchmark run. Owns the
/// `tokio::select!`, the fault schedule, and (when monitoring is deployed) a
/// PromQL [`Collector`] that polls Prometheus on the metrics tick. The
/// orchestrator never touches stdout or local files — that is caller policy.
async fn run_benchmark_loop<P: ProtocolCommands + ProtocolMetrics>(
    orchestrator: &Orchestrator<P>,
    settings: &Settings,
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
    metrics_interval.tick().await; // First tick returns immediately.
    let mut faults_interval = time::interval(parameters.settings.faults.crash_interval());
    faults_interval.tick().await; // First tick returns immediately.

    // Per-commit subdirectory so re-running the same benchmark parameters
    // against different commits doesn't overwrite previous results.
    let results_path = settings
        .results_dir
        .join(format!("results-{}", settings.repository.commit));
    std::fs::create_dir_all(&results_path)
        .map_err(MonitorError::ResultsWriteError)
        .wrap_err("Failed to create results directory")?;

    let mut progress = Progress::new(stderr_supports_color());
    progress.start(Some(benchmark_duration), &label);
    let start = Instant::now();
    let outcome = loop {
        tokio::select! {
            now = metrics_interval.tick() => {
                let elapsed = now.duration_since(start);
                progress.set_elapsed(elapsed);
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
                let action = match orchestrator.apply_faults_step(parameters, &mut schedule).await {
                    Ok(action) => action,
                    Err(error) => {
                        progress.stop();
                        return Err(error).wrap_err("Failed to apply fault schedule");
                    }
                };
                if !action.kill.is_empty() || !action.boot.is_empty() {
                    progress.suspend(|| eprintln!("Testbed update: {action}"));
                }
            }
        }
        // Pace the bar even when the loop spins on faults rather than metrics.
        progress.set_elapsed(
            start
                .elapsed()
                .min(benchmark_duration + Duration::from_secs(1)),
        );
    };
    progress.stop();
    outcome.wrap_err("Metrics collection failed")
}
