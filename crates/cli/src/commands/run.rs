// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::PathBuf;

use dag::{config::ImportExport, context::TokioCtx};
use eyre::{Result, eyre};
use replica::{
    builder::ReplicaBuilder,
    config::{LoadGeneratorConfig, PrivateReplicaConfig, PublicReplicaConfig},
    prometheus::{MetricsRegistry, PrometheusServer},
};
use tracing_subscriber::filter::LevelFilter;

use crate::{args::RunArgs, tracing::ReplicaTracing};

pub async fn run(
    args: RunArgs,
    log_level: Option<LevelFilter>,
    log_file: Option<PathBuf>,
) -> Result<()> {
    let _guard = match log_level {
        Some(level) => ReplicaTracing::new(level),
        None => ReplicaTracing::default(),
    }
    .with_log_file(log_file)
    .setup()?;

    let RunArgs {
        authority,
        public_config_path,
        private_config_path,
        load_generator_config_path,
    } = args;
    tracing::info!("Starting replica {authority}");

    // Load configuration from YAML.
    let public_config = PublicReplicaConfig::load(&public_config_path)?;
    let private_config = PrivateReplicaConfig::load(&private_config_path)?;
    let load_generator_config = load_generator_config_path
        .map(|path| LoadGeneratorConfig::load(&path))
        .transpose()?;

    // Resolve this authority's network and metrics addresses from the public config.
    let metrics_address = public_config
        .metrics_address(authority)
        .ok_or_else(|| eyre!("No metrics address for authority {authority}"))?;
    let network_address = public_config
        .network_address(authority)
        .ok_or_else(|| eyre!("No network address for authority {authority}"))?;

    // Build and start the replica on tokio. The registry is shared with the metrics HTTP server
    // below.
    let registry = MetricsRegistry::new();
    let mut handle = ReplicaBuilder::new(authority, public_config, private_config)
        .with_registry(registry.clone())
        .build()
        .run::<TokioCtx>()
        .await?;
    // Keep the generator task bound to the command's scope so it isn't dropped (detached) mid-run.
    let load_generator = load_generator_config.map(|config| handle.start_load_generator(config));

    // Expose metrics over HTTP on all interfaces for external scraping.
    let metrics_server = PrometheusServer::new(metrics_address, &registry)
        .bind_all_interfaces()
        .start()
        .await?;

    tracing::info!("Metrics server listening on {metrics_address}");
    tracing::info!("Replica {authority} listening on {network_address}");

    // Wait for either the replica or the metrics server to finish; surface whichever crashes first.
    tokio::select! {
        result = handle.await_completion() => result,
        result = metrics_server => result.map_err(|error| eyre!("Metrics server crashed: {error}")),
        result = async {
            match load_generator {
                Some(task) => match task.await {
                    Ok(()) => Err(eyre!("Load generator stopped unexpectedly")),
                    Err(error) => Err(eyre!("Load generator crashed: {error}")),
                },
                None => std::future::pending::<Result<()>>().await,
            }
        } => result,
    }
}
