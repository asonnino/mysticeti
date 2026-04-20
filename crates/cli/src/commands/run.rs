// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dag::{authority::Authority, config::ImportExport, context::TokioCtx};
use eyre::{Result, eyre};
use replica::{
    builder::ReplicaBuilder,
    config::{LoadGeneratorConfig, PrivateReplicaConfig, PublicReplicaConfig},
    prometheus::{MetricsRegistry, PrometheusServer},
};
use tracing_subscriber::filter::LevelFilter;

use crate::tracing::ReplicaTracing;

pub async fn run(
    authority: Authority,
    public_config_path: String,
    private_config_path: String,
    load_generator_config_path: Option<String>,
    log_level: Option<LevelFilter>,
) -> Result<()> {
    match log_level {
        Some(level) => ReplicaTracing::new(level),
        None => ReplicaTracing::default(),
    }
    .setup();
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
    if let Some(config) = load_generator_config {
        handle.start_load_generator(config);
    }

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
    }
}
