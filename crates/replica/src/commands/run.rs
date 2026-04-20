// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dag::{authority::Authority, config::ImportExport};
use eyre::{Result, eyre};
use tracing_subscriber::filter::LevelFilter;

use crate::{
    builder::ReplicaBuilder,
    config::{LoadGeneratorConfig, PrivateReplicaConfig, PublicReplicaConfig},
    tracing::ReplicaTracing,
};

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

    // Load configuration files.
    let public_config = PublicReplicaConfig::load(&public_config_path)?;
    let private_config = PrivateReplicaConfig::load(&private_config_path)?;
    let load_generator_config = load_generator_config_path
        .map(|path| LoadGeneratorConfig::load(&path))
        .transpose()?;

    let metrics_address = public_config
        .metrics_address(authority)
        .ok_or(eyre!("No metrics address for authority {authority}"))?;
    let network_address = public_config
        .network_address(authority)
        .ok_or(eyre!("No network address for authority {authority}"))?;

    // Build and run the replica (blocks forever).
    let mut builder =
        ReplicaBuilder::new(authority, public_config, private_config).with_metrics_server();
    if let Some(config) = load_generator_config {
        builder = builder.with_load_generator(config);
    }
    let handle = builder.build().run().await?;

    tracing::info!("Metrics server listening on {metrics_address}");
    tracing::info!("Replica {authority} listening on {network_address}");

    handle.join().await
}
