// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};

use dag::{
    committee::Committee,
    config::{ClientParameters, ImportExport, NodePrivateConfig, NodePublicConfig},
    types::Authority,
};
use eyre::{Result, eyre};
use tracing_subscriber::filter::LevelFilter;

use crate::{builder::ReplicaBuilder, tracing::ReplicaTracing};

pub async fn run(
    authority: u64,
    committee_path: String,
    public_config_path: String,
    private_config_path: String,
    client_parameters_path: String,
    log_level: Option<LevelFilter>,
) -> Result<()> {
    match log_level {
        Some(level) => ReplicaTracing::new(level),
        None => ReplicaTracing::default(),
    }
    .setup();
    let authority = Authority::new(authority);
    tracing::info!("Starting replica {authority}");

    // Load all configuration files.
    let committee = Committee::load(&committee_path)?;
    let public_config = NodePublicConfig::load(&public_config_path)?;
    let private_config = NodePrivateConfig::load(&private_config_path)?;
    let client_parameters = ClientParameters::load(&client_parameters_path)?;

    // Resolve the metrics address for this authority and bind
    // to 0.0.0.0 so the server is reachable from outside.
    let metrics_address = public_config
        .metrics_address(authority)
        .ok_or(eyre!("No metrics address for authority {authority}"))?;
    let mut binding_address = metrics_address;
    binding_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

    let network_address = public_config
        .network_address(authority)
        .ok_or(eyre!("No network address for authority {authority}"))?;

    // Build and run the replica (blocks forever).
    let handle = ReplicaBuilder::new(
        authority,
        Arc::new(committee),
        public_config,
        private_config,
    )
    .with_load_generator(client_parameters)
    .with_metrics_server(binding_address)
    .build()
    .run()
    .await?;

    tracing::info!("Metrics server listening on {metrics_address}");
    tracing::info!("Replica {authority} listening on {network_address}");

    handle.join().await
}
