// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};

use ::prometheus::Registry;
use dag::{
    committee::Committee,
    config::{ClientParameters, ImportExport, NodePrivateConfig, NodePublicConfig},
    types::AuthorityIndex,
};
use eyre::{Result, eyre};
use tracing_subscriber::filter::LevelFilter;

use crate::{tracing::ValidatorTracing, validator::ValidatorBuilder};

pub async fn run(
    authority: AuthorityIndex,
    committee_path: String,
    public_config_path: String,
    private_config_path: String,
    client_parameters_path: String,
    log_level: Option<LevelFilter>,
) -> Result<()> {
    match log_level {
        Some(level) => ValidatorTracing::new(level),
        None => ValidatorTracing::default(),
    }
    .setup();
    tracing::info!("Starting validator {authority}");

    // Load all configuration files.
    let committee = Committee::load(&committee_path)?;
    let public_config = NodePublicConfig::load(&public_config_path)?;
    let private_config = NodePrivateConfig::load(&private_config_path)?;
    let client_parameters = ClientParameters::load(&client_parameters_path)?;

    // Resolve the metrics address for this authority and bind to
    // 0.0.0.0 so the server is reachable from outside the host.
    let metrics_address = public_config
        .metrics_address(authority)
        .ok_or(eyre!("No metrics address for authority {authority}"))?;
    let mut binding_address = metrics_address;
    binding_address.set_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
    tracing::info!("Metrics server listening on {metrics_address}");

    let network_address = public_config
        .network_address(authority)
        .ok_or(eyre!("No network address for authority {authority}"))?;

    // Start the validator with metrics and block forever.
    let validator = ValidatorBuilder::new(
        authority,
        Arc::new(committee),
        public_config,
        private_config,
        client_parameters,
        Registry::new(),
    )
    .with_metrics_server(binding_address)
    .start()
    .await?;
    tracing::info!("Validator {authority} listening on {network_address}");

    validator.await_completion().await
}
