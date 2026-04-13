// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs,
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
};

use dag::{
    committee::Committee,
    config::{ClientParameters, ImportExport, NodeParameters, NodePrivateConfig, NodePublicConfig},
    types::AuthorityIndex,
};
use eyre::{Context, Result};
use serde::{Deserialize, Serialize};
use tracing_subscriber::filter::LevelFilter;

use crate::{banner, builder::ReplicaBuilder, tracing::ReplicaTracing};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestbedConfig {
    pub committee_size: usize,
    pub node_parameters: NodeParameters,
    pub client_parameters: ClientParameters,
}

impl Default for TestbedConfig {
    fn default() -> Self {
        Self {
            committee_size: 4,
            node_parameters: NodeParameters::default(),
            client_parameters: ClientParameters::default(),
        }
    }
}

impl ImportExport for TestbedConfig {}

pub async fn local_testbed(
    config_path: Option<PathBuf>,
    dump_config: bool,
    log_level: Option<LevelFilter>,
) -> Result<()> {
    if dump_config {
        println!("{}", TestbedConfig::default().to_yaml());
        return Ok(());
    }

    banner::print_banner("Local Testbed");
    match log_level {
        Some(level) => ReplicaTracing::new(level),
        None => ReplicaTracing::new(LevelFilter::DEBUG),
    }
    .setup();

    let config = match config_path {
        Some(path) => {
            tracing::info!("Loading testbed config from {}", path.display());
            TestbedConfig::load(&path)?
        }
        None => {
            tracing::info!("Using default testbed config");
            TestbedConfig::default()
        }
    };

    let committee_size = config.committee_size;
    let ips = vec![IpAddr::V4(Ipv4Addr::LOCALHOST); committee_size];
    let committee = Committee::new_for_benchmarks(committee_size);
    let public_config = NodePublicConfig::new_for_benchmarks(ips, Some(config.node_parameters));

    let working_dir = PathBuf::from("local-testbed");
    match fs::remove_dir_all(&working_dir) {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(e).wrap_err(format!(
                "Failed to remove directory '{}'",
                working_dir.display()
            ));
        }
    }

    let private_configs = NodePrivateConfig::new_for_benchmarks(&working_dir, committee_size);
    for private_config in &private_configs {
        fs::create_dir_all(&private_config.storage_path).wrap_err(format!(
            "Failed to create directory '{}'",
            private_config.storage_path.display()
        ))?;
    }

    tracing::info!("Starting local testbed with {committee_size} validators");

    let mut handles = Vec::with_capacity(committee_size);
    for (i, private_config) in private_configs.into_iter().enumerate() {
        let authority = i as AuthorityIndex;
        let mut builder = ReplicaBuilder::new(
            authority,
            committee.clone(),
            public_config.clone(),
            private_config,
        )
        .with_load_generator(config.client_parameters.clone());

        if let Some(address) = public_config.metrics_address(authority) {
            builder = builder.with_metrics_server(address);
        }

        handles.push(builder.build().run().await?);
    }

    tracing::info!("All {committee_size} validators running. Press Ctrl-C to stop.");

    tokio::signal::ctrl_c()
        .await
        .wrap_err("Failed to listen for Ctrl-C")?;

    tracing::info!("Shutting down...");
    drop(handles);

    Ok(())
}
