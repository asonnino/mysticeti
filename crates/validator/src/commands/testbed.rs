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

use crate::validator::Validator;

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

pub async fn local_testbed(config_path: Option<PathBuf>, dump_config: bool) -> Result<()> {
    if dump_config {
        let config = TestbedConfig::default();
        let yaml = serde_yaml::to_string(&config).map_err(eyre::Report::msg)?;
        println!("{yaml}");
        return Ok(());
    }

    let config = match config_path {
        Some(path) => TestbedConfig::load(&path).wrap_err("Failed to load testbed config")?,
        None => TestbedConfig::default(),
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

    let mut validators = Vec::with_capacity(committee_size);
    for (i, private_config) in private_configs.into_iter().enumerate() {
        let authority = i as AuthorityIndex;
        let validator = Validator::start(
            authority,
            committee.clone(),
            public_config.clone(),
            private_config,
            config.client_parameters.clone(),
        )
        .await?;
        validators.push(validator);
    }

    tracing::info!("All {committee_size} validators running. Press Ctrl-C to stop.");

    tokio::signal::ctrl_c()
        .await
        .wrap_err("Failed to listen for Ctrl-C")?;

    tracing::info!("Shutting down...");
    drop(validators);

    Ok(())
}
