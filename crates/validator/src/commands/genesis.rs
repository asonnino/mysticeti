// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fs, net::IpAddr, path::PathBuf};

use dag::{
    committee::Committee,
    config::{ImportExport, NodeParameters, NodePrivateConfig, NodePublicConfig},
    types::AuthorityIndex,
};
use eyre::{Context, Result};

pub fn benchmark_genesis(
    ips: Vec<IpAddr>,
    working_directory: PathBuf,
    node_parameters_path: Option<PathBuf>,
) -> Result<()> {
    tracing::info!("Generating benchmark genesis files");
    fs::create_dir_all(&working_directory).wrap_err(format!(
        "Failed to create directory '{}'",
        working_directory.display()
    ))?;

    let committee_size = ips.len();
    let mut committee_path = working_directory.clone();
    committee_path.push(Committee::DEFAULT_FILENAME);
    Committee::new_for_benchmarks(committee_size)
        .print(&committee_path)
        .wrap_err("Failed to print committee file")?;
    tracing::info!("Generated committee file: {}", committee_path.display());

    let node_parameters = match node_parameters_path {
        Some(path) => NodeParameters::load(&path).wrap_err(format!(
            "Failed to load parameters file '{}'",
            path.display()
        ))?,
        None => NodeParameters::default(),
    };

    let node_public_config = NodePublicConfig::new_for_benchmarks(ips, Some(node_parameters));
    let mut node_public_config_path = working_directory.clone();
    node_public_config_path.push(NodePublicConfig::DEFAULT_FILENAME);
    node_public_config
        .print(&node_public_config_path)
        .wrap_err("Failed to print parameters file")?;
    tracing::info!(
        "Generated public node config file: {}",
        node_public_config_path.display()
    );

    let node_private_configs =
        NodePrivateConfig::new_for_benchmarks(&working_directory, committee_size);
    for (i, private_config) in node_private_configs.into_iter().enumerate() {
        fs::create_dir_all(&private_config.storage_path)
            .expect("Failed to create storage directory");
        let path = working_directory.join(NodePrivateConfig::default_filename(i as AuthorityIndex));
        private_config
            .print(&path)
            .wrap_err("Failed to print private config file")?;
        tracing::info!("Generated private config file: {}", path.display());
    }

    Ok(())
}
