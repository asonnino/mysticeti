// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fs, net::IpAddr, path::PathBuf};

use dag::{
    committee::Committee,
    config::{ImportExport, NodeParameters, NodePrivateConfig, NodePublicConfig},
    types::AuthorityIndex,
};
use eyre::{Context, Result};
use tracing_subscriber::filter::LevelFilter;

use crate::tracing::ReplicaTracing;

pub fn test_genesis(
    ips: Vec<IpAddr>,
    working_directory: PathBuf,
    node_parameters_path: Option<PathBuf>,
    log_level: Option<LevelFilter>,
) -> Result<()> {
    match log_level {
        Some(level) => ReplicaTracing::new(level),
        None => ReplicaTracing::default(),
    }
    .setup();

    let committee_size = ips.len();
    tracing::info!("Generating test genesis for {committee_size} validators");

    // Create the output directory for all genesis files.
    fs::create_dir_all(&working_directory).wrap_err(format!(
        "Failed to create working directory '{}'",
        working_directory.display()
    ))?;

    // Generate the committee file (maps authorities to stakes).
    let committee = Committee::new_for_benchmarks(committee_size);
    let committee_path = working_directory.join(Committee::DEFAULT_FILENAME);
    committee.print(&committee_path)?;
    tracing::info!("Wrote {}", committee_path.display());

    // Load custom node parameters or fall back to defaults.
    let node_parameters = match node_parameters_path {
        Some(path) => {
            tracing::info!("Loading node parameters from {}", path.display());
            NodeParameters::load(&path)?
        }
        None => {
            tracing::info!("Using default node parameters");
            NodeParameters::default()
        }
    };

    // Generate the public config (network addresses, parameters).
    let public_config = NodePublicConfig::new_for_benchmarks(ips, Some(node_parameters));
    let public_config_path = working_directory.join(NodePublicConfig::DEFAULT_FILENAME);
    public_config.print(&public_config_path)?;
    tracing::info!("Wrote {}", public_config_path.display());

    // Generate one private config per validator (keys, storage path).
    let private_configs = NodePrivateConfig::new_for_benchmarks(&working_directory, committee_size);
    for (i, private_config) in private_configs.into_iter().enumerate() {
        let authority = i as AuthorityIndex;
        fs::create_dir_all(&private_config.storage_path).wrap_err(format!(
            "Failed to create storage directory for validator {authority}"
        ))?;
        let path = working_directory.join(NodePrivateConfig::default_filename(authority));
        private_config.print(&path)?;
        tracing::info!("Wrote {}", path.display());
    }

    tracing::info!(
        "Test genesis for {committee_size} validators ready in '{}'",
        working_directory.display()
    );
    Ok(())
}
