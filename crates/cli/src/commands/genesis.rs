// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fs, path::PathBuf};

use dag::{authority::Authority, config::ImportExport};
use eyre::{Context, Result};
use tracing_subscriber::filter::LevelFilter;

use replica::config::{PrivateReplicaConfig, PublicReplicaConfig, ReplicaParameters};

use crate::{args::TestGenesisArgs, tracing::ReplicaTracing};

pub fn test_genesis(
    args: TestGenesisArgs,
    log_level: Option<LevelFilter>,
    log_file: Option<PathBuf>,
) -> Result<()> {
    let _guard = match log_level {
        Some(level) => ReplicaTracing::new(level),
        None => ReplicaTracing::default(),
    }
    .with_log_file(log_file)
    .setup()?;

    let TestGenesisArgs {
        ips,
        working_directory,
        replica_parameters_path,
    } = args;
    let committee_size = ips.len();
    tracing::info!("Generating test genesis for {committee_size} replicas");

    // Create the output directory for all genesis files.
    fs::create_dir_all(&working_directory).wrap_err(format!(
        "Failed to create working directory '{}'",
        working_directory.display()
    ))?;

    // Load custom replica parameters or fall back to defaults.
    let parameters = match replica_parameters_path {
        Some(path) => {
            tracing::info!("Loading replica parameters from {}", path.display());
            ReplicaParameters::load(&path)?
        }
        None => {
            tracing::info!("Using default replica parameters");
            ReplicaParameters::default()
        }
    };

    // Generate the public replica config: parameters + per-replica
    // identities (public key, stake, network/metrics addresses). The
    // committee used by consensus is derived from this file at runtime.
    let public_config = PublicReplicaConfig::new_for_benchmarks(ips).with_parameters(parameters);
    let public_config_path = working_directory.join(PublicReplicaConfig::DEFAULT_FILENAME);
    public_config.print(&public_config_path)?;
    tracing::info!("Wrote {}", public_config_path.display());

    // Generate one private config per replica (keys, storage path).
    let private_configs =
        PrivateReplicaConfig::new_for_benchmarks(&working_directory, committee_size);
    for (i, private_config) in private_configs.into_iter().enumerate() {
        let authority = Authority::from(i);
        fs::create_dir_all(&private_config.storage_path).wrap_err(format!(
            "Failed to create storage directory for replica {authority}"
        ))?;
        let path = working_directory.join(PrivateReplicaConfig::default_filename(authority));
        private_config.print(&path)?;
        tracing::info!("Wrote {}", path.display());
    }

    tracing::info!(
        "Test genesis for {committee_size} replicas ready in '{}'",
        working_directory.display()
    );
    Ok(())
}
