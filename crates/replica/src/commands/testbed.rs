// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs,
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
};

use dag::{authority::Authority, config::ImportExport};
use eyre::{Context, Result};
use tracing_subscriber::filter::LevelFilter;

use crate::{
    banner,
    builder::ReplicaBuilder,
    config::{LoadGeneratorConfig, PrivateReplicaConfig, PublicReplicaConfig, ReplicaParameters},
    tracing::ReplicaTracing,
};

pub async fn local_testbed(
    committee_size: usize,
    replica_parameters_path: Option<PathBuf>,
    load_generator_config_path: Option<PathBuf>,
    log_level: Option<LevelFilter>,
) -> Result<()> {
    match log_level {
        Some(level) => ReplicaTracing::new(level),
        None => ReplicaTracing::new(LevelFilter::DEBUG),
    }
    .setup();

    let replica_parameters = match replica_parameters_path {
        Some(path) => {
            tracing::info!("Loading replica parameters from {}", path.display());
            ReplicaParameters::load(&path)?
        }
        None => ReplicaParameters::default(),
    };
    let load_generator_config = match load_generator_config_path {
        Some(path) => {
            tracing::info!("Loading load generator config from {}", path.display());
            LoadGeneratorConfig::load(&path)?
        }
        None => LoadGeneratorConfig::default(),
    };

    let nodes = committee_size.to_string();
    let tx_size = load_generator_config.transaction_size.to_string();
    let load_str = load_generator_config.load.to_string();
    banner::BannerPrinter::new(
        "Mysticeti",
        &[
            ("Mode", "Local Testbed"),
            ("Nodes", &nodes),
            ("Tx size", &tx_size),
            ("Load", &load_str),
        ],
    )
    .print();

    let ips = vec![IpAddr::V4(Ipv4Addr::LOCALHOST); committee_size];
    let public_config =
        PublicReplicaConfig::new_for_benchmarks(ips).with_parameters(replica_parameters);

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

    let private_configs = PrivateReplicaConfig::new_for_benchmarks(&working_dir, committee_size);
    for private_config in &private_configs {
        fs::create_dir_all(&private_config.storage_path).wrap_err(format!(
            "Failed to create directory '{}'",
            private_config.storage_path.display()
        ))?;
    }

    tracing::info!("Starting local testbed with {committee_size} validators");

    let mut handles = Vec::with_capacity(committee_size);
    for (i, private_config) in private_configs.into_iter().enumerate() {
        let authority = Authority::from(i);
        let builder = ReplicaBuilder::new(authority, public_config.clone(), private_config)
            .with_metrics_server()
            .with_load_generator(load_generator_config.clone());
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
