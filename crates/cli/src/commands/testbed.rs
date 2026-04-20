// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs,
    net::{IpAddr, Ipv4Addr},
    path::PathBuf,
};

use dag::{authority::Authority, config::ImportExport, context::TokioCtx};
use eyre::{Context, Result};
use replica::{
    builder::ReplicaBuilder,
    config::{LoadGeneratorConfig, PrivateReplicaConfig, PublicReplicaConfig, ReplicaParameters},
    prometheus::{MetricsRegistry, PrometheusServer},
};
use tracing_subscriber::filter::LevelFilter;

use crate::{banner, tracing::ReplicaTracing};

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

    // Load optional parameter overrides; fall back to defaults.
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

    // Print the startup banner.
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

    // Generate a localhost public config with the chosen parameters.
    let ips = vec![IpAddr::V4(Ipv4Addr::LOCALHOST); committee_size];
    let public_config =
        PublicReplicaConfig::new_for_benchmarks(ips).with_parameters(replica_parameters);

    // Prepare a clean working directory for the validators' WAL files.
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

    // Spin up each validator: run the replica, start its load generator, expose its metrics.
    // Each validator gets its own registry and metrics server on a distinct port.
    let mut handles = Vec::with_capacity(committee_size);
    let mut metrics_servers = Vec::with_capacity(committee_size);
    for (i, private_config) in private_configs.into_iter().enumerate() {
        let authority = Authority::from(i);
        let metrics_address = public_config
            .metrics_address(authority)
            .expect("metrics address must exist");
        let registry = MetricsRegistry::new();
        let mut handle = ReplicaBuilder::new(authority, public_config.clone(), private_config)
            .with_registry(registry.clone())
            .build()
            .run::<TokioCtx>()
            .await?;
        handle.start_load_generator(load_generator_config.clone());
        metrics_servers.push(
            PrometheusServer::new(metrics_address, &registry)
                .bind_all_interfaces()
                .start()
                .await?,
        );
        handles.push(handle);
    }

    tracing::info!("All {committee_size} validators running. Press Ctrl-C to stop.");

    // Block until the user interrupts, then tear everything down.
    tokio::signal::ctrl_c()
        .await
        .wrap_err("Failed to listen for Ctrl-C")?;

    tracing::info!("Shutting down...");
    drop(handles);
    drop(metrics_servers);

    Ok(())
}
