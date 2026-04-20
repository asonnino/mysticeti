// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::VecDeque, fs, net::SocketAddr, time::Duration};

use tempfile::TempDir;
use tokio::time;

use dag::{authority::Authority, context::TokioCtx};
use replica::{
    builder::ReplicaBuilder,
    config::{LoadGeneratorConfig, PrivateReplicaConfig, PublicReplicaConfig},
    prometheus::{self, MetricsRegistry, PrometheusServer},
    replica::ReplicaHandle,
};
use tokio::task::JoinHandle;

async fn run_replica(
    authority: Authority,
    public_config: &PublicReplicaConfig,
    private_config: PrivateReplicaConfig,
    load_generator_config: &LoadGeneratorConfig,
) -> (ReplicaHandle<TokioCtx>, JoinHandle<()>) {
    let metrics_address = public_config
        .metrics_address(authority)
        .expect("metrics address must exist");
    let registry = MetricsRegistry::new();
    let mut handle = ReplicaBuilder::new(authority, public_config.clone(), private_config)
        .with_registry(registry.clone())
        .build()
        .run::<TokioCtx>()
        .await
        .unwrap();
    handle.start_load_generator(load_generator_config.clone());
    let metrics_server = PrometheusServer::new(metrics_address, &registry)
        .bind_all_interfaces()
        .start()
        .await
        .expect("metrics server bind failed");
    (handle, metrics_server)
}

async fn check_commit(address: &SocketAddr) -> Result<bool, reqwest::Error> {
    let route = prometheus::METRICS_ROUTE;
    let res = reqwest::get(format!("http://{address}{route}")).await?;
    let string = res.text().await?;
    let commit = string.contains("committed_leaders_total");
    Ok(commit)
}

async fn await_for_commits(addresses: Vec<SocketAddr>) {
    let mut queue = VecDeque::from(addresses);
    while let Some(address) = queue.pop_front() {
        time::sleep(Duration::from_millis(100)).await;
        match check_commit(&address).await {
            Ok(commits) if commits => (),
            _ => queue.push_back(address),
        }
    }
}

#[tokio::test]
async fn replica_commit() {
    let committee_size = 4;
    let public_config = PublicReplicaConfig::new_for_tests(committee_size).with_port_offset(0);
    let load_generator_config = LoadGeneratorConfig::default();

    let dir = TempDir::new().unwrap();
    let private_configs = PrivateReplicaConfig::new_for_benchmarks(dir.as_ref(), committee_size);
    private_configs.iter().for_each(|private_config| {
        fs::create_dir_all(&private_config.storage_path).unwrap();
    });

    let mut handles = Vec::new();
    for (i, private_config) in private_configs.into_iter().enumerate() {
        handles.push(
            run_replica(
                Authority::from(i),
                &public_config,
                private_config,
                &load_generator_config,
            )
            .await,
        );
    }

    let addresses = public_config
        .all_metric_addresses()
        .map(|address| address.to_owned())
        .collect();
    let timeout = Duration::from_secs(5);

    tokio::select! {
        _ = await_for_commits(addresses) => (),
        _ = time::sleep(timeout) => {
            panic!(
                "Failed to gather commits within a few timeouts"
            )
        },
    }
}

#[tokio::test]
async fn replica_sync() {
    let committee_size = 4;
    let public_config = PublicReplicaConfig::new_for_tests(committee_size).with_port_offset(100);
    let load_generator_config = LoadGeneratorConfig::default();

    let dir = TempDir::new().unwrap();
    let private_configs = PrivateReplicaConfig::new_for_benchmarks(dir.as_ref(), committee_size);
    private_configs.iter().for_each(|private_config| {
        fs::create_dir_all(&private_config.storage_path).unwrap();
    });

    let mut handles = Vec::new();
    for (i, private_config) in private_configs.into_iter().enumerate() {
        if i == 0 {
            continue;
        }
        handles.push(
            run_replica(
                Authority::from(i),
                &public_config,
                private_config,
                &load_generator_config,
            )
            .await,
        );
    }

    let addresses = public_config
        .all_metric_addresses()
        .skip(1)
        .map(|address| address.to_owned())
        .collect();
    let timeout = Duration::from_secs(5);
    tokio::select! {
        _ = await_for_commits(addresses) => (),
        _ = time::sleep(timeout) => {
            panic!(
                "Failed to gather commits within a few timeouts"
            )
        },
    }

    let authority = 0;
    let private_config =
        PrivateReplicaConfig::new_for_benchmarks(dir.as_ref(), committee_size).remove(authority);
    handles.push(
        run_replica(
            Authority::from(authority),
            &public_config,
            private_config,
            &load_generator_config,
        )
        .await,
    );

    let address = public_config
        .all_metric_addresses()
        .next()
        .map(|address| address.to_owned())
        .unwrap();
    let timeout = Duration::from_secs(5);
    tokio::select! {
        _ = await_for_commits(vec![address]) => (),
        _ = time::sleep(timeout) => {
            panic!(
                "Failed to gather commits within a few timeouts"
            )
        },
    }
}

#[tokio::test]
async fn replica_crash_faults() {
    let committee_size = 4;
    let public_config = PublicReplicaConfig::new_for_tests(committee_size).with_port_offset(200);
    let load_generator_config = LoadGeneratorConfig::default();

    let dir = TempDir::new().unwrap();
    let private_configs = PrivateReplicaConfig::new_for_benchmarks(dir.as_ref(), committee_size);
    private_configs.iter().for_each(|private_config| {
        fs::create_dir_all(&private_config.storage_path).unwrap();
    });

    let mut handles = Vec::new();
    for (i, private_config) in private_configs.into_iter().enumerate() {
        if i == 0 {
            continue;
        }
        handles.push(
            run_replica(
                Authority::from(i),
                &public_config,
                private_config,
                &load_generator_config,
            )
            .await,
        );
    }

    let addresses = public_config
        .all_metric_addresses()
        .skip(1)
        .map(|address| address.to_owned())
        .collect();
    let timeout = Duration::from_secs(15);

    tokio::select! {
        _ = await_for_commits(addresses) => (),
        _ = time::sleep(timeout) => {
            panic!(
                "Failed to gather commits within a few timeouts"
            )
        },
    }
}
