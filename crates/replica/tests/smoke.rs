// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::VecDeque, fs, net::SocketAddr, sync::Arc, time::Duration};

use tempfile::TempDir;
use tokio::time;

use dag::{
    authority::Authority,
    committee::Committee,
    config::{ClientParameters, NodePrivateConfig, NodePublicConfig},
};
use replica::{builder::ReplicaBuilder, prometheus, replica::ReplicaHandle};

async fn run_replica(
    authority: Authority,
    committee: &Arc<Committee>,
    public_config: &NodePublicConfig,
    private_config: NodePrivateConfig,
    client_parameters: &ClientParameters,
) -> ReplicaHandle {
    let mut builder = ReplicaBuilder::new(
        authority,
        committee.clone(),
        public_config.clone(),
        private_config,
    )
    .with_load_generator(client_parameters.clone());

    if let Some(address) = public_config.metrics_address(authority) {
        builder = builder.with_metrics_server(address);
    }

    builder.build().run().await.unwrap()
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
    let committee = Committee::new_for_benchmarks(committee_size);
    let public_config = NodePublicConfig::new_for_tests(committee_size).with_port_offset(0);
    let client_parameters = ClientParameters::default();

    let dir = TempDir::new().unwrap();
    let private_configs = NodePrivateConfig::new_for_benchmarks(dir.as_ref(), committee_size);
    private_configs.iter().for_each(|private_config| {
        fs::create_dir_all(&private_config.storage_path).unwrap();
    });

    let mut handles = Vec::new();
    for (i, private_config) in private_configs.into_iter().enumerate() {
        handles.push(
            run_replica(
                Authority::from(i),
                &committee,
                &public_config,
                private_config,
                &client_parameters,
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
    let committee = Committee::new_for_benchmarks(committee_size);
    let public_config = NodePublicConfig::new_for_tests(committee_size).with_port_offset(100);
    let client_parameters = ClientParameters::default();

    let dir = TempDir::new().unwrap();
    let private_configs = NodePrivateConfig::new_for_benchmarks(dir.as_ref(), committee_size);
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
                &committee,
                &public_config,
                private_config,
                &client_parameters,
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
        NodePrivateConfig::new_for_benchmarks(dir.as_ref(), committee_size).remove(authority);
    handles.push(
        run_replica(
            Authority::from(authority),
            &committee,
            &public_config,
            private_config,
            &client_parameters,
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
    let committee = Committee::new_for_benchmarks(committee_size);
    let public_config = NodePublicConfig::new_for_tests(committee_size).with_port_offset(200);
    let client_parameters = ClientParameters::default();

    let dir = TempDir::new().unwrap();
    let private_configs = NodePrivateConfig::new_for_benchmarks(dir.as_ref(), committee_size);
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
                &committee,
                &public_config,
                private_config,
                &client_parameters,
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
