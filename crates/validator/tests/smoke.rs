// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::VecDeque, fs, net::SocketAddr, time::Duration};

use tempdir::TempDir;
use tokio::time;

use dag::{
    committee::Committee,
    config::{self, ClientParameters, NodePrivateConfig, NodePublicConfig},
    metrics,
    types::AuthorityIndex,
};
use validator::validator::Validator;

async fn check_commit(address: &SocketAddr) -> Result<bool, reqwest::Error> {
    let route = metrics::server::METRICS_ROUTE;
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
async fn validator_commit() {
    let committee_size = 4;
    let committee = Committee::new_for_benchmarks(committee_size);
    let public_config = NodePublicConfig::new_for_tests(committee_size).with_port_offset(0);
    let client_parameters = ClientParameters::default();

    let mut handles = Vec::new();
    let dir = TempDir::new("validator_commit").unwrap();
    let private_configs = NodePrivateConfig::new_for_benchmarks(dir.as_ref(), committee_size);
    private_configs.iter().for_each(|private_config| {
        fs::create_dir_all(&private_config.storage_path).unwrap();
    });

    for (i, private_config) in private_configs.into_iter().enumerate() {
        let authority = i as AuthorityIndex;
        let validator = Validator::start(
            authority,
            committee.clone(),
            public_config.clone(),
            private_config,
            client_parameters.clone(),
        )
        .await
        .unwrap();
        handles.push(validator.await_completion());
    }

    let addresses = public_config
        .all_metric_addresses()
        .map(|address| address.to_owned())
        .collect();
    let timeout = config::node_defaults::default_leader_timeout() * 5;

    tokio::select! {
        _ = await_for_commits(addresses) => (),
        _ = time::sleep(timeout) => panic!("Failed to gather commits within a few timeouts"),
    }
}

#[tokio::test]
async fn validator_sync() {
    let committee_size = 4;
    let committee = Committee::new_for_benchmarks(committee_size);
    let public_config = NodePublicConfig::new_for_tests(committee_size).with_port_offset(100);
    let client_parameters = ClientParameters::default();

    let mut handles = Vec::new();
    let dir = TempDir::new("validator_sync").unwrap();
    let private_configs = NodePrivateConfig::new_for_benchmarks(dir.as_ref(), committee_size);
    private_configs.iter().for_each(|private_config| {
        fs::create_dir_all(&private_config.storage_path).unwrap();
    });

    for (i, private_config) in private_configs.into_iter().enumerate() {
        if i == 0 {
            continue;
        }
        let authority = i as AuthorityIndex;
        let validator = Validator::start(
            authority,
            committee.clone(),
            public_config.clone(),
            private_config,
            client_parameters.clone(),
        )
        .await
        .unwrap();
        handles.push(validator.await_completion());
    }

    let addresses = public_config
        .all_metric_addresses()
        .skip(1)
        .map(|address| address.to_owned())
        .collect();
    let timeout = config::node_defaults::default_leader_timeout() * 5;
    tokio::select! {
        _ = await_for_commits(addresses) => (),
        _ = time::sleep(timeout) => panic!("Failed to gather commits within a few timeouts"),
    }

    let authority = 0;
    let private_config =
        NodePrivateConfig::new_for_benchmarks(dir.as_ref(), committee_size).remove(authority);
    let validator = Validator::start(
        authority as AuthorityIndex,
        committee.clone(),
        public_config.clone(),
        private_config,
        client_parameters,
    )
    .await
    .unwrap();
    handles.push(validator.await_completion());

    let address = public_config
        .all_metric_addresses()
        .next()
        .map(|address| address.to_owned())
        .unwrap();
    let timeout = config::node_defaults::default_leader_timeout() * 5;
    tokio::select! {
        _ = await_for_commits(vec![address]) => (),
        _ = time::sleep(timeout) => panic!("Failed to gather commits within a few timeouts"),
    }
}

#[tokio::test]
async fn validator_crash_faults() {
    let committee_size = 4;
    let committee = Committee::new_for_benchmarks(committee_size);
    let public_config = NodePublicConfig::new_for_tests(committee_size).with_port_offset(200);
    let client_parameters = ClientParameters::default();

    let mut handles = Vec::new();
    let dir = TempDir::new("validator_crash_faults").unwrap();
    let private_configs = NodePrivateConfig::new_for_benchmarks(dir.as_ref(), committee_size);
    private_configs.iter().for_each(|private_config| {
        fs::create_dir_all(&private_config.storage_path).unwrap();
    });

    for (i, private_config) in private_configs.into_iter().enumerate() {
        if i == 0 {
            continue;
        }
        let authority = i as AuthorityIndex;
        let validator = Validator::start(
            authority,
            committee.clone(),
            public_config.clone(),
            private_config,
            client_parameters.clone(),
        )
        .await
        .unwrap();
        handles.push(validator.await_completion());
    }

    let addresses = public_config
        .all_metric_addresses()
        .skip(1)
        .map(|address| address.to_owned())
        .collect();
    let timeout = config::node_defaults::default_leader_timeout() * 15;

    tokio::select! {
        _ = await_for_commits(addresses) => (),
        _ = time::sleep(timeout) => panic!("Failed to gather commits within a few timeouts"),
    }
}
