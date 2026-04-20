// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use consensus::test_util::committee_and_cores;
use dag::{
    context::TokioCtx,
    core::block_handler::CommitHandler,
    metrics::Metrics,
    sync::net_sync::NetworkSyncer,
    test_util::{check_commits, networks_and_addresses},
};

#[tokio::test]
async fn test_network_sync() {
    let (_committee, cores) = committee_and_cores::<TokioCtx>(4);
    let metrics: Vec<_> = cores.iter().map(|c| c.metrics.clone()).collect();
    let (networks, _) = networks_and_addresses(&metrics).await;
    let mut network_syncers = vec![];
    for (network, core) in networks.into_iter().zip(cores.into_iter()) {
        let commit_handler = CommitHandler::new(
            core.block_handler().transaction_time.clone(),
            Metrics::new_for_test(0),
        );
        let network_syncer = NetworkSyncer::start(
            network,
            core,
            Duration::from_secs(1),
            false,
            commit_handler,
            Metrics::new_for_test(0),
        );
        network_syncers.push(network_syncer);
    }
    println!("Started");
    tokio::time::sleep(Duration::from_secs(3)).await;
    println!("Done");
    let mut syncers = vec![];
    for network_syncer in network_syncers {
        let syncer = network_syncer.shutdown().await;
        syncers.push(syncer);
    }

    check_commits(&syncers);
}
