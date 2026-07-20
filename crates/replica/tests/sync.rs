// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

mod common;

use common::committee_and_cores;
use dag::{
    context::TokioCtx,
    core::block_handler::CommitHandler,
    metrics::Metrics,
    sync::net_sync::NetworkSyncer,
    test_util::{check_commits, networks_and_addresses},
};
use tokio::sync::mpsc;

#[tokio::test]
async fn test_network_sync() {
    let (_committee, cores) = committee_and_cores::<TokioCtx>(4);
    let metrics: Vec<_> = cores.iter().map(|c| c.metrics.clone()).collect();
    let (networks, _) = networks_and_addresses(&metrics, 5101).await;
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
            None,
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

/// Every consumer must receive exactly the anchors its replica committed, in commit order.
#[tokio::test]
async fn test_network_sync_with_commit_consumer() {
    const MIN_COMMITS: usize = 10;
    let (_committee, cores) = committee_and_cores::<TokioCtx>(4);
    let metrics: Vec<_> = cores.iter().map(|c| c.metrics.clone()).collect();
    let (networks, _) = networks_and_addresses(&metrics, 5201).await;
    let mut network_syncers = vec![];
    let mut receivers = vec![];
    for (network, core) in networks.into_iter().zip(cores.into_iter()) {
        let commit_handler = CommitHandler::new(
            core.block_handler().transaction_time.clone(),
            Metrics::new_for_test(0),
        );
        let (sender, receiver) = mpsc::channel(1024);
        receivers.push(receiver);
        let network_syncer = NetworkSyncer::start(
            network,
            core,
            Duration::from_secs(1),
            false,
            commit_handler,
            Metrics::new_for_test(0),
            Some(sender),
        );
        network_syncers.push(network_syncer);
    }

    let mut received = vec![vec![]; receivers.len()];
    tokio::time::timeout(Duration::from_secs(10), async {
        for (receiver, anchors) in receivers.iter_mut().zip(received.iter_mut()) {
            while anchors.len() < MIN_COMMITS {
                anchors.push(receiver.recv().await.expect("channel closed early").anchor);
            }
        }
    })
    .await
    .expect("timed out waiting for commits");

    let mut syncers = vec![];
    for network_syncer in network_syncers {
        syncers.push(network_syncer.shutdown().await);
    }

    // Shutdown dropped the senders; draining yields the complete forwarded sequence.
    for ((mut receiver, mut anchors), syncer) in receivers.into_iter().zip(received).zip(&syncers) {
        while let Some(subdag) = receiver.recv().await {
            anchors.push(subdag.anchor);
        }
        assert_eq!(anchors, syncer.commit_handler().committed_leaders());
    }
    check_commits(&syncers);
}
