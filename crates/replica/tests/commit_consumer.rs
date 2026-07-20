// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Port offsets across test binaries: smoke.rs uses 0/100/200, this file uses 400/500.

use std::{sync::Arc, time::Duration};

use tokio::{sync::mpsc, time};

use dag::{
    block::{BlockReference, transaction::Transaction},
    context::TokioCtx,
    metrics::Metrics,
};
use replica::{
    config::LoadGeneratorConfig,
    replica::{Replica, ReplicaHandle},
};

async fn run_all(replicas: Vec<Replica>) -> Vec<ReplicaHandle<TokioCtx>> {
    let mut handles = Vec::with_capacity(replicas.len());
    for replica in replicas {
        handles.push(replica.run().await.expect("test replica must start"));
    }
    handles
}

async fn committed_leaders(metrics: &Arc<Metrics>) -> u64 {
    metrics.collect().total_committed_leaders()
}

/// Wait until `metrics` reports at least `target` committed leaders.
async fn await_commits(metrics: &Arc<Metrics>, target: u64) {
    while committed_leaders(metrics).await < target {
        time::sleep(Duration::from_millis(50)).await;
    }
}

/// Transactions submitted through clients on two different replicas must all come back,
/// in commit order, through the consumer attached to replica 0.
#[tokio::test]
async fn submit_to_commit_round_trip() {
    const BATCHES_PER_CLIENT: usize = 5;
    let (sender, mut receiver) = mpsc::channel(1024);
    let (replicas, _metrics) = Replica::new_committee_for_test(4, 400, Some(sender));
    let mut handles = run_all(replicas).await;

    for client in [
        handles[0].transaction_client(),
        handles[1].transaction_client(),
    ] {
        for _ in 0..BATCHES_PER_CLIENT {
            client
                .submit(vec![Transaction::new_for_test()])
                .await
                .unwrap();
        }
    }

    // Receive sub-dags until every submitted transaction has been observed.
    let mut anchors: Vec<BlockReference> = vec![];
    let mut observed_transactions = 0;
    time::timeout(Duration::from_secs(30), async {
        while observed_transactions < 2 * BATCHES_PER_CLIENT {
            let subdag = receiver.recv().await.expect("channel closed early");
            observed_transactions += subdag.count_test_transactions();
            anchors.push(subdag.anchor);
        }
    })
    .await
    .expect("timed out waiting for submitted transactions to commit");

    // After shutdown the drained stream must equal the persisted commit sequence exactly.
    let storage = handles.remove(0).shutdown().await.into_storage();
    while let Some(subdag) = receiver.recv().await {
        anchors.push(subdag.anchor);
    }
    let persisted: Vec<BlockReference> =
        storage.iter_commits().map(|commit| commit.leader).collect();
    assert_eq!(anchors, persisted);
}

/// A full capacity-1 consumer must freeze replica 0 (backpressure) without deadlocking it:
/// once drained, the replica catches up and its stream still matches storage.
#[tokio::test]
async fn slow_consumer_throttles_replica_without_deadlock() {
    // The committee's own progress defines the observation windows; no fixed settle sleeps.
    const WINDOW: u64 = 5;
    let (sender, mut receiver) = mpsc::channel(1);
    let (replicas, metrics) = Replica::new_committee_for_test(4, 500, Some(sender));
    let mut handles = run_all(replicas).await;
    for handle in &mut handles {
        // Detach: the generator runs until the replica shuts down.
        drop(handle.start_load_generator(LoadGeneratorConfig::default()));
    }

    // Stage 1: the pipeline is live.
    let first = time::timeout(Duration::from_secs(15), receiver.recv())
        .await
        .expect("timed out waiting for the first commit")
        .expect("channel closed early");

    // Stage 2: with the channel full and undrained, the core thread blocks in blocking_send.
    // Replica 0 is wedged once its committed count stands still while the others advance.
    let mut frozen = committed_leaders(&metrics[0]).await;
    time::timeout(Duration::from_secs(30), async {
        loop {
            let others = committed_leaders(&metrics[1]).await;
            await_commits(&metrics[1], others + WINDOW).await;
            let current = committed_leaders(&metrics[0]).await;
            if current == frozen {
                break;
            }
            frozen = current;
        }
    })
    .await
    .expect("replica 0 was never throttled by the full consumer");

    // Stage 3: draining un-wedges the replica; no deadlock.
    let drain = tokio::spawn(async move {
        let mut anchors = vec![first.anchor];
        while let Some(subdag) = receiver.recv().await {
            anchors.push(subdag.anchor);
        }
        anchors
    });
    time::timeout(
        Duration::from_secs(20),
        await_commits(&metrics[0], frozen + WINDOW),
    )
    .await
    .expect("replica 0 did not catch up after the consumer drained");

    let storage = handles.remove(0).shutdown().await.into_storage();
    let anchors = drain.await.unwrap();
    let persisted: Vec<BlockReference> =
        storage.iter_commits().map(|commit| commit.leader).collect();
    assert_eq!(anchors, persisted);
}
