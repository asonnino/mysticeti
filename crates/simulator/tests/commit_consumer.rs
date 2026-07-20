// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use futures::future::join_all;
use rand::{SeedableRng, rngs::StdRng};

use dag::{
    block::{BlockReference, transaction::Transaction},
    consensus::CommittedSubDag,
    context::Ctx,
};
use replica::replica::ReplicaHandle;
use simulator::{SimulatedNetwork, SimulatorContext, SimulatorExecutor};
use tokio::sync::mpsc;

/// One committed sub-dag as observed by a consumer: the anchor and its block references.
type ObservedCommit = (BlockReference, Vec<BlockReference>);

/// Run a committee with a consumer and a submitter task per replica; return each replica's
/// consumer-observed commit sequence, after asserting it matches that replica's storage.
fn run_once(seed: u64, committee_size: usize) -> Vec<Vec<ObservedCommit>> {
    let rng = StdRng::seed_from_u64(seed);
    SimulatorExecutor::run(rng, async move {
        let mut senders = Vec::with_capacity(committee_size);
        let mut collectors = Vec::with_capacity(committee_size);
        for _ in 0..committee_size {
            let (sender, mut receiver) = mpsc::channel::<CommittedSubDag>(1024);
            senders.push(Some(sender));
            collectors.push(SimulatorContext::spawn(async move {
                let mut observed: Vec<ObservedCommit> = vec![];
                while let Some(subdag) = receiver.recv().await {
                    let references: Vec<BlockReference> = subdag
                        .blocks
                        .iter()
                        .map(|block| *block.reference())
                        .collect();
                    observed.push((subdag.anchor, references));
                }
                observed
            }));
        }

        let (_network, replicas) =
            SimulatedNetwork::new_for_test_with_commit_consumers(senders).await;
        let submitters: Vec<_> = replicas
            .iter()
            .map(|handle| {
                let client = handle.transaction_client();
                SimulatorContext::spawn(async move {
                    while client
                        .submit(vec![Transaction::new_for_test()])
                        .await
                        .is_ok()
                    {
                        SimulatorContext::sleep(Duration::from_millis(100)).await;
                    }
                })
            })
            .collect();

        SimulatorContext::sleep(Duration::from_secs(30)).await;
        for submitter in &submitters {
            SimulatorContext::abort(submitter);
        }

        let syncers = join_all(replicas.into_iter().map(ReplicaHandle::shutdown)).await;
        let mut sequences = Vec::with_capacity(committee_size);
        for (collector, syncer) in collectors.into_iter().zip(syncers) {
            let observed = collector.await.expect("collector task failed");
            let storage = syncer.into_storage();
            let persisted: Vec<ObservedCommit> = storage
                .iter_commits()
                .map(|commit| (commit.leader, commit.sub_dag))
                .collect();
            assert_eq!(observed, persisted, "consumer stream must match the WAL");
            assert!(!observed.is_empty(), "expected at least one commit");
            sequences.push(observed);
        }
        sequences
    })
}

/// Identical seeds must yield identical consumer-observed commit sequences.
#[test]
fn commit_stream_is_deterministic_per_seed() {
    for seed in [7, 42] {
        let first = run_once(seed, 4);
        let second = run_once(seed, 4);
        assert_eq!(
            first, second,
            "seed {seed} produced diverging commit streams"
        );
    }
}
