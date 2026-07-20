// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use futures::future::join_all;
use rand::{SeedableRng, rngs::StdRng};

use dag::{block::transaction::Transaction, context::Ctx};
use replica::replica::ReplicaHandle;
use simulator::{SimulatedNetwork, SimulatorContext, SimulatorExecutor};

/// Externally submitted transactions must flow through to commit on every node when the
/// submitter runs as a task inside the simulated world.
#[test]
fn submitter_task_inside_simulation() {
    let rng = StdRng::seed_from_u64(42);
    let all_nodes_committed_marker = SimulatorExecutor::run(rng, async move {
        let (_network, replicas) = SimulatedNetwork::new_for_test(4).await;

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

        SimulatorContext::sleep(Duration::from_secs(20)).await;
        drop(submitters);

        let syncers = join_all(replicas.into_iter().map(ReplicaHandle::shutdown)).await;
        syncers.into_iter().all(|syncer| {
            let storage = syncer.into_storage();
            storage.iter_commits().any(|commit| {
                commit.sub_dag.iter().any(|reference| {
                    storage
                        .block_reader()
                        .get_block(*reference)
                        .is_some_and(|block| {
                            block
                                .transactions()
                                .iter()
                                .any(Transaction::has_test_marker)
                        })
                })
            })
        })
    });
    assert!(
        all_nodes_committed_marker,
        "every node must commit at least one externally submitted transaction"
    );
}
