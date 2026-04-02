// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{sync::atomic::Ordering, time::Duration};

use crate::context::SimulatedCtx;
use crate::executor::SimulatedExecutorState;
use crate::sim_tracing::setup_simulator_tracing;
use crate::test_util::{simulated_network_syncers, simulated_network_syncers_with_epoch_duration};
use dag::config;
use dag::context::Ctx;
use dag::net_sync::NetworkSyncer;
use dag::syncer::Syncer;
use dag::test_util::{check_commits, print_stats, rng_at_seed};

async fn wait_for_epoch_to_close(
    network_syncers: Vec<NetworkSyncer<SimulatedCtx>>,
) -> Vec<Syncer<SimulatedCtx>> {
    let mut any_closed = false;
    while !any_closed {
        for net_sync in network_syncers.iter() {
            if net_sync.epoch_closing_time().load(Ordering::Relaxed) != 0 {
                any_closed = true;
            }
        }
        SimulatedCtx::sleep(Duration::from_secs(10)).await;
    }
    SimulatedCtx::sleep(config::node_defaults::default_shutdown_grace_period()).await;
    let mut syncers = vec![];
    for net_sync in network_syncers {
        let syncer = net_sync.shutdown().await;
        syncers.push(syncer);
    }
    syncers
}

#[test]
fn test_exact_commits_in_epoch() {
    SimulatedExecutorState::run(rng_at_seed(0), test_exact_commits_in_epoch_async());
}

async fn test_exact_commits_in_epoch_async() {
    let n = 4;
    let rounds_in_epoch = 3000;
    let (simulated_network, network_syncers) =
        simulated_network_syncers_with_epoch_duration(n, rounds_in_epoch);
    simulated_network.connect_all().await;
    let syncers = wait_for_epoch_to_close(network_syncers).await;
    let canonical_commit_seq = syncers[0].commit_handler().committed_leaders();
    for syncer in &syncers {
        let commit_seq = syncer.commit_handler().committed_leaders();
        assert_eq!(canonical_commit_seq, commit_seq);
    }
    print_stats(&syncers);
}

#[test]
fn test_network_sync_sim_all_up() {
    setup_simulator_tracing();
    SimulatedExecutorState::run(rng_at_seed(0), test_network_sync_sim_all_up_async());
}

async fn test_network_sync_sim_all_up_async() {
    let (simulated_network, network_syncers) = simulated_network_syncers(10);
    simulated_network.connect_all().await;
    SimulatedCtx::sleep(Duration::from_secs(20)).await;
    let mut syncers = vec![];
    for network_syncer in network_syncers {
        let syncer = network_syncer.shutdown().await;
        syncers.push(syncer);
    }

    check_commits(&syncers);
    print_stats(&syncers);
}

#[test]
fn test_network_sync_sim_one_down() {
    setup_simulator_tracing();
    SimulatedExecutorState::run(rng_at_seed(0), test_network_sync_sim_one_down_async());
}

async fn test_network_sync_sim_one_down_async() {
    let (simulated_network, network_syncers) = simulated_network_syncers(10);
    simulated_network.connect_some(|a, _b| a != 0).await;
    println!("Started");
    SimulatedCtx::sleep(Duration::from_secs(40)).await;
    println!("Done");
    let mut syncers = vec![];
    for network_syncer in network_syncers {
        let syncer = network_syncer.shutdown().await;
        syncers.push(syncer);
    }

    check_commits(&syncers);
    print_stats(&syncers);
}

/// Test disabled: the vote-per-transaction fast path causes
/// oversized blocks during catch-up.
#[test]
#[ignore = "oversized blocks from vote-per-tx fast path"]
fn test_network_partition() {
    setup_simulator_tracing();
    SimulatedExecutorState::run(rng_at_seed(0), test_network_partition_async());
}

async fn test_network_partition_async() {
    let (simulated_network, network_syncers) = simulated_network_syncers(10);
    simulated_network
        .connect_some(|a, b| a != 0 || b == 1)
        .await;

    println!("Started");
    SimulatedCtx::sleep(Duration::from_secs(40)).await;
    println!("Done");
    let mut syncers = vec![];
    for network_syncer in network_syncers {
        let syncer = network_syncer.shutdown().await;
        syncers.push(syncer);
    }

    check_commits(&syncers);
    print_stats(&syncers);
}
