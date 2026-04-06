// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// TODO: Re-enable these tests using the SimulationRunner API.

// use std::time::Duration;
//
// use crate::context::SimulatedCtx;
// use crate::executor::SimulatedExecutorState;
// use crate::sim_tracing::setup_simulator_tracing;
// use crate::test_util::simulated_network_syncers;
// use dag::context::Ctx;
// use dag::test_util::{check_commits, print_stats, rng_at_seed};
//
// #[test]
// fn test_network_sync_sim_all_up() {
//     setup_simulator_tracing();
//     SimulatedExecutorState::run(
//         rng_at_seed(0),
//         test_network_sync_sim_all_up_async(),
//     );
// }
//
// async fn test_network_sync_sim_all_up_async() {
//     let (simulated_network, network_syncers) =
//         simulated_network_syncers(10);
//     simulated_network.connect_all().await;
//     SimulatedCtx::sleep(Duration::from_secs(20)).await;
//     let mut syncers = vec![];
//     for network_syncer in network_syncers {
//         let syncer = network_syncer.shutdown().await;
//         syncers.push(syncer);
//     }
//
//     check_commits(&syncers);
//     print_stats(&syncers);
// }
//
// #[test]
// fn test_network_sync_sim_one_down() {
//     setup_simulator_tracing();
//     SimulatedExecutorState::run(
//         rng_at_seed(0),
//         test_network_sync_sim_one_down_async(),
//     );
// }
//
// async fn test_network_sync_sim_one_down_async() {
//     let (simulated_network, network_syncers) =
//         simulated_network_syncers(10);
//     simulated_network
//         .connect_some(|a, _b| a != 0)
//         .await;
//     println!("Started");
//     SimulatedCtx::sleep(Duration::from_secs(40)).await;
//     println!("Done");
//     let mut syncers = vec![];
//     for network_syncer in network_syncers {
//         let syncer = network_syncer.shutdown().await;
//         syncers.push(syncer);
//     }
//
//     check_commits(&syncers);
//     print_stats(&syncers);
// }
//
// /// Test disabled: the vote-per-transaction fast path causes
// /// oversized blocks during catch-up.
// #[test]
// #[ignore = "oversized blocks from vote-per-tx fast path"]
// fn test_network_partition() {
//     setup_simulator_tracing();
//     SimulatedExecutorState::run(
//         rng_at_seed(0),
//         test_network_partition_async(),
//     );
// }
//
// async fn test_network_partition_async() {
//     let (simulated_network, network_syncers) =
//         simulated_network_syncers(10);
//     simulated_network
//         .connect_some(|a, b| a != 0 || b == 1)
//         .await;
//
//     println!("Started");
//     SimulatedCtx::sleep(Duration::from_secs(40)).await;
//     println!("Done");
//     let mut syncers = vec![];
//     for network_syncer in network_syncers {
//         let syncer = network_syncer.shutdown().await;
//         syncers.push(syncer);
//     }
//
//     check_commits(&syncers);
//     print_stats(&syncers);
// }
