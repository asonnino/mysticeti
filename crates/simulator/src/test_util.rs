// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::context::SimulatedCtx;
use super::executor::OverrideNodeContext;
use super::network::SimulatedNetwork;
use dag::block_handler::CommitHandler;
use dag::config;
use dag::config::NodePublicConfig;
use dag::metrics::Metrics;
use dag::net_sync::NetworkSyncer;
use dag::test_util::committee_and_cores_epoch_duration;
use dag::types::RoundNumber;

pub fn simulated_network_syncers(n: usize) -> (SimulatedNetwork, Vec<NetworkSyncer<SimulatedCtx>>) {
    simulated_network_syncers_with_epoch_duration(
        n,
        config::node_defaults::default_rounds_in_epoch(),
    )
}

pub fn simulated_network_syncers_with_epoch_duration(
    n: usize,
    rounds_in_epoch: RoundNumber,
) -> (SimulatedNetwork, Vec<NetworkSyncer<SimulatedCtx>>) {
    let (committee, cores) = committee_and_cores_epoch_duration::<SimulatedCtx>(n, rounds_in_epoch);
    let (simulated_network, networks) = SimulatedNetwork::new(&committee);
    let public_config = NodePublicConfig::new_for_tests(n);
    let mut network_syncers = vec![];
    for (network, core) in networks.into_iter().zip(cores.into_iter()) {
        let commit_handler = CommitHandler::new(
            core.block_handler().transaction_time.clone(),
            core.metrics.clone(),
        );
        let node_context = OverrideNodeContext::enter(Some(core.authority()));
        let network_syncer = NetworkSyncer::start_for_test(
            network,
            core,
            3,
            commit_handler,
            config::node_defaults::default_shutdown_grace_period(),
            Metrics::new_for_test(0),
            &public_config,
        );
        drop(node_context);
        network_syncers.push(network_syncer);
    }
    (simulated_network, network_syncers)
}
