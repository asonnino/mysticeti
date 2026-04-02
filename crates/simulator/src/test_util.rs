// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::context::SimulatedCtx;
use super::executor::OverrideNodeContext;
use super::network::SimulatedNetwork;
use dag::config::NodePublicConfig;
use dag::core::block_handler::CommitHandler;
use dag::metrics::Metrics;
use dag::sync::net_sync::NetworkSyncer;
use dag::test_util::committee_and_cores;

pub fn simulated_network_syncers(n: usize) -> (SimulatedNetwork, Vec<NetworkSyncer<SimulatedCtx>>) {
    let (_committee, cores) = committee_and_cores::<SimulatedCtx>(n);
    let (simulated_network, networks) = SimulatedNetwork::new(&_committee);
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
            Metrics::new_for_test(0),
            &public_config,
        );
        drop(node_context);
        network_syncers.push(network_syncer);
    }
    (simulated_network, network_syncers)
}
