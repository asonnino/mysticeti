// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{io, path::Path, time::Duration};

use consensus::test_util::committee_and_cores;
use dag::{
    config::{ImportExport, NodePublicConfig},
    context::Ctx,
    core::block_handler::CommitHandler,
    metrics::{Metrics, MetricsSnapshot},
    sync::net_sync::NetworkSyncer,
    test_util::rng_at_seed,
    types::BlockReference,
};

use crate::{
    config::{NetworkTopology, SimulationConfig},
    context::SimulatedCtx,
    executor::{OverrideNodeContext, SimulatedExecutorState},
    network::SimulatedNetwork,
    tracing::SimulatorTracing,
};

pub struct SimulationResults {
    pub committed_leaders: Vec<Vec<BlockReference>>,
    pub metrics: Vec<MetricsSnapshot>,
    pub commits_consistent: bool,
}

pub struct SimulationRunner {
    config: SimulationConfig,
}

impl SimulationRunner {
    pub fn new(config: SimulationConfig) -> Self {
        Self { config }
    }

    pub fn from_yaml(path: impl AsRef<Path>) -> Result<Self, io::Error> {
        let config = SimulationConfig::load(path)?;
        Ok(Self::new(config))
    }

    pub fn config(&self) -> &SimulationConfig {
        &self.config
    }

    pub fn run(&self) -> SimulationResults {
        SimulatorTracing::setup();
        let config = self.config.clone();
        let rng = rng_at_seed(config.rng_seed);
        SimulatedExecutorState::run(rng, run_simulation(config))
    }
}

async fn run_simulation(config: SimulationConfig) -> SimulationResults {
    let committee_size = config.committee_size;
    let (committee, cores) = committee_and_cores::<SimulatedCtx>(committee_size);

    let (simulated_network, networks) = SimulatedNetwork::new(&committee, config.latency_range());

    let mut public_config = NodePublicConfig::new_for_tests(committee_size);
    public_config.parameters = config.node_parameters.clone();

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
            config.commit_period,
            commit_handler,
            Metrics::new_for_test(0),
            &public_config,
        );
        drop(node_context);
        network_syncers.push(network_syncer);
    }

    apply_topology(&simulated_network, &config.topology).await;

    let duration = Duration::from_secs(config.duration_secs);
    SimulatedCtx::sleep(duration).await;

    let mut syncers = vec![];
    for network_syncer in network_syncers {
        let syncer = network_syncer.shutdown().await;
        syncers.push(syncer);
    }

    let committed_leaders: Vec<Vec<BlockReference>> = syncers
        .iter()
        .map(|syncer| syncer.commit_handler().committed_leaders().to_vec())
        .collect();

    let metrics: Vec<MetricsSnapshot> = syncers
        .iter()
        .map(|syncer| syncer.core().metrics.collect())
        .collect();

    let commits_consistent = check_commits_consistent(&committed_leaders);

    SimulationResults {
        committed_leaders,
        metrics,
        commits_consistent,
    }
}

fn check_commits_consistent(committed: &[Vec<BlockReference>]) -> bool {
    let empty: &[BlockReference] = &[];
    let mut max_commit: &[BlockReference] = empty;
    for commit in committed {
        if commit.len() >= max_commit.len() {
            if !commit.starts_with(max_commit) {
                return false;
            }
            max_commit = commit;
        } else if !max_commit.starts_with(commit) {
            return false;
        }
    }
    true
}

async fn apply_topology(network: &SimulatedNetwork, topology: &NetworkTopology) {
    match topology {
        NetworkTopology::FullMesh => {
            network.connect_all().await;
        }
        NetworkTopology::OneDown(node) => {
            let excluded = *node;
            network.connect_some(|a, _b| a != excluded).await;
        }
        NetworkTopology::Partition(groups) => {
            network
                .connect_some(|a, b| {
                    groups
                        .iter()
                        .any(|group| group.contains(&a) && group.contains(&b))
                })
                .await;
        }
        NetworkTopology::Star(center) => {
            let center = *center;
            network
                .connect_some(|a, b| a == center || b == center)
                .await;
        }
    }
}
