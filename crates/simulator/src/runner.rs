// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;

use consensus::universal_committer::UniversalCommitter;
use dag::{
    committee::Committee,
    config::{ConfigError, ImportExport, NodePublicConfig},
    context::Ctx,
    metrics::MetricsSnapshot,
    sync::net_sync::NetworkSyncer,
    types::{AuthorityIndex, BlockReference},
};
use rand::{SeedableRng, rngs::StdRng};

use crate::{
    config::{NetworkTopology, SimulationConfig},
    context::SimulatorContext,
    executor::SimulatorExecutor,
    network::SimulatedNetwork,
    replica::SimulatedReplica,
    tracing::SimulatorTracing,
};

type Syncer = NetworkSyncer<SimulatorContext, UniversalCommitter>;

pub struct SimulationResults {
    pub committed_leaders: Vec<Vec<BlockReference>>,
    pub metrics: Vec<MetricsSnapshot>,
    pub commits_consistent: bool,
}

impl SimulationResults {
    fn check_consistency(committed: &[Vec<BlockReference>]) -> bool {
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
}

pub struct SimulationRunner {
    config: SimulationConfig,
}

impl SimulationRunner {
    pub fn new(config: SimulationConfig) -> Self {
        Self { config }
    }

    pub fn from_yaml(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let config = SimulationConfig::load(path)?;
        Ok(Self::new(config))
    }

    pub fn config(&self) -> &SimulationConfig {
        &self.config
    }

    /// Run the simulation to completion and return results.
    ///
    /// Executes inside a deterministic discrete-event simulator:
    /// all time is simulated, no real wall-clock time elapses.
    pub fn run(self) -> SimulationResults {
        SimulatorTracing::setup();
        let rng = StdRng::seed_from_u64(self.config.rng_seed);
        SimulatorExecutor::run(rng, async {
            // Set up the committee, network, and per-node consensus.
            let duration = self.config.duration();
            let state = SimulationState::setup(self.config);

            // Wire up network connections according to the topology.
            state.apply_topology().await;

            // Let the simulation run for the configured duration.
            SimulatorContext::sleep(duration).await;

            // Shut down all nodes and collect committed blocks.
            state.collect_results().await
        })
    }
}

struct SimulationState {
    config: SimulationConfig,
    network: SimulatedNetwork,
    network_syncers: Vec<Syncer>,
}

impl SimulationState {
    fn setup(config: SimulationConfig) -> Self {
        let committee_size = config.committee_size;
        let committee = Committee::new_test(vec![1; committee_size]);

        let mut public_config = NodePublicConfig::new_for_tests(committee_size);
        public_config.parameters = config.node_parameters.clone();

        let (network, networks) = SimulatedNetwork::new(&committee, config.latency_range());

        let network_syncers = networks
            .into_iter()
            .enumerate()
            .map(|(i, node_network)| {
                SimulatedReplica::new(
                    i as AuthorityIndex,
                    committee.clone(),
                    public_config.clone(),
                    node_network,
                    config.commit_period,
                )
                .start()
            })
            .collect();

        Self {
            config,
            network,
            network_syncers,
        }
    }

    async fn apply_topology(&self) {
        match &self.config.topology {
            NetworkTopology::FullMesh => {
                self.network.connect_all().await;
            }
            NetworkTopology::OneDown(node) => {
                let excluded = *node;
                self.network.connect_some(|a, _b| a != excluded).await;
            }
            NetworkTopology::Partition(groups) => {
                self.network
                    .connect_some(|a, b| {
                        groups
                            .iter()
                            .any(|group| group.contains(&a) && group.contains(&b))
                    })
                    .await;
            }
            NetworkTopology::Star(center) => {
                let center = *center;
                self.network
                    .connect_some(|a, b| a == center || b == center)
                    .await;
            }
        }
    }

    async fn collect_results(self) -> SimulationResults {
        let mut syncers = vec![];
        for network_syncer in self.network_syncers {
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

        let commits_consistent = SimulationResults::check_consistency(&committed_leaders);

        SimulationResults {
            committed_leaders,
            metrics,
            commits_consistent,
        }
    }
}
