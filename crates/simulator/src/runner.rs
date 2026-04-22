// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::path::{Path, PathBuf};

use consensus::committer::Committer;
use dag::{
    authority::Authority,
    committee::Committee,
    config::{ConfigError, ImportExport},
    context::Ctx,
    core::syncer::Syncer,
    metrics::{Metrics, RunResult},
};
use rand::{SeedableRng, rngs::StdRng};
use replica::{
    builder::{ReplicaBuilder, StorageKind},
    config::{PrivateReplicaConfig, PublicReplicaConfig},
    replica::ReplicaHandle,
};

use crate::{
    config::{NetworkTopology, SimulationConfig},
    context::SimulatorContext,
    executor::{JoinHandle, SimulatorExecutor},
    network::SimulatedNetwork,
    tracing::SimulatorTracing,
};

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

    /// Run the simulation to completion and return the result.
    ///
    /// Executes inside a deterministic discrete-event simulator:
    /// all time is simulated, no real wall-clock time elapses.
    pub fn run(self) -> RunResult<SimulationConfig> {
        let _guard = SimulatorTracing::new().setup().ok();
        let rng = StdRng::seed_from_u64(self.config.rng_seed);
        SimulatorExecutor::run(rng, async {
            let state = SimulationState::setup(self.config).await;
            state.apply_topology().await;
            SimulatorContext::sleep(state.config.duration()).await;
            state.collect_result().await
        })
    }
}

struct SimulationState {
    config: SimulationConfig,
    network: SimulatedNetwork,
    replicas: Vec<ReplicaHandle<SimulatorContext>>,
    /// JoinHandles for any load generators we started, so they stay alive for the duration
    /// of the simulation.
    _load_generators: Vec<JoinHandle<()>>,
}

impl SimulationState {
    async fn setup(config: SimulationConfig) -> Self {
        let committee_size = config.committee_size;
        let committee = Committee::new_test(vec![1; committee_size]);

        let public_config = PublicReplicaConfig::new_for_tests(committee_size)
            .with_parameters(config.replica_parameters.clone());

        let (network, networks) = SimulatedNetwork::new(&committee, config.latency_range());

        // The simulator doesn't touch disk; the WAL path in the private
        // configs is unused once we override storage with `InMemory`.
        let private_configs =
            PrivateReplicaConfig::new_for_benchmarks(&PathBuf::from("simulator"), committee_size);

        let mut replicas = Vec::with_capacity(committee_size);
        let mut load_generators = Vec::new();
        for (i, (node_network, private_config)) in
            networks.into_iter().zip(private_configs).enumerate()
        {
            let authority = Authority::from(i);
            let metrics = Metrics::new_for_test(committee_size);
            let mut handle = ReplicaBuilder::new(authority, public_config.clone(), private_config)
                .with_storage(StorageKind::Ephemeral)
                .with_crypto_disabled()
                .with_metrics(metrics)
                .with_network(node_network)
                .build()
                .run::<SimulatorContext>()
                .await
                .expect("simulator replica build must not fail");
            if let Some(load_generator) = config.load_generator.clone() {
                load_generators.push(handle.start_load_generator(load_generator));
            }
            replicas.push(handle);
        }

        Self {
            config,
            network,
            replicas,
            _load_generators: load_generators,
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

    async fn collect_result(self) -> RunResult<SimulationConfig> {
        let syncers: Vec<Syncer<SimulatorContext, Committer>> =
            futures::future::join_all(self.replicas.into_iter().map(ReplicaHandle::shutdown)).await;

        let (metrics, storages): (Vec<_>, Vec<&_>) = syncers
            .iter()
            .map(|syncer| {
                let core = syncer.core();
                (core.metrics.collect(), core.storage())
            })
            .unzip();

        let duration = self.config.duration();
        RunResult::collect(metrics, &storages, self.config, duration)
    }
}
