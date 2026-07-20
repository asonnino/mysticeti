// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    io,
    ops::Range,
    path::{Path, PathBuf},
    time::Duration,
};

use consensus::committer::Committer;
use dag::{
    authority::Authority,
    committee::Committee,
    config::{ConfigError, ImportExport},
    context::Ctx,
    core::syncer::Syncer,
    metrics::Metrics,
    storage::Storage,
};
use rand::{SeedableRng, rngs::StdRng};
use replica::{
    builder::{ReplicaBuilder, StorageKind},
    config::{LoadGeneratorConfig, PrivateReplicaConfig, PublicReplicaConfig},
    replica::ReplicaHandle,
    result::{RunKind, RunResult},
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
    pub fn run(self) -> io::Result<RunResult<SimulationConfig>> {
        let _guard = SimulatorTracing::new().setup().ok();
        let rng = StdRng::seed_from_u64(self.config.rng_seed);
        let Self { config } = self;
        SimulatorExecutor::run(rng, async move {
            let state = SimulationState::setup(config).await;
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

impl SimulatedNetwork {
    /// A fully connected committee with default parameters. The returned network must be
    /// kept alive for the duration of the run.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn new_for_test(
        committee_size: usize,
    ) -> (Self, Vec<ReplicaHandle<SimulatorContext>>) {
        let public_config = PublicReplicaConfig::new_for_tests(committee_size);
        let latency_range = Duration::from_millis(50)..Duration::from_millis(100);
        let (network, replicas, _) =
            SimulationState::build_replicas(committee_size, public_config, latency_range, None)
                .await;
        network.connect_all().await;
        (network, replicas)
    }
}

impl SimulationState {
    /// Build a committee of simulated replicas wired through a [`SimulatedNetwork`].
    async fn build_replicas(
        committee_size: usize,
        public_config: PublicReplicaConfig,
        latency_range: Range<Duration>,
        load_generator: Option<LoadGeneratorConfig>,
    ) -> (
        SimulatedNetwork,
        Vec<ReplicaHandle<SimulatorContext>>,
        Vec<JoinHandle<()>>,
    ) {
        let committee = Committee::new_test(vec![1; committee_size]);
        let (network, networks) = SimulatedNetwork::new(&committee, latency_range);

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
            if let Some(load_generator) = load_generator.clone() {
                load_generators.push(handle.start_load_generator(load_generator));
            }
            replicas.push(handle);
        }
        (network, replicas, load_generators)
    }

    async fn setup(config: SimulationConfig) -> Self {
        let public_config = PublicReplicaConfig::new_for_tests(config.committee_size)
            .with_parameters(config.replica_parameters.clone());
        let (network, replicas, load_generators) = Self::build_replicas(
            config.committee_size,
            public_config,
            config.latency_range(),
            config.load_generator.clone(),
        )
        .await;

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

    async fn collect_result(self) -> io::Result<RunResult<SimulationConfig>> {
        let Self {
            config, replicas, ..
        } = self;
        let duration = config.duration();
        let syncers: Vec<Syncer<SimulatorContext, Committer>> =
            futures::future::join_all(replicas.into_iter().map(ReplicaHandle::shutdown)).await;
        let metrics: Vec<_> = syncers
            .iter()
            .map(|syncer| syncer.core().metrics.collect())
            .collect();
        let storages: Vec<Storage> = syncers.into_iter().map(Syncer::into_storage).collect();

        Ok(RunResult::new(
            metrics,
            storages,
            config,
            duration,
            RunKind::Simulation,
        ))
    }
}
