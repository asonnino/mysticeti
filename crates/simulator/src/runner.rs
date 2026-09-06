// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    io,
    ops::Range,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicU64},
    time::Duration,
};

use consensus::committer::Committer;
use dag::{
    authority::Authority,
    config::{ConfigError, ImportExport},
    consensus::CommittedSubDag,
    context::Ctx,
    core::syncer::Syncer,
    metrics::{Metrics, MetricsSnapshot},
    storage::Storage,
};
use rand::{SeedableRng, rngs::StdRng};
use replica::{
    builder::{ReplicaBuilder, StorageKind},
    config::{LoadGeneratorConfig, PrivateReplicaConfig, PublicReplicaConfig},
    replica::ReplicaHandle,
    result::{RunKind, RunResult, TimeSeriesRow},
};
use tokio::sync::{Mutex, mpsc};

use crate::{
    conditions::NetworkConditions,
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
    /// Replica slots shared with the crash tasks, which shut replicas down
    /// mid-run and leave their final state behind for the results.
    replicas: Arc<Mutex<Vec<Option<ReplicaSlot>>>>,
    /// JoinHandles for any load generators we started, so they stay alive for the duration
    /// of the simulation.
    _load_generators: Vec<JoinHandle<()>>,
    /// JoinHandles for the crash tasks, kept alive for the run.
    _crash_tasks: Vec<JoinHandle<()>>,
    /// In-run samples shared with the sampler task.
    time_series: Arc<Mutex<Vec<TimeSeriesRow>>>,
    /// JoinHandle for the sampler, kept alive for the run.
    _sampler_task: Option<JoinHandle<()>>,
}

enum ReplicaSlot {
    Running(ReplicaHandle<SimulatorContext>),
    Crashed(Box<Syncer<SimulatorContext, Committer>>),
}

impl SimulatedNetwork {
    /// A fully connected committee with default parameters. The returned network must be
    /// kept alive for the duration of the run.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn new_for_test(
        committee_size: usize,
    ) -> (Self, Vec<ReplicaHandle<SimulatorContext>>) {
        Self::new_for_test_with_commit_consumers(vec![None; committee_size]).await
    }

    /// Like [`SimulatedNetwork::new_for_test`], with one optional commit consumer per
    /// replica.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn new_for_test_with_commit_consumers(
        commit_consumers: Vec<Option<mpsc::Sender<CommittedSubDag>>>,
    ) -> (Self, Vec<ReplicaHandle<SimulatorContext>>) {
        let public_config = PublicReplicaConfig::new_for_tests(commit_consumers.len());
        let latency_range = Duration::from_millis(50)..Duration::from_millis(100);
        let (network, replicas, _, _) = SimulationState::build_replicas(
            public_config,
            latency_range,
            None,
            None,
            None,
            None,
            commit_consumers,
        )
        .await;
        network.connect_all().await;
        (network, replicas)
    }
}

impl SimulationState {
    /// Build a committee of simulated replicas wired through a [`SimulatedNetwork`]. The
    /// committee is derived from `public_config`; `commit_consumers` holds one optional
    /// consumer per replica.
    async fn build_replicas(
        public_config: PublicReplicaConfig,
        latency_range: Range<Duration>,
        link_jitter: Option<Duration>,
        conditions: Option<Arc<NetworkConditions>>,
        period_cell: Option<Arc<AtomicU64>>,
        load_generator: Option<LoadGeneratorConfig>,
        commit_consumers: Vec<Option<mpsc::Sender<CommittedSubDag>>>,
    ) -> (
        SimulatedNetwork,
        Vec<ReplicaHandle<SimulatorContext>>,
        Vec<JoinHandle<()>>,
        Vec<Arc<Metrics>>,
    ) {
        let committee = public_config.committee();
        let committee_size = committee.len();
        assert_eq!(commit_consumers.len(), committee_size);
        let (network, networks) =
            SimulatedNetwork::new(&committee, latency_range, link_jitter, conditions);

        // The simulator doesn't touch disk; the WAL path in the private
        // configs is unused once we override storage with `InMemory`.
        let private_configs =
            PrivateReplicaConfig::new_for_benchmarks(&PathBuf::from("simulator"), committee_size);

        let mut replicas = Vec::with_capacity(committee_size);
        let mut load_generators = Vec::new();
        let mut metrics_handles = Vec::with_capacity(committee_size);
        for (i, ((node_network, private_config), commit_consumer)) in networks
            .into_iter()
            .zip(private_configs)
            .zip(commit_consumers)
            .enumerate()
        {
            let authority = Authority::from(i);
            let metrics = Metrics::new_for_test(committee_size);
            metrics_handles.push(metrics.clone());
            let mut builder = ReplicaBuilder::new(authority, public_config.clone(), private_config)
                .with_storage(StorageKind::Ephemeral)
                .with_crypto_disabled()
                .with_metrics(metrics)
                .with_network(node_network);
            if let Some(commit_consumer) = commit_consumer {
                builder = builder.with_commit_consumer(commit_consumer);
            }
            // The adversary reads replica 0's live period.
            if i == 0
                && let Some(cell) = &period_cell
            {
                builder = builder.with_period_cell(cell.clone());
            }
            let mut handle = builder
                .build()
                .run::<SimulatorContext>()
                .await
                .expect("simulator replica build must not fail");
            if let Some(load_generator) = load_generator.clone() {
                load_generators.push(handle.start_load_generator(load_generator));
            }
            replicas.push(handle);
        }
        (network, replicas, load_generators, metrics_handles)
    }

    async fn setup(config: SimulationConfig) -> Self {
        let public_config = PublicReplicaConfig::new_for_tests(config.committee_size)
            .with_parameters(config.replica_parameters.clone());
        let commit_consumers = vec![None; config.committee_size];
        let condition_phases = config.condition_phases();
        let adversary_period_cell = None;
        let conditions = if condition_phases.is_empty() {
            None
        } else {
            // The adversary's strategy is protocol-independent (it attacks
            // the public round-robin schedule); only the cohort size comes
            // from the configuration.
            let protocol = config
                .replica_parameters
                .consensus
                .to_protocol(&public_config.committee())
                .expect("valid protocol");
            Some(Arc::new(NetworkConditions::new(
                condition_phases,
                config.committee_size,
                protocol.leader_count.get(),
            )))
        };
        let (network, replicas, load_generators, metrics_handles) = Self::build_replicas(
            public_config,
            config.latency_range(),
            config.link_jitter(),
            conditions,
            adversary_period_cell,
            config.load_generator.clone(),
            commit_consumers,
        )
        .await;

        let replicas = Arc::new(Mutex::new(
            replicas
                .into_iter()
                .map(|handle| Some(ReplicaSlot::Running(handle)))
                .collect::<Vec<_>>(),
        ));
        let crash_tasks = Self::spawn_crash_tasks(&config, &replicas);
        let time_series = Arc::new(Mutex::new(Vec::new()));
        let sampler_task = config
            .sample_interval_secs
            .map(|interval| Self::spawn_sampler(interval, metrics_handles, time_series.clone()));

        Self {
            config,
            network,
            replicas,
            time_series,
            _load_generators: load_generators,
            _crash_tasks: crash_tasks,
            _sampler_task: sampler_task,
        }
    }

    /// Periodically snapshot every replica's counters into the time series.
    fn spawn_sampler(
        interval_secs: u64,
        metrics_handles: Vec<Arc<Metrics>>,
        time_series: Arc<Mutex<Vec<TimeSeriesRow>>>,
    ) -> JoinHandle<()> {
        SimulatorContext::spawn(async move {
            let interval = Duration::from_secs(interval_secs.max(1));
            // Previous snapshots, for the windowed latency columns; the first
            // tick's window spans everything since startup.
            let mut previous: Vec<Option<MetricsSnapshot>> = std::iter::repeat_with(|| None)
                .take(metrics_handles.len())
                .collect();
            loop {
                SimulatorContext::sleep(interval).await;
                let time_s = SimulatorContext::time().as_secs();
                let mut rows = time_series.lock().await;
                for (replica, metrics) in metrics_handles.iter().enumerate() {
                    let snapshot = metrics.collect();
                    let (latency_p50_ms, latency_avg_ms) = match &previous[replica] {
                        Some(earlier) => (
                            snapshot.latency_window_percentile_ms(earlier, 0.5),
                            snapshot.latency_window_mean_ms(earlier),
                        ),
                        None => (
                            snapshot.latency_percentile_ms(0.5),
                            snapshot.latency_mean_ms(),
                        ),
                    };
                    rows.push(TimeSeriesRow {
                        time_s,
                        replica,
                        direct_commits: snapshot.direct_commits(),
                        indirect_commits: snapshot.indirect_commits(),
                        direct_skips: snapshot.direct_skips(),
                        indirect_skips: snapshot.indirect_skips(),
                        leader_timeouts: snapshot.leader_timeouts(),
                        steelhead_period: snapshot.steelhead_period(),
                        latency_p50_ms,
                        latency_avg_ms,
                    });
                    previous[replica] = Some(snapshot);
                }
            }
        })
    }

    /// One task per crash spec: sleep to the crash time, then shut the replica
    /// down — its peers observe the dropped connections — and keep its final
    /// state for the results.
    fn spawn_crash_tasks(
        config: &SimulationConfig,
        replicas: &Arc<Mutex<Vec<Option<ReplicaSlot>>>>,
    ) -> Vec<JoinHandle<()>> {
        config
            .crashes
            .iter()
            .map(|crash| {
                assert!(
                    crash.replica < config.committee_size,
                    "crash target {} outside the committee",
                    crash.replica
                );
                assert!(
                    crash.at_secs < config.duration_secs,
                    "crash at {}s must land before the run ends at {}s",
                    crash.at_secs,
                    config.duration_secs
                );
                let replicas = replicas.clone();
                let crash = *crash;
                SimulatorContext::spawn(async move {
                    SimulatorContext::sleep(Duration::from_secs(crash.at_secs)).await;
                    let mut slots = replicas.lock().await;
                    // Result collection may already have drained the slots.
                    let Some(slot) = slots.get_mut(crash.replica) else {
                        return;
                    };
                    match slot.take() {
                        Some(ReplicaSlot::Running(handle)) => {
                            let syncer = handle.shutdown().await;
                            *slot = Some(ReplicaSlot::Crashed(Box::new(syncer)));
                        }
                        // Crashed by an earlier spec: keep that state.
                        other => *slot = other,
                    }
                })
            })
            .collect()
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
            config,
            replicas,
            time_series,
            ..
        } = self;
        let duration = config.duration();
        let mut syncers: Vec<Syncer<SimulatorContext, Committer>> = Vec::new();
        for slot in replicas.lock().await.drain(..) {
            match slot.expect("replica slot present") {
                ReplicaSlot::Running(handle) => syncers.push(handle.shutdown().await),
                ReplicaSlot::Crashed(syncer) => syncers.push(*syncer),
            }
        }
        let metrics: Vec<_> = syncers
            .iter()
            .map(|syncer| syncer.core().metrics.collect())
            .collect();
        let storages: Vec<Storage> = syncers.into_iter().map(Syncer::into_storage).collect();

        let time_series = std::mem::take(&mut *time_series.lock().await);
        Ok(
            RunResult::new(metrics, storages, config, duration, RunKind::Simulation)
                .with_time_series(time_series),
        )
    }
}
