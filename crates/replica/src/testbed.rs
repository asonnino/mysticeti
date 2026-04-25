// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Orchestrates `N` replicas in a single tokio process, mirroring
//! [`SimulationRunner`](../../simulator/src/runner.rs) but running real
//! networking + metrics instead of the discrete-event simulator. Used by the
//! `local-testbed` CLI subcommand and reusable from integration tests.
//!
//! Single-entry-point API:
//! - [`LocalTestbedRunner::new`] holds run-wide configuration.
//! - [`LocalTestbedRunner::run`] brings every replica online and returns a
//!   [`LocalTestbedHandle`]: a live testbed that exposes per-replica metrics
//!   for live observation.
//! - [`LocalTestbedHandle::stop`] cancels the run and produces the
//!   `RunResult<TestbedConfig>`.
//!
//! The cancellation token is owned internally by the handle. Callers wanting
//! fixed-duration runs `sleep` then call `stop()`; callers wanting external
//! triggers (Ctrl-C, oneshot, etc.) `select!` against `stop()` accordingly.

use std::{
    io,
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
    time::Instant,
};

use dag::{
    authority::Authority,
    context::TokioCtx,
    metrics::{Metrics, RunKind, RunResult},
};
use eyre::{Result, WrapErr};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    builder::{ReplicaBuilder, StorageKind},
    config::{LoadGeneratorConfig, PrivateReplicaConfig, PublicReplicaConfig, ReplicaParameters},
    prometheus::{MetricsRegistry, PrometheusServer},
    replica::ReplicaHandle,
};

/// Full configuration for a local-testbed run. Embedded in the resulting
/// [`RunResult`] and round-tripped via the exporter's `config.yaml`, so a run
/// is fully reproducible from its archived config.
#[derive(Clone, Serialize, Deserialize)]
pub struct TestbedConfig {
    pub committee_size: usize,
    pub replica_parameters: ReplicaParameters,
    pub load_generator: LoadGeneratorConfig,
}

/// Builder for a local-testbed run. Holds the configuration and any optional
/// knobs (e.g. a DAG-log writer); consume via [`Self::run`] to bring replicas
/// online and obtain a [`LocalTestbedHandle`].
pub struct LocalTestbedRunner {
    config: TestbedConfig,
    dag_writer: Option<Box<dyn io::Write + Send + Sync + 'static>>,
}

impl LocalTestbedRunner {
    pub fn new(config: TestbedConfig) -> Self {
        Self {
            config,
            dag_writer: None,
        }
    }

    /// Stream every committed sub-DAG to `writer` as newline-delimited JSON
    /// during the post-run storage scan. Memory stays `O(replicas)` regardless
    /// of commit count.
    pub fn with_dag_writer<W: io::Write + Send + Sync + 'static>(mut self, writer: W) -> Self {
        self.dag_writer = Some(Box::new(writer));
        self
    }

    pub fn config(&self) -> &TestbedConfig {
        &self.config
    }

    /// Bring every replica online (storage, prometheus servers, load
    /// generators) and spawn a background task that waits for cancellation
    /// then collects results. Returns a [`LocalTestbedHandle`] holding the
    /// live `Arc<Metrics>` snapshots and the cancellation token.
    ///
    /// Storage is [`StorageKind::Ephemeral`] (anonymous tempfile per replica),
    /// matching the simulator. The testbed therefore leaves no on-disk WAL
    /// state behind, so `--output-dir` only ever contains the exporter's
    /// artefacts.
    pub async fn run(self) -> Result<LocalTestbedHandle> {
        let state = LocalTestbedState::setup(self).await?;
        let metrics = state.metrics().to_vec();
        let cancel = CancellationToken::new();
        let task = tokio::spawn({
            let cancel = cancel.clone();
            async move {
                cancel.cancelled().await;
                state.collect_result().await
            }
        });
        Ok(LocalTestbedHandle {
            metrics,
            cancel,
            task,
        })
    }
}

/// A live local testbed. The replicas are running and producing metrics; a
/// background task is waiting for [`Self::stop`] to fire to shut them down
/// and collect the [`RunResult`].
///
/// Drop the handle without calling [`Self::stop`] to abort without collecting:
/// the runtime aborts the task at process exit.
pub struct LocalTestbedHandle {
    metrics: Vec<Arc<Metrics>>,
    cancel: CancellationToken,
    task: JoinHandle<Result<RunResult<TestbedConfig>>>,
}

impl LocalTestbedHandle {
    /// Per-replica metrics handles, in authority order.
    pub fn metrics(&self) -> &[Arc<Metrics>] {
        &self.metrics
    }

    /// Cancel the run, wait for the collector to finish, and return the
    /// [`RunResult<TestbedConfig>`]. Idempotent at the cancellation level
    /// (calling `stop` after the token was already cancelled is fine).
    pub async fn stop(self) -> Result<RunResult<TestbedConfig>> {
        self.cancel.cancel();
        self.task.await.wrap_err("testbed task panicked")?
    }
}

/// Live state of a running testbed: replica handles, prometheus / load-gen
/// background tasks held alive by the leading-underscore fields, and the
/// optional DAG-log writer carried into [`Self::collect_result`].
struct LocalTestbedState {
    config: TestbedConfig,
    handles: Vec<ReplicaHandle<TokioCtx>>,
    metrics_per_replica: Vec<Arc<Metrics>>,
    /// Kept alive only because they hold the prometheus servers / load
    /// generators running. The leading underscore signals "lifetime hold,
    /// not used".
    _metrics_servers: Vec<JoinHandle<()>>,
    _load_generators: Vec<JoinHandle<()>>,
    started_at: Instant,
    dag_writer: Option<Box<dyn io::Write + Send + Sync + 'static>>,
}

impl LocalTestbedState {
    /// Per-replica metrics handles populated at [`Self::setup`]. Read once by
    /// [`LocalTestbedRunner::run`] to seed the [`LocalTestbedHandle`]; not
    /// touched by [`Self::collect_result`] (which takes fresh snapshots from
    /// each shut-down syncer).
    fn metrics(&self) -> &[Arc<Metrics>] {
        &self.metrics_per_replica
    }

    async fn setup(runner: LocalTestbedRunner) -> Result<Self> {
        let LocalTestbedRunner { config, dag_writer } = runner;
        let committee_size = config.committee_size;

        let ips = vec![IpAddr::V4(Ipv4Addr::LOCALHOST); committee_size];
        let public_config = PublicReplicaConfig::new_for_benchmarks(ips)
            .with_parameters(config.replica_parameters.clone());

        // The Ephemeral storage kind ignores `private_config.storage_path`, so the
        // path passed here is unused — we just need the rest of `PrivateReplicaConfig`'s
        // identity (key material, authority index).
        let private_configs = PrivateReplicaConfig::new_for_benchmarks(
            std::path::Path::new("local-testbed"),
            committee_size,
        );

        let mut handles: Vec<ReplicaHandle<TokioCtx>> = Vec::with_capacity(committee_size);
        let mut metrics_servers = Vec::with_capacity(committee_size);
        let mut load_generators = Vec::with_capacity(committee_size);
        let mut metrics_per_replica: Vec<Arc<Metrics>> = Vec::with_capacity(committee_size);

        for (index, private_config) in private_configs.into_iter().enumerate() {
            let authority = Authority::from(index);
            let metrics_address = public_config
                .metrics_address(authority)
                .expect("metrics address must exist for every authority we just constructed");
            let registry = MetricsRegistry::new();
            let metrics = Metrics::new(&registry, committee_size, None);
            metrics_per_replica.push(metrics.clone());
            let mut handle = ReplicaBuilder::new(authority, public_config.clone(), private_config)
                .with_storage(StorageKind::Ephemeral)
                .with_registry(registry.clone())
                .with_metrics(metrics)
                .build()
                .run::<TokioCtx>()
                .await?;
            load_generators.push(handle.start_load_generator(config.load_generator.clone()));
            metrics_servers.push(
                PrometheusServer::new(metrics_address, &registry)
                    .bind_all_interfaces()
                    .start()
                    .await?,
            );
            handles.push(handle);
        }

        Ok(Self {
            config,
            handles,
            metrics_per_replica,
            _metrics_servers: metrics_servers,
            _load_generators: load_generators,
            started_at: Instant::now(),
            dag_writer,
        })
    }

    /// Shut every replica down (sequentially — the syncers borrow each
    /// other's storage during the WAL scan, so we just need the borrows to be
    /// live, not concurrent) and assemble the `RunResult<TestbedConfig>`.
    async fn collect_result(self) -> Result<RunResult<TestbedConfig>> {
        let elapsed = self.started_at.elapsed();
        let Self {
            config,
            handles,
            dag_writer,
            ..
        } = self;

        let mut syncers = Vec::with_capacity(handles.len());
        for handle in handles {
            syncers.push(handle.shutdown().await);
        }
        let (snapshots, storages): (Vec<_>, Vec<&_>) = syncers
            .iter()
            .map(|syncer| {
                let core = syncer.core();
                (core.metrics.collect(), core.storage())
            })
            .unzip();

        let mut writer = dag_writer;
        let mut builder =
            RunResult::builder(snapshots, &storages, config, elapsed, RunKind::Testbed);
        if let Some(w) = &mut writer {
            builder = builder.with_dag_log(&mut **w);
        }
        let result = builder.collect()?;
        // Flush before drop: BufWriter's `Drop` flushes silently, swallowing errors. The
        // DAG log is the only artefact the runner writes mid-run, so a flush failure here
        // should surface to the caller.
        if let Some(mut w) = writer {
            w.flush().wrap_err("flushing DAG log")?;
        }
        Ok(result)
    }
}
