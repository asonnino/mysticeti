// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Per-benchmark-run state. A [`BenchmarkSession`] is created once per run via
//! [`BenchmarkSession::new`] and advanced one tick at a time via
//! [`BenchmarkSession::tick`].

use tokio::time::{self, Instant, Interval};

use crate::{
    benchmark::Parameters,
    collector::Collector,
    error::TestbedResult,
    faults::CrashRecoverySchedule,
    orchestrator::Orchestrator,
    protocol::{Protocol, ProtocolCommands, ProtocolMetrics},
    report::{MonitoringReport, TickReport},
};

/// Mutable state for a single benchmark run.
///
/// A session is single-use: create one per run with [`BenchmarkSession::new`],
/// drive it to completion with [`BenchmarkSession::tick`], then drop it.
pub struct BenchmarkSession<P: Protocol> {
    pub(crate) schedule: CrashRecoverySchedule,
    pub(crate) collector: Option<Collector<P::NodeParameters, P::ClientParameters>>,
    metrics_interval: Interval,
    faults_interval: Interval,
    /// Instant the session started; used to compute elapsed time on each tick.
    pub start: Instant,
}

impl<P: ProtocolCommands + ProtocolMetrics> BenchmarkSession<P> {
    /// Initialise a benchmark session. Consumes the first (immediately-firing)
    /// tick of each interval so the first call to [`Self::tick`] waits a full
    /// interval before returning.
    pub async fn new(
        orchestrator: &Orchestrator<P>,
        parameters: &Parameters<P>,
        monitoring: Option<&MonitoringReport>,
    ) -> TestbedResult<Self> {
        let (_, nodes, _) = orchestrator.select_instances(parameters)?;
        let schedule = CrashRecoverySchedule::new(parameters.settings.faults.clone(), nodes);
        let collector = monitoring
            .map(|r| {
                Collector::new(
                    &r.prometheus_address,
                    parameters.clone(),
                    orchestrator.protocol().metrics(),
                )
            })
            .transpose()?;

        let mut metrics_interval = time::interval(parameters.settings.scrape_interval);
        metrics_interval.tick().await;
        let mut faults_interval = time::interval(parameters.settings.faults.crash_interval());
        faults_interval.tick().await;

        Ok(Self {
            schedule,
            collector,
            metrics_interval,
            faults_interval,
            start: Instant::now(),
        })
    }

    /// Advance the session by one tick. Blocks until either the metrics
    /// interval or the faults interval fires, then returns a [`TickReport`].
    pub async fn tick(
        &mut self,
        orchestrator: &Orchestrator<P>,
        parameters: &Parameters<P>,
    ) -> TestbedResult<TickReport> {
        tokio::select! {
            _ = self.metrics_interval.tick() => {
                if let Some(collector) = self.collector.as_mut() {
                    collector.collect().await?;
                }
                let results = self.collector.as_ref().map(|c| c.results().to_yaml());
                Ok(TickReport::MetricsTick {
                    elapsed: self.start.elapsed(),
                    results,
                })
            }
            _ = self.faults_interval.tick() => {
                let action = orchestrator
                    .apply_faults_step(parameters, &mut self.schedule)
                    .await?;
                Ok(TickReport::FaultUpdate {
                    elapsed: self.start.elapsed(),
                    action,
                })
            }
        }
    }
}
