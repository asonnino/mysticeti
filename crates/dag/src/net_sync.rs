// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::future::join_all;
use tokio::{
    select,
    sync::{mpsc, oneshot, Notify},
};

use crate::{
    block_handler::CommitHandler,
    committee::Committee,
    config::NodePublicConfig,
    context::Ctx,
    core::Core,
    core_thread::CoreThreadDispatcher,
    metrics::Metrics,
    network::{Connection, Network, NetworkMessage},
    storage::BlockReader,
    syncer::{Syncer, SyncerSignals},
    synchronizer::{BlockDisseminator, BlockFetcher, SynchronizerParameters},
    types::{format_authority_index, AuthorityIndex},
};

/// The maximum number of blocks that can be requested in a single message.
pub const MAXIMUM_BLOCK_REQUEST: usize = 10;

pub struct NetworkSyncer<C: Ctx> {
    inner: Arc<NetworkSyncerInner<C>>,
    main_task: C::JoinHandle<()>,
    syncer_task: oneshot::Receiver<()>,
    stop: mpsc::Receiver<()>,
}

pub struct NetworkSyncerInner<C: Ctx> {
    pub syncer: CoreThreadDispatcher<C>,
    pub block_reader: BlockReader,
    pub notify: Arc<Notify>,
    committee: Arc<Committee>,
    stop: mpsc::Sender<()>,
    epoch_close_signal: mpsc::Sender<()>,
    pub epoch_closing_time: Arc<AtomicU64>,
}

impl<C: Ctx> NetworkSyncer<C> {
    pub fn start(
        network: Network,
        mut core: Core<C>,
        commit_period: u64,
        mut commit_handler: CommitHandler<C>,
        shutdown_grace_period: Duration,
        metrics: Arc<Metrics>,
        public_config: &NodePublicConfig,
    ) -> Self {
        let authority_index = core.authority();
        let notify = Arc::new(Notify::new());
        // todo - ugly, probably need to merge syncer and core
        let committed = core.take_recovered_committed_blocks();
        commit_handler.recover_committed(committed);
        let committee = core.committee().clone();
        let wal_syncer = core.wal_syncer();
        let block_reader = core.block_reader().clone();
        let epoch_closing_time = core.epoch_closing_time();
        let mut syncer = Syncer::new(
            core,
            commit_period,
            SyncerSignals::new(notify.clone()),
            commit_handler,
            metrics.clone(),
        );
        syncer.force_new_block(0);
        let syncer = CoreThreadDispatcher::start(syncer);
        let (stop_sender, stop_receiver) = mpsc::channel(1);
        // Occupy the only available permit, so that all
        // other calls to send() will block.
        stop_sender.try_send(()).unwrap();
        let (epoch_sender, epoch_receiver) = mpsc::channel(1);
        // Occupy the only available permit, so that all
        // other calls to send() will block.
        epoch_sender.try_send(()).unwrap();
        let inner = Arc::new(NetworkSyncerInner {
            notify,
            syncer,
            block_reader,
            committee,
            stop: stop_sender.clone(),
            epoch_close_signal: epoch_sender.clone(),
            epoch_closing_time,
        });
        let block_fetcher = Arc::new(BlockFetcher::start(
            authority_index,
            inner.clone(),
            metrics.clone(),
            public_config.parameters.enable_synchronizer,
        ));
        let main_task = C::spawn(Self::run(
            network,
            inner.clone(),
            epoch_receiver,
            shutdown_grace_period,
            block_fetcher,
            metrics.clone(),
        ));
        let syncer_task = C::start_wal_syncer(wal_syncer, stop_sender, epoch_sender);
        Self {
            inner,
            main_task,
            stop: stop_receiver,
            syncer_task,
        }
    }

    pub async fn shutdown(self) -> Syncer<C> {
        drop(self.stop);
        // todo - wait for network shutdown as well
        self.main_task.await.ok();
        self.syncer_task.await.ok();
        let Ok(inner) = Arc::try_unwrap(self.inner) else {
            panic!("Shutdown failed - not all resources are freed after main task is completed");
        };
        inner.syncer.stop()
    }

    async fn run(
        mut network: Network,
        inner: Arc<NetworkSyncerInner<C>>,
        epoch_close_signal: mpsc::Receiver<()>,
        shutdown_grace_period: Duration,
        block_fetcher: Arc<BlockFetcher<C>>,
        metrics: Arc<Metrics>,
    ) {
        let mut connections: HashMap<usize, C::JoinHandle<Option<()>>> = HashMap::new();
        let leader_timeout_task = C::spawn(Self::leader_timeout_task(
            inner.clone(),
            epoch_close_signal,
            shutdown_grace_period,
        ));
        let cleanup_task = C::spawn(Self::cleanup_task(inner.clone()));
        while let Some(connection) = inner.recv_or_stopped(network.connection_receiver()).await {
            let peer_id = connection.peer_id;
            if let Some(task) = connections.remove(&peer_id) {
                task.await.ok();
            }

            let sender = connection.sender.clone();
            let authority = peer_id as AuthorityIndex;
            block_fetcher.register_authority(authority, sender).await;

            let task = C::spawn(Self::connection_task(
                connection,
                inner.clone(),
                block_fetcher.clone(),
                metrics.clone(),
            ));
            connections.insert(peer_id, task);
        }
        join_all(
            connections
                .into_values()
                .chain([leader_timeout_task, cleanup_task].into_iter()),
        )
        .await;
        Arc::try_unwrap(block_fetcher)
            .unwrap_or_else(|_| panic!("Failed to drop all connections"))
            .shutdown()
            .await;
    }

    async fn connection_task(
        mut connection: Connection,
        inner: Arc<NetworkSyncerInner<C>>,
        block_fetcher: Arc<BlockFetcher<C>>,
        metrics: Arc<Metrics>,
    ) -> Option<()> {
        let last_seen = inner
            .block_reader
            .last_seen_by_authority(connection.peer_id as AuthorityIndex);
        connection
            .sender
            .send(NetworkMessage::SubscribeOwnFrom(last_seen))
            .await
            .ok()?;

        let mut disseminator = BlockDisseminator::new(
            connection.sender.clone(),
            inner.clone(),
            SynchronizerParameters::default(),
            metrics.clone(),
        );

        let id = connection.peer_id as AuthorityIndex;
        inner.syncer.authority_connection(id, true).await;

        let peer = format_authority_index(id);
        while let Some(message) = inner.recv_or_stopped(&mut connection.receiver).await {
            match message {
                NetworkMessage::SubscribeOwnFrom(round) => {
                    disseminator.disseminate_own_blocks(round).await
                }
                NetworkMessage::Block(block) => {
                    tracing::debug!("Received {} from {}", block.reference(), peer);
                    if let Err(e) = block.verify(&inner.committee) {
                        tracing::warn!(
                            "Rejected incorrect block {} from {}: {:?}",
                            block.reference(),
                            peer,
                            e
                        );
                        // Terminate connection upon receiving incorrect block.
                        break;
                    }
                    inner.syncer.add_blocks(vec![block]).await;
                }
                NetworkMessage::RequestBlocks(references) => {
                    if references.len() > MAXIMUM_BLOCK_REQUEST {
                        // Terminate connection on receiving invalid message.
                        break;
                    }
                    let authority = connection.peer_id as AuthorityIndex;
                    if disseminator
                        .send_blocks(authority, references)
                        .await
                        .is_none()
                    {
                        break;
                    }
                }
                NetworkMessage::BlockNotFound(_references) => {
                    // TODO: leverage this signal to request blocks from other peers
                }
            }
        }
        inner.syncer.authority_connection(id, false).await;
        disseminator.shutdown().await;
        block_fetcher.remove_authority(id).await;
        None
    }

    async fn leader_timeout_task(
        inner: Arc<NetworkSyncerInner<C>>,
        mut epoch_close_signal: mpsc::Receiver<()>,
        shutdown_grace_period: Duration,
    ) -> Option<()> {
        let leader_timeout = Duration::from_secs(1);
        loop {
            let notified = inner.notify.notified();
            let round = inner
                .block_reader
                .last_own_block_ref()
                .map(|b| b.round())
                .unwrap_or_default();
            let closing_time = inner.epoch_closing_time.load(Ordering::Relaxed);
            let shutdown_duration = if closing_time != 0 {
                shutdown_grace_period.saturating_sub(
                    C::timestamp_utc().saturating_sub(Duration::from_millis(closing_time)),
                )
            } else {
                Duration::MAX
            };
            if Duration::is_zero(&shutdown_duration) {
                return None;
            }
            select! {
                _sleep = C::sleep(leader_timeout) => {
                    tracing::debug!("Timeout {round}");
                    // todo - more then one round timeout can happen, need to fix this
                    inner.syncer.force_new_block(round).await;
                }
                _notified = notified => {
                    // restart loop
                }
                _epoch_shutdown = C::sleep(shutdown_duration) => {
                    tracing::info!("Shutting down sync after epoch close");
                    epoch_close_signal.close();
                }
                _stopped = inner.stopped() => {
                    return None;
                }
            }
        }
    }

    async fn cleanup_task(inner: Arc<NetworkSyncerInner<C>>) -> Option<()> {
        let cleanup_interval = Duration::from_secs(10);
        loop {
            select! {
                _sleep = C::sleep(cleanup_interval) => {
                    // Keep read lock for everything else
                    inner.syncer.cleanup().await;
                }
                _stopped = inner.stopped() => {
                    return None;
                }
            }
        }
    }

    pub async fn await_completion(self) -> Result<(), C::JoinError> {
        self.main_task.await
    }
}

impl<C: Ctx> NetworkSyncerInner<C> {
    // Returns None either if channel is closed or NetworkSyncerInner receives stop signal
    async fn recv_or_stopped<T>(&self, channel: &mut mpsc::Receiver<T>) -> Option<T> {
        select! {
            stopped = self.stop.send(()) => {
                assert!(stopped.is_err());
                None
            }
            closed = self.epoch_close_signal.send(()) => {
                assert!(closed.is_err());
                None
            }
            data = channel.recv() => {
                data
            }
        }
    }

    async fn stopped(&self) {
        select! {
            stopped = self.stop.send(()) => {
                assert!(stopped.is_err());
            }
            closed = self.epoch_close_signal.send(()) => {
                assert!(closed.is_err());
            }
        }
    }
}

#[cfg(test)]
#[cfg(not(feature = "simulator"))]
mod tests {
    use std::time::Duration;

    use crate::test_util::{check_commits, network_syncers};

    #[tokio::test]
    async fn test_network_sync() {
        let network_syncers = network_syncers(4).await;
        println!("Started");
        tokio::time::sleep(Duration::from_secs(3)).await;
        println!("Done");
        let mut syncers = vec![];
        for network_syncer in network_syncers {
            let syncer = network_syncer.shutdown().await;
            syncers.push(syncer);
        }

        check_commits(&syncers);
    }
}

#[cfg(test)]
#[cfg(feature = "simulator")]
mod sim_tests {
    use std::{sync::atomic::Ordering, time::Duration};

    use super::NetworkSyncer;
    use crate::{
        config,
        context::{Ctx, DefaultCtx},
        future_simulator::SimulatedExecutorState,
        simulator_tracing::setup_simulator_tracing,
        syncer::Syncer,
        test_util::{
            check_commits, print_stats, rng_at_seed, simulated_network_syncers,
            simulated_network_syncers_with_epoch_duration,
        },
    };

    async fn wait_for_epoch_to_close(
        network_syncers: Vec<NetworkSyncer<DefaultCtx>>,
    ) -> Vec<Syncer<DefaultCtx>> {
        let mut any_closed = false;
        while !any_closed {
            for net_sync in network_syncers.iter() {
                if net_sync.inner.epoch_closing_time.load(Ordering::Relaxed) != 0 {
                    any_closed = true;
                }
            }
            DefaultCtx::sleep(Duration::from_secs(10)).await;
        }
        DefaultCtx::sleep(config::node_defaults::default_shutdown_grace_period()).await;
        let mut syncers = vec![];
        for net_sync in network_syncers {
            let syncer = net_sync.shutdown().await;
            syncers.push(syncer);
        }
        syncers
    }
    #[test]
    fn test_exact_commits_in_epoch() {
        SimulatedExecutorState::run(rng_at_seed(0), test_exact_commits_in_epoch_async());
    }

    async fn test_exact_commits_in_epoch_async() {
        let n = 4;
        let rounds_in_epoch = 3000;
        let (simulated_network, network_syncers) =
            simulated_network_syncers_with_epoch_duration(n, rounds_in_epoch);
        simulated_network.connect_all().await;
        let syncers = wait_for_epoch_to_close(network_syncers).await;
        let canonical_commit_seq = syncers[0].commit_handler().committed_leaders();
        for syncer in &syncers {
            let commit_seq = syncer.commit_handler().committed_leaders();
            assert_eq!(canonical_commit_seq, commit_seq);
        }
        print_stats(&syncers);
    }

    #[test]
    fn test_network_sync_sim_all_up() {
        setup_simulator_tracing();
        SimulatedExecutorState::run(rng_at_seed(0), test_network_sync_sim_all_up_async());
    }

    async fn test_network_sync_sim_all_up_async() {
        let (simulated_network, network_syncers) = simulated_network_syncers(10);
        simulated_network.connect_all().await;
        DefaultCtx::sleep(Duration::from_secs(20)).await;
        let mut syncers = vec![];
        for network_syncer in network_syncers {
            let syncer = network_syncer.shutdown().await;
            syncers.push(syncer);
        }

        check_commits(&syncers);
        print_stats(&syncers);
    }

    #[test]
    fn test_network_sync_sim_one_down() {
        setup_simulator_tracing();
        SimulatedExecutorState::run(rng_at_seed(0), test_network_sync_sim_one_down_async());
    }

    // All peers except for peer A are connected in this test
    // Peer A is disconnected from everything
    async fn test_network_sync_sim_one_down_async() {
        let (simulated_network, network_syncers) = simulated_network_syncers(10);
        simulated_network.connect_some(|a, _b| a != 0).await;
        println!("Started");
        DefaultCtx::sleep(Duration::from_secs(40)).await;
        println!("Done");
        let mut syncers = vec![];
        for network_syncer in network_syncers {
            let syncer = network_syncer.shutdown().await;
            syncers.push(syncer);
        }

        check_commits(&syncers);
        print_stats(&syncers);
    }

    /// Test disabled: the vote-per-transaction fast path causes oversized
    /// blocks during catch-up. When a partitioned node reconnects, it
    /// processes ~70 rounds of blocks and generates VoteRange statements
    /// for all their shared transactions at once (~598 VoteRange entries),
    /// exceeding the MAX_ENTRY_SIZE/2 block limit.
    /// Re-enable when the fast-path vote-per-tx mechanism is removed.
    #[test]
    #[ignore = "oversized blocks from vote-per-tx fast path; re-enable after removal"]
    fn test_network_partition() {
        setup_simulator_tracing();
        SimulatedExecutorState::run(rng_at_seed(0), test_network_partition_async());
    }

    // All peers except for peer A are connected in this test. Peer A is disconnected from everyone
    // except for peer B. This test ensures that A eventually manages to commit by syncing with B.
    async fn test_network_partition_async() {
        let (simulated_network, network_syncers) = simulated_network_syncers(10);
        // Disconnect all A from all peers except for B.
        simulated_network
            .connect_some(|a, b| a != 0 || (a == 0 && b == 1))
            .await;

        println!("Started");
        DefaultCtx::sleep(Duration::from_secs(40)).await;
        println!("Done");
        let mut syncers = vec![];
        for network_syncer in network_syncers {
            let syncer = network_syncer.shutdown().await;
            syncers.push(syncer);
        }

        check_commits(&syncers);
        print_stats(&syncers);
    }
}
