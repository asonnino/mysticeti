// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    num::NonZeroU64,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use futures::future::join_all;
use tokio::{
    select,
    sync::{Notify, mpsc, oneshot},
};

use crate::{
    authority::Authority,
    block::RoundNumber,
    committee::Committee,
    committee::Stake,
    consensus::{CommittedSubDag, DagConsensus},
    context::Ctx,
    core::{
        Core,
        block_handler::CommitHandler,
        core_thread::CoreDispatch,
        syncer::{Syncer, SyncerSignals},
    },
    crypto::CryptoVerifier,
    metrics::Metrics,
    storage::BlockReader,
    sync::{
        network::{Connection, Network, NetworkMessage},
        synchronizer::{BlockDisseminator, BlockFetcher, SynchronizerParameters},
    },
};

/// The maximum number of blocks that can be requested in a single message.
pub const MAXIMUM_BLOCK_REQUEST: usize = 10;

/// Which rounds advance under the short quorum cap instead of the leader cap.
#[derive(Clone)]
pub enum QuorumTimeoutRounds {
    None,
    Every,
    EveryNth(NonZeroU64),
    /// Every multiple of the live period in the cell (adaptive Steelhead;
    /// 0 encodes an infinite period, i.e. no quorum rounds).
    Dynamic(Arc<AtomicU64>),
}

/// Round-advance timeouts: a round's proposer holds the door up to `leader`
/// for the leader block(s), or up to `quorum` for stragglers past the quorum,
/// depending on which cap `quorum_rounds` assigns to the round.
#[derive(Clone)]
pub struct RoundTimeouts {
    pub leader: Duration,
    pub quorum: Duration,
    pub quorum_rounds: QuorumTimeoutRounds,
}

impl RoundTimeouts {
    pub fn for_round(&self, round: RoundNumber) -> Duration {
        let quorum_round = match &self.quorum_rounds {
            QuorumTimeoutRounds::None => false,
            QuorumTimeoutRounds::Every => true,
            QuorumTimeoutRounds::EveryNth(period) => round.is_multiple_of(period.get()),
            QuorumTimeoutRounds::Dynamic(cell) => match cell.load(Ordering::Relaxed) {
                0 => false,
                period => round.is_multiple_of(period),
            },
        };
        if quorum_round {
            self.quorum
        } else {
            self.leader
        }
    }
}

pub struct NetworkSyncer<C: Ctx, D: DagConsensus> {
    inner: Arc<NetworkSyncerInner<C, D>>,
    main_task: C::JoinHandle<()>,
    syncer_task: oneshot::Receiver<()>,
    stop: mpsc::Receiver<()>,
}

pub struct NetworkSyncerInner<C: Ctx, D: DagConsensus> {
    pub syncer: C::Dispatcher<D>,
    pub block_reader: BlockReader,
    pub notify: Arc<Notify>,
    committee: Arc<Committee>,
    quorum_threshold: Stake,
    crypto: CryptoVerifier,
    round_timeouts: RoundTimeouts,
    stop: mpsc::Sender<()>,
}

impl<C: Ctx, D: DagConsensus> NetworkSyncer<C, D> {
    pub fn start(
        network: Network,
        mut core: Core<C, D>,
        round_timeouts: RoundTimeouts,
        enable_synchronizer: bool,
        mut commit_handler: CommitHandler<C>,
        metrics: Arc<Metrics>,
        commit_consumer: Option<mpsc::Sender<CommittedSubDag>>,
    ) -> Self {
        let authority_index = core.authority();
        let notify = Arc::new(Notify::new());
        let committed = core.take_recovered_committed_blocks();
        commit_handler.recover_committed(committed);
        let committee = core.committee().clone();
        let quorum_threshold = core.quorum_threshold();
        let crypto = core.verifier();
        let wal_syncer = core.wal_syncer();
        let block_reader = core.block_reader().clone();
        let mut syncer = Syncer::new(
            core,
            SyncerSignals::new(notify.clone()),
            commit_handler,
            metrics.clone(),
        );
        let startup_commits = syncer.force_new_block(0);
        let syncer = C::create_dispatcher(syncer, commit_consumer.clone());
        let (stop_sender, stop_receiver) = mpsc::channel(1);
        // Occupy the only available permit, so that all
        // other calls to send() will block.
        stop_sender.try_send(()).unwrap();
        let inner = Arc::new(NetworkSyncerInner {
            notify,
            syncer,
            block_reader,
            committee,
            quorum_threshold,
            crypto,
            round_timeouts,
            stop: stop_sender.clone(),
        });
        let block_fetcher = Arc::new(BlockFetcher::start(
            authority_index,
            inner.clone(),
            metrics.clone(),
            enable_synchronizer,
        ));
        let main_task = C::spawn(Self::run(
            network,
            inner.clone(),
            block_fetcher,
            metrics.clone(),
            startup_commits,
            commit_consumer,
        ));
        let syncer_task = C::start_wal_syncer(wal_syncer, stop_sender);
        Self {
            inner,
            main_task,
            stop: stop_receiver,
            syncer_task,
        }
    }

    pub async fn shutdown(self) -> Syncer<C, D> {
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
        inner: Arc<NetworkSyncerInner<C, D>>,
        block_fetcher: Arc<BlockFetcher<C>>,
        metrics: Arc<Metrics>,
        startup_commits: Vec<CommittedSubDag>,
        commit_consumer: Option<mpsc::Sender<CommittedSubDag>>,
    ) {
        // The startup proposal runs on the raw syncer before the dispatcher exists, so its
        // recovery-time commits are forwarded here instead. The consumer channel closes only
        // once both this sender clone and the dispatcher's are dropped (after shutdown).
        if let Some(consumer) = &commit_consumer {
            for subdag in startup_commits {
                if consumer.send(subdag).await.is_err() {
                    break;
                }
            }
        }

        let mut connections: HashMap<usize, C::JoinHandle<Option<()>>> = HashMap::new();
        let round_timeout_task = C::spawn(Self::round_timeout_task(inner.clone()));
        let cleanup_task = C::spawn(Self::cleanup_task(inner.clone()));
        while let Some(connection) = inner.recv_or_stopped(network.connection_receiver()).await {
            let peer_id = connection.peer_id;
            if let Some(task) = connections.remove(&peer_id) {
                task.await.ok();
            }

            let sender = connection.sender.clone();
            let authority = Authority::from(peer_id);
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
                .chain([round_timeout_task, cleanup_task]),
        )
        .await;
        Arc::try_unwrap(block_fetcher)
            .unwrap_or_else(|_| panic!("Failed to drop all connections"))
            .shutdown()
            .await;
    }

    #[tracing::instrument(
        skip_all,
        fields(peer = %Authority::from(connection.peer_id))
    )]
    async fn connection_task(
        mut connection: Connection,
        inner: Arc<NetworkSyncerInner<C, D>>,
        block_fetcher: Arc<BlockFetcher<C>>,
        metrics: Arc<Metrics>,
    ) -> Option<()> {
        let last_seen = inner
            .block_reader
            .last_seen_by_authority(Authority::from(connection.peer_id));
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

        let id = Authority::from(connection.peer_id);
        inner.syncer.authority_connection(id, true).await;

        let peer = id;
        while let Some(message) = inner.recv_or_stopped(&mut connection.receiver).await {
            match message {
                NetworkMessage::SubscribeOwnFrom(round) => {
                    disseminator.disseminate_own_blocks(round).await
                }
                NetworkMessage::Block(block) => {
                    tracing::debug!("Received {} from {}", block.reference(), peer);
                    if let Err(e) =
                        block.verify(&inner.committee, inner.quorum_threshold, &inner.crypto)
                    {
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
                    let authority = Authority::from(connection.peer_id);
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

    #[tracing::instrument(level = "debug", skip_all)]
    async fn round_timeout_task(inner: Arc<NetworkSyncerInner<C, D>>) -> Option<()> {
        let round_timeouts = inner.round_timeouts.clone();
        loop {
            let notified = inner.notify.notified();
            let round = inner
                .block_reader
                .last_own_block_ref()
                .map(|b| b.round())
                .unwrap_or_default();
            select! {
                _sleep = C::sleep(round_timeouts.for_round(round)) => {
                    tracing::debug!("Timeout {round}");
                    // todo - more then one round timeout can happen, need to fix this
                    inner.syncer.force_new_block(round).await;
                }
                _notified = notified => {
                    // restart loop
                }
                _stopped = inner.stopped() => {
                    return None;
                }
            }
        }
    }

    async fn cleanup_task(inner: Arc<NetworkSyncerInner<C, D>>) -> Option<()> {
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

impl<C: Ctx, D: DagConsensus> NetworkSyncerInner<C, D> {
    // Returns None either if channel is closed or NetworkSyncerInner receives stop signal
    async fn recv_or_stopped<T>(&self, channel: &mut mpsc::Receiver<T>) -> Option<T> {
        select! {
            stopped = self.stop.send(()) => {
                assert!(stopped.is_err());
                None
            }
            data = channel.recv() => {
                data
            }
        }
    }

    async fn stopped(&self) {
        let stopped = self.stop.send(()).await;
        assert!(stopped.is_err());
    }
}
