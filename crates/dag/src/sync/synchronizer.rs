// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, sync::Arc, time::Duration};

use futures::future::join_all;
use rand::{seq::SliceRandom, thread_rng};
use tokio::sync::mpsc;

use crate::{
    authority::Authority,
    block::{BlockReference, RoundNumber},
    consensus::DagConsensus,
    context::Ctx,
    core::core_thread::CoreDispatch,
    metrics::Metrics,
    sync::{
        net_sync::{self, NetworkSyncerInner},
        network::NetworkMessage,
    },
};

pub struct SynchronizerParameters {
    pub maximum_helpers_per_authority: usize,
    pub batch_size: usize,
    pub sample_precision: Duration,
    pub grace_period: Duration,
    pub stream_interval: Duration,
}

impl Default for SynchronizerParameters {
    fn default() -> Self {
        Self {
            maximum_helpers_per_authority: 2,
            batch_size: 10,
            sample_precision: Duration::from_secs(5),
            grace_period: Duration::from_secs(15),
            stream_interval: Duration::from_secs(1),
        }
    }
}

pub struct BlockDisseminator<C: Ctx, D: DagConsensus> {
    sender: mpsc::Sender<NetworkMessage>,
    inner: Arc<NetworkSyncerInner<C, D>>,
    own_blocks: Option<C::JoinHandle<Option<()>>>,
    other_blocks: Vec<C::JoinHandle<Option<()>>>,
    parameters: SynchronizerParameters,
    metrics: Arc<Metrics>,
}

impl<C: Ctx, D: DagConsensus + Send + 'static> BlockDisseminator<C, D> {
    pub fn new(
        sender: mpsc::Sender<NetworkMessage>,
        inner: Arc<NetworkSyncerInner<C, D>>,
        parameters: SynchronizerParameters,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            sender,
            inner,
            own_blocks: None,
            other_blocks: Vec::new(),
            parameters,
            metrics,
        }
    }

    pub async fn shutdown(mut self) {
        let mut waiters = Vec::with_capacity(1 + self.other_blocks.len());
        if let Some(handle) = self.own_blocks.take() {
            C::abort(&handle);
            waiters.push(handle);
        }
        for handle in self.other_blocks {
            C::abort(&handle);
            waiters.push(handle);
        }
        join_all(waiters).await;
    }

    pub async fn send_blocks(
        &mut self,
        peer: Authority,
        references: Vec<BlockReference>,
    ) -> Option<()> {
        let mut missing = Vec::new();
        for reference in references {
            let stored_block = self.inner.block_reader.get_block(reference);
            let found = stored_block.is_some();
            match stored_block {
                Some(block) => self.sender.send(NetworkMessage::Block(block)).await.ok()?,
                None => missing.push(reference),
            }
            self.metrics
                .inc_block_sync_requests_received(&peer.to_string(), &found.to_string());
        }
        self.sender
            .send(NetworkMessage::BlockNotFound(missing))
            .await
            .ok()
    }

    pub async fn disseminate_own_blocks(&mut self, round: RoundNumber) {
        if let Some(existing) = self.own_blocks.take() {
            C::abort(&existing);
            existing.await.ok();
        }

        let handle = C::spawn(Self::stream_own_blocks(
            self.sender.clone(),
            self.inner.clone(),
            round,
            self.parameters.batch_size,
        ));
        self.own_blocks = Some(handle);
    }

    async fn stream_own_blocks(
        to: mpsc::Sender<NetworkMessage>,
        inner: Arc<NetworkSyncerInner<C, D>>,
        mut round: RoundNumber,
        batch_size: usize,
    ) -> Option<()> {
        loop {
            let notified = inner.notify.notified();
            let blocks = inner.block_reader.get_own_blocks(round, batch_size);
            for block in blocks {
                round = block.round();
                to.send(NetworkMessage::Block(block)).await.ok()?;
            }
            notified.await
        }
    }

    #[allow(dead_code)]
    pub fn disseminate_others_blocks(&mut self, round: RoundNumber, author: Authority) {
        if self.other_blocks.len() >= self.parameters.maximum_helpers_per_authority {
            return;
        }

        let handle = C::spawn(Self::stream_others_blocks(
            self.sender.clone(),
            self.inner.clone(),
            round,
            author,
            self.parameters.batch_size,
            self.parameters.stream_interval,
        ));
        self.other_blocks.push(handle);
    }

    async fn stream_others_blocks(
        to: mpsc::Sender<NetworkMessage>,
        inner: Arc<NetworkSyncerInner<C, D>>,
        mut round: RoundNumber,
        author: Authority,
        batch_size: usize,
        stream_interval: Duration,
    ) -> Option<()> {
        loop {
            let blocks = inner
                .block_reader
                .get_others_blocks(round, author, batch_size);
            for block in blocks {
                round = block.round();
                to.send(NetworkMessage::Block(block)).await.ok()?;
            }
            C::sleep(stream_interval).await;
        }
    }
}

enum BlockFetcherMessage {
    RegisterAuthority(Authority, mpsc::Sender<NetworkMessage>),
    RemoveAuthority(Authority),
}

pub struct BlockFetcher<C: Ctx> {
    sender: mpsc::Sender<BlockFetcherMessage>,
    handle: C::JoinHandle<Option<()>>,
}

impl<C: Ctx> BlockFetcher<C> {
    pub fn start<D: DagConsensus + Send + 'static>(
        id: Authority,
        inner: Arc<NetworkSyncerInner<C, D>>,
        metrics: Arc<Metrics>,
        enable: bool,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(100);
        let worker = BlockFetcherWorker::new(id, inner, receiver, metrics, enable);
        let handle = C::spawn(worker.run());
        Self { sender, handle }
    }

    pub async fn register_authority(
        &self,
        authority: Authority,
        sender: mpsc::Sender<NetworkMessage>,
    ) {
        self.sender
            .send(BlockFetcherMessage::RegisterAuthority(authority, sender))
            .await
            .ok();
    }

    pub async fn remove_authority(&self, authority: Authority) {
        self.sender
            .send(BlockFetcherMessage::RemoveAuthority(authority))
            .await
            .ok();
    }

    pub async fn shutdown(self) {
        C::abort(&self.handle);
        self.handle.await.ok();
    }
}

struct BlockFetcherWorker<C: Ctx, D: DagConsensus> {
    id: Authority,
    inner: Arc<NetworkSyncerInner<C, D>>,
    receiver: mpsc::Receiver<BlockFetcherMessage>,
    senders: HashMap<Authority, mpsc::Sender<NetworkMessage>>,
    parameters: SynchronizerParameters,
    metrics: Arc<Metrics>,
    missing: HashMap<BlockReference, Duration>,
    enable: bool,
}

impl<C: Ctx, D: DagConsensus + Send + 'static> BlockFetcherWorker<C, D> {
    pub fn new(
        id: Authority,
        inner: Arc<NetworkSyncerInner<C, D>>,
        receiver: mpsc::Receiver<BlockFetcherMessage>,
        metrics: Arc<Metrics>,
        enable: bool,
    ) -> Self {
        Self {
            id,
            inner,
            receiver,
            senders: Default::default(),
            parameters: Default::default(),
            metrics,
            missing: Default::default(),
            enable,
        }
    }

    async fn run(mut self) -> Option<()> {
        loop {
            tokio::select! {
                _ = C::sleep(self.parameters.sample_precision) => {
                    self.sync_strategy().await
                }
                message = self.receiver.recv() => {
                    match message {
                        Some(BlockFetcherMessage::RegisterAuthority(
                            authority, sender,
                        )) => {
                            self.senders.insert(authority, sender);
                        }
                        Some(BlockFetcherMessage::RemoveAuthority(
                            authority,
                        )) => {
                            self.senders.remove(&authority);
                        }
                        None => return None,
                    }
                }
            }
        }
    }

    async fn sync_strategy(&mut self) {
        if self.enable {
            return;
        }

        let now = C::timestamp_utc();
        let mut to_request = Vec::new();
        let missing_blocks = self.inner.syncer.get_missing_blocks().await;
        for (authority, missing) in missing_blocks.into_iter().enumerate() {
            self.metrics
                .set_missing_blocks(&authority.to_string(), missing.len() as i64);

            for reference in missing {
                let time = self.missing.entry(reference).or_insert(now);
                if now.checked_sub(*time).unwrap_or_default() >= self.parameters.grace_period {
                    to_request.push(reference);
                    self.missing.remove(&reference);
                }
            }
        }
        self.missing.retain(|_, time| {
            now.checked_sub(*time).unwrap_or_default() < self.parameters.grace_period
        });

        for chunks in to_request.chunks(net_sync::MAXIMUM_BLOCK_REQUEST) {
            let Some((peer, permit)) = self.sample_peer(&[self.id]) else {
                break;
            };
            let message = NetworkMessage::RequestBlocks(chunks.to_vec());
            permit.send(message);

            self.metrics.inc_block_sync_requests_sent(&peer.to_string());
        }
    }

    fn sample_peer(
        &self,
        except: &[Authority],
    ) -> Option<(Authority, mpsc::Permit<'_, NetworkMessage>)> {
        let mut senders = self
            .senders
            .iter()
            .filter(|&(index, _)| !except.contains(index))
            .collect::<Vec<_>>();

        senders.shuffle(&mut thread_rng());

        for (peer, sender) in senders {
            if let Ok(permit) = sender.try_reserve() {
                return Some((*peer, permit));
            }
        }
        None
    }
}
