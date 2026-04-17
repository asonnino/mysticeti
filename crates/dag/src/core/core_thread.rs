// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, future::Future, sync::Arc, thread};

use tokio::sync::{mpsc, oneshot};

use super::syncer::Syncer;
use crate::{
    consensus::DagConsensus,
    context::Ctx,
    data::Data,
    metrics::Metrics,
    types::{Authority, BlockReference, RoundNumber, StatementBlock},
};

pub trait CoreDispatch<C: Ctx, D: DagConsensus>: Send + Sync {
    fn add_blocks(&self, blocks: Vec<Data<StatementBlock>>)
    -> impl Future<Output = ()> + Send + '_;

    fn force_new_block(&self, round: RoundNumber) -> impl Future<Output = ()> + Send + '_;

    fn cleanup(&self) -> impl Future<Output = ()> + Send + '_;

    fn get_missing_blocks(&self) -> impl Future<Output = Vec<HashSet<BlockReference>>> + Send + '_;

    fn authority_connection(
        &self,
        authority: Authority,
        connected: bool,
    ) -> impl Future<Output = ()> + Send + '_;

    fn stop(self) -> Syncer<C, D>;
}

enum CoreThreadCommand {
    AddBlocks(Vec<Data<StatementBlock>>, oneshot::Sender<()>),
    ForceNewBlock(RoundNumber, oneshot::Sender<()>),
    Cleanup(oneshot::Sender<()>),
    GetMissing(oneshot::Sender<Vec<HashSet<BlockReference>>>),
    ConnectionEstablished(Authority, oneshot::Sender<()>),
    ConnectionDropped(Authority, oneshot::Sender<()>),
}

pub struct ThreadedDispatcher<C: Ctx, D: DagConsensus> {
    sender: mpsc::Sender<CoreThreadCommand>,
    join_handle: thread::JoinHandle<Syncer<C, D>>,
    metrics: Arc<Metrics>,
}

impl<C: Ctx, D: DagConsensus> ThreadedDispatcher<C, D> {
    pub fn new(syncer: Syncer<C, D>) -> Self {
        let (sender, receiver) = mpsc::channel(32);
        let metrics = syncer.core().metrics.clone();
        let core_thread = CoreThread { syncer, receiver };
        let join_handle = thread::Builder::new()
            .name("dag".to_string())
            .spawn(move || core_thread.run())
            .unwrap();
        Self {
            sender,
            join_handle,
            metrics,
        }
    }

    async fn send(&self, command: CoreThreadCommand) {
        self.metrics.inc_core_lock_enqueued();
        if self.sender.send(command).await.is_err() {
            panic!("core thread is not expected to stop");
        }
    }
}

impl<C: Ctx, D: DagConsensus> CoreDispatch<C, D> for ThreadedDispatcher<C, D> {
    async fn add_blocks(&self, blocks: Vec<Data<StatementBlock>>) {
        let (tx, rx) = oneshot::channel();
        self.send(CoreThreadCommand::AddBlocks(blocks, tx)).await;
        rx.await.expect("core thread is not expected to stop");
    }

    async fn force_new_block(&self, round: RoundNumber) {
        let (tx, rx) = oneshot::channel();
        self.send(CoreThreadCommand::ForceNewBlock(round, tx)).await;
        rx.await.expect("core thread is not expected to stop");
    }

    async fn cleanup(&self) {
        let (tx, rx) = oneshot::channel();
        self.send(CoreThreadCommand::Cleanup(tx)).await;
        rx.await.expect("core thread is not expected to stop");
    }

    async fn get_missing_blocks(&self) -> Vec<HashSet<BlockReference>> {
        let (tx, rx) = oneshot::channel();
        self.send(CoreThreadCommand::GetMissing(tx)).await;
        rx.await.expect("core thread is not expected to stop")
    }

    async fn authority_connection(&self, authority: Authority, connected: bool) {
        let (tx, rx) = oneshot::channel();
        let command = if connected {
            CoreThreadCommand::ConnectionEstablished(authority, tx)
        } else {
            CoreThreadCommand::ConnectionDropped(authority, tx)
        };
        self.send(command).await;
        rx.await.expect("core thread is not expected to stop");
    }

    fn stop(self) -> Syncer<C, D> {
        drop(self.sender);
        self.join_handle.join().unwrap()
    }
}

struct CoreThread<C: Ctx, D: DagConsensus> {
    syncer: Syncer<C, D>,
    receiver: mpsc::Receiver<CoreThreadCommand>,
}

impl<C: Ctx, D: DagConsensus> CoreThread<C, D> {
    fn run(mut self) -> Syncer<C, D> {
        tracing::info!("Started core thread with tid {}", gettid::gettid());
        let metrics = self.syncer.core().metrics.clone();
        while let Some(command) = self.receiver.blocking_recv() {
            let _timer = metrics.core_lock_utilization_timer();
            metrics.inc_core_lock_dequeued();
            match command {
                CoreThreadCommand::AddBlocks(blocks, sender) => {
                    self.syncer.add_blocks(blocks);
                    sender.send(()).ok();
                }
                CoreThreadCommand::ForceNewBlock(round, sender) => {
                    self.syncer.force_new_block(round);
                    sender.send(()).ok();
                }
                CoreThreadCommand::Cleanup(sender) => {
                    self.syncer.core().cleanup();
                    sender.send(()).ok();
                }
                CoreThreadCommand::GetMissing(sender) => {
                    let missing = self.syncer.core().block_manager().missing_blocks();
                    sender.send(missing.to_vec()).ok();
                }
                CoreThreadCommand::ConnectionEstablished(authority, sender) => {
                    self.syncer.connect_authority(authority);
                    sender.send(()).ok();
                }
                CoreThreadCommand::ConnectionDropped(authority, sender) => {
                    self.syncer.disconnect_authority(authority);
                    sender.send(()).ok();
                }
            }
        }
        self.syncer
    }
}
