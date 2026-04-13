// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, sync::Arc, thread};

use tokio::sync::{mpsc, oneshot};

#[cfg(any(test, feature = "simulator"))]
use parking_lot::Mutex;

use super::syncer::Syncer;
use crate::{
    consensus::DagConsensus,
    context::Ctx,
    data::Data,
    metrics::Metrics,
    types::{AuthorityIndex, BlockReference, RoundNumber, StatementBlock},
};

pub struct CoreThreadDispatcher<C: Ctx, D: DagConsensus> {
    inner: Inner<C, D>,
}

enum Inner<C: Ctx, D: DagConsensus> {
    Spawned {
        sender: mpsc::Sender<CoreThreadCommand>,
        join_handle: thread::JoinHandle<Syncer<C, D>>,
        metrics: Arc<Metrics>,
    },
    #[cfg(any(test, feature = "simulator"))]
    Test(Box<Mutex<Syncer<C, D>>>),
}

enum CoreThreadCommand {
    AddBlocks(Vec<Data<StatementBlock>>, oneshot::Sender<()>),
    ForceNewBlock(RoundNumber, oneshot::Sender<()>),
    Cleanup(oneshot::Sender<()>),
    GetMissing(oneshot::Sender<Vec<HashSet<BlockReference>>>),
    ConnectionEstablished(AuthorityIndex, oneshot::Sender<()>),
    ConnectionDropped(AuthorityIndex, oneshot::Sender<()>),
}

impl<C: Ctx, D: DagConsensus + Send + 'static> CoreThreadDispatcher<C, D> {
    pub fn start(syncer: Syncer<C, D>) -> Self {
        let (sender, receiver) = mpsc::channel(32);
        let metrics = syncer.core().metrics.clone();
        let core_thread = CoreThread { syncer, receiver };
        let join_handle = thread::Builder::new()
            .name("dag".to_string())
            .spawn(move || core_thread.run())
            .unwrap();
        Self {
            inner: Inner::Spawned {
                sender,
                join_handle,
                metrics,
            },
        }
    }

    #[cfg(any(test, feature = "simulator"))]
    pub fn new_for_test(syncer: Syncer<C, D>) -> Self {
        Self {
            inner: Inner::Test(Box::new(Mutex::new(syncer))),
        }
    }

    pub fn stop(self) -> Syncer<C, D> {
        match self.inner {
            Inner::Spawned {
                sender,
                join_handle,
                ..
            } => {
                drop(sender);
                join_handle.join().unwrap()
            }
            #[cfg(any(test, feature = "simulator"))]
            Inner::Test(mutex) => mutex.into_inner(),
        }
    }

    pub async fn add_blocks(&self, blocks: Vec<Data<StatementBlock>>) {
        match &self.inner {
            Inner::Spawned {
                sender, metrics, ..
            } => {
                let (tx, rx) = oneshot::channel();
                Self::send(sender, metrics, CoreThreadCommand::AddBlocks(blocks, tx)).await;
                rx.await.expect("core thread is not expected to stop");
            }
            #[cfg(any(test, feature = "simulator"))]
            Inner::Test(mutex) => {
                mutex.lock().add_blocks(blocks);
            }
        }
    }

    pub async fn force_new_block(&self, round: RoundNumber) {
        match &self.inner {
            Inner::Spawned {
                sender, metrics, ..
            } => {
                let (tx, rx) = oneshot::channel();
                Self::send(sender, metrics, CoreThreadCommand::ForceNewBlock(round, tx)).await;
                rx.await.expect("core thread is not expected to stop");
            }
            #[cfg(any(test, feature = "simulator"))]
            Inner::Test(mutex) => {
                mutex.lock().force_new_block(round);
            }
        }
    }

    pub async fn cleanup(&self) {
        match &self.inner {
            Inner::Spawned {
                sender, metrics, ..
            } => {
                let (tx, rx) = oneshot::channel();
                Self::send(sender, metrics, CoreThreadCommand::Cleanup(tx)).await;
                rx.await.expect("core thread is not expected to stop");
            }
            #[cfg(any(test, feature = "simulator"))]
            Inner::Test(mutex) => {
                mutex.lock().core().cleanup();
            }
        }
    }

    pub async fn get_missing_blocks(&self) -> Vec<HashSet<BlockReference>> {
        match &self.inner {
            Inner::Spawned {
                sender, metrics, ..
            } => {
                let (tx, rx) = oneshot::channel();
                Self::send(sender, metrics, CoreThreadCommand::GetMissing(tx)).await;
                rx.await.expect("core thread is not expected to stop")
            }
            #[cfg(any(test, feature = "simulator"))]
            Inner::Test(mutex) => mutex
                .lock()
                .core()
                .block_manager()
                .missing_blocks()
                .to_vec(),
        }
    }

    pub async fn authority_connection(&self, authority: AuthorityIndex, connected: bool) {
        match &self.inner {
            Inner::Spawned {
                sender, metrics, ..
            } => {
                let (tx, rx) = oneshot::channel();
                let command = if connected {
                    CoreThreadCommand::ConnectionEstablished(authority, tx)
                } else {
                    CoreThreadCommand::ConnectionDropped(authority, tx)
                };
                Self::send(sender, metrics, command).await;
                rx.await.expect("core thread is not expected to stop");
            }
            #[cfg(any(test, feature = "simulator"))]
            Inner::Test(mutex) => {
                let mut lock = mutex.lock();
                if connected {
                    lock.connected_authorities.insert(authority);
                } else {
                    lock.connected_authorities.remove(&authority);
                }
            }
        }
    }

    async fn send(
        sender: &mpsc::Sender<CoreThreadCommand>,
        metrics: &Metrics,
        command: CoreThreadCommand,
    ) {
        metrics.inc_core_lock_enqueued();
        if sender.send(command).await.is_err() {
            panic!("core thread is not expected to stop");
        }
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
                    self.syncer.connected_authorities.insert(authority);
                    sender.send(()).ok();
                }
                CoreThreadCommand::ConnectionDropped(authority, sender) => {
                    self.syncer.connected_authorities.remove(&authority);
                    sender.send(()).ok();
                }
            }
        }
        self.syncer
    }
}
