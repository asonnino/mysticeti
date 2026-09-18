// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, fmt::Debug, sync::Arc};

use tokio::sync::mpsc;

use super::context::SimulatorContext;
use super::equivocator::Equivocator;
use super::latency::{LatencyModel, LinkLatency};
use dag::committee::Committee;
use dag::context::Ctx;
use dag::sync::network::{Connection, Network, NetworkMessage};

pub struct SimulatedNetwork {
    senders: Vec<mpsc::Sender<Connection>>,
    latency: LatencyModel,
    equivocators: HashMap<usize, Arc<Equivocator>>,
}

impl SimulatedNetwork {
    /// Panics on a `latency` that fails [`LatencyModel::validate`].
    pub fn new(committee: &Committee, latency: LatencyModel) -> (SimulatedNetwork, Vec<Network>) {
        latency.validate().expect("invalid latency model");
        let (networks, senders): (Vec<_>, Vec<_>) = committee
            .authorities()
            .map(|_| {
                let (sender, receiver) = mpsc::channel(16);
                (Network::new_from_raw(receiver), sender)
            })
            .unzip();
        (
            Self {
                senders,
                latency,
                equivocators: HashMap::new(),
            },
            networks,
        )
    }

    /// Make `leaders` equivocate in their leader rounds; `leader_count` is the cohort size.
    pub fn with_equivocating_leaders(mut self, leaders: &[usize], leader_count: usize) -> Self {
        let committee_size = self.senders.len();
        for &index in leaders {
            assert!(
                index < committee_size,
                "equivocating leader {index} is not in the committee"
            );
            let equivocator = Equivocator::new(index, committee_size, leader_count);
            self.equivocators.insert(index, Arc::new(equivocator));
        }
        self
    }

    pub async fn connect_all(&self) {
        for a in 0..self.senders.len() {
            for b in a + 1..self.senders.len() {
                self.connect(a, b).await
            }
        }
    }

    pub async fn connect_some<F: Fn(usize, usize) -> bool>(&self, should_connect: F) {
        for a in 0..self.senders.len() {
            for b in a + 1..self.senders.len() {
                if should_connect(a, b) {
                    self.connect(a, b).await
                }
            }
        }
    }

    pub async fn connect(&self, a: usize, b: usize) {
        // `a_receiver` is what `a` hears, i.e. the `b -> a` direction.
        let (a_sender, a_receiver) = Self::latency_channel(self.latency.link(b, a));
        let (b_sender, b_receiver) = Self::latency_channel(self.latency.link(a, b));
        let (a_inbound, b_inbound) = (a_sender.clone(), b_sender.clone());
        let a_connection = Connection {
            peer_id: b,
            sender: self.outbound(a, b, b_sender, a_inbound),
            receiver: a_receiver,
        };
        let b_connection = Connection {
            peer_id: a,
            sender: self.outbound(b, a, a_sender, b_inbound),
            receiver: b_receiver,
        };
        let a = &self.senders[a];
        let b = &self.senders[b];
        a.send(a_connection).await.ok();
        b.send(b_connection).await.ok();
    }

    /// The `from -> to` link, routed through the equivocation shim when `from` equivocates.
    /// `inbound` is the `to -> from` direction of the same link, through which the shim
    /// reflects every twin back to `from` as if a peer had sent it (best effort).
    fn outbound(
        &self,
        from: usize,
        to: usize,
        sender: mpsc::Sender<NetworkMessage>,
        inbound: mpsc::Sender<NetworkMessage>,
    ) -> mpsc::Sender<NetworkMessage> {
        let Some(equivocator) = self.equivocators.get(&from).cloned() else {
            return sender;
        };
        let (shim_sender, mut shim_receiver) = mpsc::channel(16);
        SimulatorContext::spawn(async move {
            while let Some(message) = shim_receiver.recv().await {
                let (messages, reflected) = equivocator.rewrite(message, to);
                for message in messages {
                    if sender.send(message).await.is_err() {
                        return;
                    }
                }
                // Never await the reflection: `inbound` is the reverse queue of this very
                // link, which `from`'s own connection task drains, and that task may be
                // waiting on us. A dropped copy is harmless since every link reflects the
                // same twin and the block manager deduplicates it.
                if let Some(twin) = reflected
                    && let Err(error) = inbound.try_send(twin)
                {
                    tracing::debug!("Dropping reflected twin on the {from} -> {to} link: {error}");
                }
            }
        });
        shim_sender
    }

    fn latency_channel<T: Send + 'static + Debug>(
        link: LinkLatency,
    ) -> (mpsc::Sender<T>, mpsc::Receiver<T>) {
        let (buf_sender, mut buf_receiver) = mpsc::channel(16);
        let (sender, receiver) = mpsc::channel(16);
        SimulatorContext::spawn(async move {
            while let Some(message) = buf_receiver.recv().await {
                let latency = SimulatorContext::with_rng(|rng| link.sample(rng));
                SimulatorContext::sleep(latency).await;
                if sender.send(message).await.is_err() {
                    return;
                }
            }
        });
        (buf_sender, receiver)
    }
}
