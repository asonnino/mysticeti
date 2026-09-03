// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{ops::Range, sync::Arc, time::Duration};

use rand::Rng;
use tokio::sync::mpsc;

use super::{conditions::NetworkConditions, context::SimulatorContext};
use dag::committee::Committee;
use dag::context::Ctx;
use dag::sync::network::{Connection, Network, NetworkMessage};

/// Latency of one link under the per-link model: a stable symmetric base plus
/// bounded per-message jitter.
#[derive(Clone, Copy)]
struct LinkLatency {
    base: Duration,
    jitter: Duration,
}

pub struct SimulatedNetwork {
    senders: Vec<mpsc::Sender<Connection>>,
    latency_range: Range<Duration>,
    /// Per-link model when set (WAN-like stable pairs); otherwise every
    /// message draws independently from `latency_range`.
    link_jitter: Option<Duration>,
    /// Timed network-condition schedule; `None` means healthy throughout.
    conditions: Option<Arc<NetworkConditions>>,
}

impl SimulatedNetwork {
    pub fn new(
        committee: &Committee,
        latency_range: Range<Duration>,
        link_jitter: Option<Duration>,
        conditions: Option<Arc<NetworkConditions>>,
    ) -> (SimulatedNetwork, Vec<Network>) {
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
                latency_range,
                link_jitter,
                conditions,
            },
            networks,
        )
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
        // One symmetric base latency per unordered pair, drawn at setup so it
        // is a stable property of the link rather than of message timing.
        let link_latency = self.link_jitter.map(|jitter| LinkLatency {
            base: SimulatorContext::with_rng(|rng| rng.gen_range(self.latency_range.clone())),
            jitter,
        });
        // Channel (b_sender → b_receiver) carries a→b traffic and vice versa.
        let (a_sender, a_receiver) = self.latency_channel(link_latency, b, a);
        let (b_sender, b_receiver) = self.latency_channel(link_latency, a, b);
        let a_connection = Connection {
            peer_id: b,
            sender: b_sender,
            receiver: a_receiver,
        };
        let b_connection = Connection {
            peer_id: a,
            sender: a_sender,
            receiver: b_receiver,
        };
        let a = &self.senders[a];
        let b = &self.senders[b];
        a.send(a_connection).await.ok();
        b.send(b_connection).await.ok();
    }

    fn latency_channel(
        &self,
        link_latency: Option<LinkLatency>,
        from: usize,
        to: usize,
    ) -> (mpsc::Sender<NetworkMessage>, mpsc::Receiver<NetworkMessage>) {
        let (buf_sender, mut buf_receiver) = mpsc::channel(16);
        let (sender, receiver) = mpsc::channel(16);
        let range = self.latency_range.clone();
        let conditions = self.conditions.clone();
        SimulatorContext::spawn(async move {
            while let Some(message) = buf_receiver.recv().await {
                let latency = match link_latency {
                    Some(link) if link.jitter.is_zero() => link.base,
                    Some(link) => {
                        let jitter = SimulatorContext::with_rng(|rng| {
                            rng.gen_range(Duration::ZERO..link.jitter)
                        });
                        link.base + jitter
                    }
                    None => SimulatorContext::with_rng(|rng| rng.gen_range(range.clone())),
                };
                let extra_delay = conditions
                    .as_ref()
                    .map(|conditions| conditions.extra_delay(from, to, &message))
                    .unwrap_or(Duration::ZERO);
                SimulatorContext::sleep(latency).await;
                if extra_delay.is_zero() {
                    if sender.send(message).await.is_err() {
                        return;
                    }
                } else {
                    // Deliver adversarially delayed messages concurrently: a
                    // held message must not block the link behind it, and the
                    // resulting reordering is exactly what asynchrony permits.
                    let sender = sender.clone();
                    SimulatorContext::spawn(async move {
                        SimulatorContext::sleep(extra_delay).await;
                        // A closed channel means the run is shutting down.
                        sender.send(message).await.ok();
                    });
                }
            }
        });
        (buf_sender, receiver)
    }
}
