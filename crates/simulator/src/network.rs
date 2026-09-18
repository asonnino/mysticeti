// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    sync::Arc,
    time::Duration,
};

use tokio::{select, sync::mpsc};

use super::context::SimulatorContext;
use super::equivocator::Equivocator;
use super::executor::Sleep;
use super::latency::{LatencyModel, LinkLatency};
use dag::committee::Committee;
use dag::context::Ctx;
use dag::sync::network::{Connection, Network, NetworkMessage};

/// Most messages a link holds in flight before it stops taking new ones.
const MAX_IN_FLIGHT: usize = 1024;

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

    /// A FIFO link that delays every message by its own latency, independently of those in
    /// flight.
    fn latency_channel<T: Send + 'static + Debug>(
        link: LinkLatency,
    ) -> (mpsc::Sender<T>, mpsc::Receiver<T>) {
        let (buf_sender, mut buf_receiver) = mpsc::channel(16);
        let (sender, receiver) = mpsc::channel(16);
        SimulatorContext::spawn(async move {
            // Messages in flight with their delivery time, and the timer of the front one.
            let mut in_flight: VecDeque<(Duration, T)> = VecDeque::new();
            let mut head_timer: Option<Sleep> = None;
            let mut open = true;
            loop {
                if head_timer.is_none()
                    && let Some((deliver_at, _)) = in_flight.front()
                {
                    let delay = deliver_at.saturating_sub(SimulatorContext::time());
                    head_timer = Some(Sleep::new(delay));
                }
                // Biased: an unbiased `select!` picks its branch from a process-seeded RNG.
                select! {
                    biased;
                    _ = async { head_timer.as_mut().expect("guarded").await },
                        if head_timer.is_some() =>
                    {
                        head_timer = None;
                        let (_, message) = in_flight.pop_front().expect("timer implies a head");
                        // A full inbox blocks the loop, so a stuck receiver stops the intake.
                        if sender.send(message).await.is_err() {
                            return;
                        }
                    }
                    message = buf_receiver.recv(),
                        if open && in_flight.len() < MAX_IN_FLIGHT =>
                    {
                        let Some(message) = message else {
                            open = false;
                            continue;
                        };
                        let latency = SimulatorContext::with_rng(|rng| link.sample(rng));
                        // A message due before its predecessor still waits behind it.
                        in_flight.push_back((SimulatorContext::time() + latency, message));
                    }
                    else => return,
                }
            }
        });
        (buf_sender, receiver)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use dag::context::Ctx;
    use rand::{SeedableRng, rngs::StdRng};

    use super::{MAX_IN_FLIGHT, SimulatedNetwork};
    use crate::{context::SimulatorContext, executor::SimulatorExecutor, latency::LinkLatency};

    fn run(test: impl Future<Output = ()> + Send + 'static) {
        SimulatorExecutor::run(StdRng::seed_from_u64(0), test);
    }

    fn constant(latency: Duration) -> LinkLatency {
        LinkLatency::new(latency, Duration::ZERO..Duration::ZERO)
    }

    #[test]
    fn messages_are_delayed_independently() {
        run(async {
            let latency = Duration::from_millis(100);
            let (sender, mut receiver) = SimulatedNetwork::latency_channel(constant(latency));
            // Ten messages, 10 ms apart: far faster than one per latency. The sender is
            // dropped with messages still in flight, which must be delivered all the same.
            SimulatorContext::spawn(async move {
                for _ in 0..10 {
                    sender.send(SimulatorContext::time()).await.unwrap();
                    SimulatorContext::sleep(Duration::from_millis(10)).await;
                }
            });
            let mut delivered = 0;
            while let Some(sent_at) = receiver.recv().await {
                let elapsed = SimulatorContext::time() - sent_at;
                assert!(latency <= elapsed && elapsed < latency + Duration::from_millis(1));
                delivered += 1;
            }
            assert_eq!(delivered, 10);
        });
    }

    #[test]
    fn jitter_preserves_the_order() {
        run(async {
            let jitter = Duration::ZERO..Duration::from_millis(100);
            let link = LinkLatency::new(Duration::ZERO, jitter);
            let (sender, mut receiver) = SimulatedNetwork::latency_channel(link);
            SimulatorContext::spawn(async move {
                for message in 0..100 {
                    sender.send(message).await.unwrap();
                }
            });
            for expected in 0..100 {
                assert_eq!(receiver.recv().await, Some(expected));
            }
            assert_eq!(receiver.recv().await, None);
        });
    }

    #[test]
    fn a_stuck_receiver_pushes_back() {
        run(async {
            let (sender, mut receiver) =
                SimulatedNetwork::latency_channel(constant(Duration::ZERO));
            let mut accepted = 0;
            while sender.try_send(accepted).is_ok() {
                accepted += 1;
                // Two 16-slot buffers and the message in hand: far below the in-flight window.
                assert!(accepted < 64, "the link never pushed back");
                SimulatorContext::sleep(Duration::from_millis(1)).await;
            }
            for expected in 0..accepted {
                assert_eq!(receiver.recv().await, Some(expected));
            }
            SimulatorContext::sleep(Duration::from_millis(1)).await;
            assert!(sender.try_send(accepted).is_ok());
        });
    }

    #[test]
    fn a_burst_is_bounded() {
        run(async {
            let latency = Duration::from_secs(1);
            let (sender, mut receiver) = SimulatedNetwork::latency_channel(constant(latency));
            let sent = Arc::new(AtomicUsize::new(0));
            let counter = sent.clone();
            SimulatorContext::spawn(async move {
                for message in 0..2 * MAX_IN_FLIGHT {
                    sender.send(message).await.unwrap();
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            });
            // Nothing is due yet: the link holds a full window and the sender is blocked.
            SimulatorContext::sleep(latency / 2).await;
            let in_flight = sent.load(Ordering::Relaxed);
            assert!((MAX_IN_FLIGHT..2 * MAX_IN_FLIGHT).contains(&in_flight));
            for expected in 0..2 * MAX_IN_FLIGHT {
                assert_eq!(receiver.recv().await, Some(expected));
            }
        });
    }
}
