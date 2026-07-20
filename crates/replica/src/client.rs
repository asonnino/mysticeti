// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use dag::block::transaction::Transaction;
use eyre::{Result, eyre};
use tokio::sync::mpsc;

/// Cloneable handle for submitting transactions to a running replica, independent of the
/// [`ReplicaHandle`]'s lifetime.
#[derive(Clone)]
pub struct TransactionClient {
    sender: mpsc::Sender<Vec<Transaction>>,
}

impl TransactionClient {
    pub(crate) fn new(sender: mpsc::Sender<Vec<Transaction>>) -> Self {
        Self { sender }
    }

    /// Submit a batch of transactions. Resolves once the batch is queued for inclusion in a
    /// future block (not once committed). Errors iff the replica has shut down.
    ///
    /// Under `SimulatorContext`, this future must be awaited from a task running inside the
    /// simulated world (spawned via `Ctx::spawn`).
    pub async fn submit(&self, transactions: Vec<Transaction>) -> Result<()> {
        self.sender
            .send(transactions)
            .await
            .map_err(|_| eyre!("Transaction channel closed"))
    }
}
