// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs::{File, OpenOptions},
    io,
    io::Write,
    path::Path,
};

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::{committee::ProcessedTransactionHandler, runtime, types::TransactionLocator};

pub struct TransactionLog {
    ch: UnboundedSender<Vec<TransactionLocator>>,
}

impl TransactionLog {
    pub fn start(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new().append(true).create(true).open(path)?;
        let (sender, receiver) = unbounded_channel();
        runtime::Handle::current().spawn(Self::run(file, receiver));
        Ok(Self { ch: sender })
    }

    async fn run(mut file: File, mut receiver: UnboundedReceiver<Vec<TransactionLocator>>) {
        while let Some(id) = receiver.recv().await {
            writeln!(file, "{:?}", id).expect("Failed to write to transaction log");
        }
    }
}

impl ProcessedTransactionHandler<TransactionLocator> for TransactionLog {
    fn transaction_processed(&mut self, k: TransactionLocator) {
        self.ch.send(vec![k]).ok();
    }
}
