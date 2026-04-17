// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Transactions carried inside blocks and the locators used to address them.

use std::{fmt, time::Duration};

use digest::Digest;
use minibytes::Bytes;
use serde::{Deserialize, Serialize};

use super::{
    BlockReference,
    crypto::{AsBytes, CryptoHash},
};

/// An opaque payload submitted by a client and included in a [`Block`](super::Block).
///
/// The consensus layer treats the bytes as a black box; only the benchmark
/// tooling imposes structure on them (see [`Transaction::extract_timestamp`]).
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Default)]
pub struct Transaction {
    data: Bytes,
}

impl Transaction {
    /// Wraps `data` as a transaction without inspecting its contents.
    pub fn new(data: Bytes) -> Self {
        Self { data }
    }

    /// Decodes the submission timestamp that the benchmark generator writes
    /// into the first eight bytes of each transaction as a little-endian `u64`
    /// of milliseconds since the Unix epoch.
    ///
    /// Returns `None` for payloads shorter than eight bytes. This method is
    /// only meaningful for transactions produced by the benchmark generator;
    /// arbitrary payloads will decode to a nonsensical timestamp.
    pub fn extract_timestamp(&self) -> Option<Duration> {
        let bytes = self.data.first_chunk::<8>()?;
        Some(Duration::from_millis(u64::from_le_bytes(*bytes)))
    }
}

impl AsBytes for Transaction {
    fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

impl fmt::Display for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tx")
    }
}

impl fmt::Debug for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tx({}B)", self.data.len())
    }
}

/// Stable address of a transaction within the DAG.
///
/// A locator pairs the [`BlockReference`] containing the transaction with its
/// position in that block's transaction list, uniquely identifying the
/// transaction across the system.
#[derive(Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Default)]
pub struct TransactionLocator {
    block: BlockReference,
    offset: u64,
}

impl TransactionLocator {
    /// Builds a locator pointing at `offset` within `block`.
    pub(crate) fn new(block: BlockReference, offset: u64) -> Self {
        Self { block, offset }
    }

    /// Returns the block that contains the transaction.
    pub fn block(&self) -> &BlockReference {
        &self.block
    }

    /// Returns the transaction's zero-based index within its block.
    pub fn offset(&self) -> u64 {
        self.offset
    }
}

impl fmt::Debug for TransactionLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl fmt::Display for TransactionLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.block, self.offset)
    }
}

impl CryptoHash for TransactionLocator {
    fn crypto_hash(&self, state: &mut impl Digest) {
        self.block.crypto_hash(state);
        self.offset.crypto_hash(state);
    }
}
