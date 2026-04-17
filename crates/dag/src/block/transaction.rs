// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt, time::Duration};

use digest::Digest;
use minibytes::Bytes;
use serde::{Deserialize, Serialize};

use super::{
    BlockReference,
    crypto::{AsBytes, CryptoHash},
};

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Default)]
pub struct Transaction {
    data: Bytes,
}

impl Transaction {
    pub fn new(data: Bytes) -> Self {
        Self { data }
    }

    #[allow(dead_code)]
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    #[allow(dead_code)]
    pub fn into_data(self) -> Bytes {
        self.data
    }

    pub fn extract_timestamp(&self) -> Duration {
        let bytes = self.data[0..8]
            .try_into()
            .expect("Transaction should be at least 8 bytes");
        Duration::from_millis(u64::from_le_bytes(bytes))
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

#[derive(Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Default)]
pub struct TransactionLocator {
    block: BlockReference,
    offset: u64,
}

impl TransactionLocator {
    pub(crate) fn new(block: BlockReference, offset: u64) -> Self {
        Self { block, offset }
    }

    pub fn block(&self) -> &BlockReference {
        &self.block
    }

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
