// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub use crate::authority::{Authority, AuthoritySet};

pub type RoundNumber = u64;
pub type BlockDigest = super::crypto::BlockDigest;
pub type Stake = u64;
pub type KeyPair = u64;
pub type PublicKey = super::crypto::PublicKey;

// Re-exports so external crates can still use `dag::types::*`.
pub use super::{
    Block, BlockReference, Detailed, TimestampNs,
    transaction::{Transaction, TransactionLocator},
};

#[cfg(test)]
pub use super::test::Dag;
