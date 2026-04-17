// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A [`BlockReference`] uniquely identifies a block in the DAG via the
//! triple `(authority, round, digest)`.
//!
//! # Hashing
//!
//! The [`std::hash::Hash`] implementation uses only the first 8 bytes of the
//! digest for performance. This is **not** cryptographically secure — it is
//! used solely for internal indexing in `HashMap`/`HashSet`. Collisions are
//! handled by the standard library's equality fallback (`PartialEq`), so
//! correctness is not affected. For cryptographic hashing, see [`CryptoHash`].

use std::{
    fmt,
    hash::{Hash, Hasher},
};

use digest::Digest;
use serde::{Deserialize, Serialize};

use super::RoundNumber;
use crate::authority::Authority;
use crate::crypto::{BlockDigest, CryptoHash};

/// A unique identifier for a block in the DAG.
#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Default)]
pub struct BlockReference {
    pub authority: Authority,
    pub round: RoundNumber,
    pub digest: BlockDigest,
}

impl BlockReference {
    /// Create a test reference. For round 0, computes a real genesis digest;
    /// for other rounds, uses a default (zero) digest.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn new_test(authority: u64, round: RoundNumber) -> Self {
        let authority = Authority::new(authority);
        if round == 0 {
            super::Block::genesis(authority).reference
        } else {
            Self {
                authority,
                round,
                digest: Default::default(),
            }
        }
    }

    pub fn round(&self) -> RoundNumber {
        self.round
    }

    /// Return `(authority, round)` for pattern matching and comparison.
    /// For display, use `authority.with_round(round)` instead.
    pub fn author_round(&self) -> (Authority, RoundNumber) {
        (self.authority, self.round)
    }

    pub fn author_digest(&self) -> (Authority, BlockDigest) {
        (self.authority, self.digest)
    }
}

impl fmt::Debug for BlockReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl fmt::Display for BlockReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.authority.with_round(self.round))
    }
}

/// Cryptographic hash for block digest computation.
impl CryptoHash for BlockReference {
    fn crypto_hash(&self, state: &mut impl Digest) {
        self.authority.as_u64().crypto_hash(state);
        self.round.crypto_hash(state);
        self.digest.crypto_hash(state);
    }
}

/// Uses only the first 8 bytes of the digest for fast bucket lookup.
/// See module docs for details on collision safety.
impl Hash for BlockReference {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(&self.digest.as_ref()[..8]);
    }
}

impl PartialOrd for BlockReference {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BlockReference {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.round, self.authority, self.digest).cmp(&(other.round, other.authority, other.digest))
    }
}
