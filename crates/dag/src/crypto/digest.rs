// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Block digest computation.
//!
//! A [`BlockDigest`] is a 32-byte Blake2b hash that uniquely identifies a block in the DAG.
//! The digest is computed in two phases: [`BlockDigest::new`] hashes the block fields into a
//! content hash, then [`BlockDigest::with_signature`] combines it with the signature into the
//! full digest `H(content_hash || signature)`. A valid digest transitively certifies the block's
//! content, letting peers skip signature verification for ancestors of already-certified blocks.

use std::fmt;

use digest::Digest;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::{
    authority::Authority,
    block::{
        BlockReference, RoundNumber,
        serde::{BytesVisitor, FromBytes},
        transaction::Transaction,
    },
};

use super::hash::{AsBytes, BlockHasher, CryptoHash};
use super::sign::SignatureBytes;

/// Length of a block digest in bytes (Blake2b-256).
pub const BLOCK_DIGEST_SIZE: usize = 32;

/// 64-bit golden-ratio constant (`floor(2^64 / phi)`, odd). Used as a multiplier in
/// [`BlockDigest::synthetic`] to diffuse `round` across all 8 bytes before XOR-ing
/// in `authority`, so both dimensions contribute entropy to every byte of the bucket
/// prefix consumed by `BlockReference::Hash`.
const GOLDEN_RATIO_MIXER: u64 = 0x9E37_79B9_7F4A_7C15;

/// A 32-byte Blake2b-256 hash that uniquely identifies a block.
#[derive(Clone, Copy, Eq, Ord, PartialOrd, PartialEq, Hash)]
pub struct BlockDigest([u8; BLOCK_DIGEST_SIZE]);

impl BlockDigest {
    /// Hashes all the provided fields.
    pub(super) fn new(
        authority: Authority,
        round: RoundNumber,
        includes: &[BlockReference],
        transactions: &[Transaction],
        timestamp_ns: u64,
    ) -> Self {
        let mut hasher = BlockHasher::default();
        authority.crypto_hash(&mut hasher);
        round.crypto_hash(&mut hasher);
        for include in includes {
            include.crypto_hash(&mut hasher);
        }
        for tx in transactions {
            // Cast to u64 so the length prefix is platform-independent.
            (tx.as_bytes().len() as u64).crypto_hash(&mut hasher);
            tx.crypto_hash(&mut hasher);
        }
        timestamp_ns.crypto_hash(&mut hasher);
        Self(hasher.finalize().into())
    }

    /// Extends the content hash with the signature: `H(self || signature)`.
    pub(super) fn with_signature(self, signature: &SignatureBytes) -> Self {
        let mut hasher = BlockHasher::default();
        hasher.update(self.0);
        hasher.update(signature);
        Self(hasher.finalize().into())
    }

    /// A digest that encodes `(round, authority)` in its first 8 bytes, with the remaining
    /// 24 bytes zeroed. Used by [`Block::genesis`], the [`CryptoEngine::disabled`] paths,
    /// and the test helpers — anywhere a real Blake2b digest is not produced. Keeps the
    /// `Hash` impl on [`BlockReference`] (which keys off the first 8 bytes) well-distributed.
    /// Two blocks at the same `(round, authority)` still collide, which matches their
    /// indistinguishability under disabled crypto.
    ///
    /// [`Block::genesis`]: crate::block::Block::genesis
    /// [`CryptoEngine::disabled`]: super::CryptoEngine::disabled
    pub(crate) fn synthetic(round: RoundNumber, authority: Authority) -> Self {
        let mut bytes = [0u8; BLOCK_DIGEST_SIZE];
        let mixed = round.wrapping_mul(GOLDEN_RATIO_MIXER) ^ authority.as_u64();
        bytes[..8].copy_from_slice(&mixed.to_be_bytes());
        Self(bytes)
    }
}

/// Test-only escape hatch to spell a digest directly.
#[cfg(any(test, feature = "test-utils"))]
impl From<[u8; BLOCK_DIGEST_SIZE]> for BlockDigest {
    fn from(bytes: [u8; BLOCK_DIGEST_SIZE]) -> Self {
        Self(bytes)
    }
}

impl AsRef<[u8]> for BlockDigest {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsBytes for BlockDigest {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for BlockDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "@{}", hex::encode(self.0))
    }
}

impl fmt::Display for BlockDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "@{}", hex::encode(&self.0[..4]))
    }
}

impl FromBytes for BlockDigest {
    fn try_copy_from_slice<E: de::Error>(v: &[u8]) -> Result<Self, E> {
        if v.len() != BLOCK_DIGEST_SIZE {
            return Err(E::custom(format!(
                "Invalid block digest length: {}",
                v.len()
            )));
        }
        let mut inner = [0u8; BLOCK_DIGEST_SIZE];
        inner.copy_from_slice(v);
        Ok(Self(inner))
    }
}

impl Serialize for BlockDigest {
    #[inline]
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for BlockDigest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_bytes(BytesVisitor::new())
    }
}
