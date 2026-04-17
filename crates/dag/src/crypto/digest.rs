// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt;

use digest::Digest;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

use crate::block::{
    BlockReference, RoundNumber,
    serde::{BytesVisitor, FromBytes},
    transaction::Transaction,
};

use super::hash::{AsBytes, BlockHasher, CryptoHash};
use super::sign::SignatureBytes;
use crate::authority::Authority;

pub const BLOCK_DIGEST_SIZE: usize = 32;

#[derive(Clone, Copy, Eq, Ord, PartialOrd, PartialEq, Default, Hash)]
pub struct BlockDigest([u8; BLOCK_DIGEST_SIZE]);

impl BlockDigest {
    pub(super) fn compute(
        authority: Authority,
        round: RoundNumber,
        includes: &[BlockReference],
        transactions: &[Transaction],
        creation_time: u64,
        signature: &SignatureBytes,
    ) -> Self {
        let mut hasher = BlockHasher::default();
        digest_without_signature(
            &mut hasher,
            authority,
            round,
            includes,
            transactions,
            creation_time,
        );
        hasher.update(signature);
        Self(hasher.finalize().into())
    }
}

/// There is a bit of a complexity around what is considered
/// block digest and what is being signed.
///
/// * Block signature covers all the fields in the block,
///   except for signature and reference.digest
/// * Block digest(e.g. block.reference.digest) covers all the above
///   **and** block signature
///
/// This is not very beautiful, but it allows to optimize block
/// synchronization, by skipping signature verification for all the
/// descendants of the certified block.
pub(super) fn digest_without_signature(
    hasher: &mut BlockHasher,
    authority: Authority,
    round: RoundNumber,
    includes: &[BlockReference],
    transactions: &[Transaction],
    creation_time: u64,
) {
    authority.as_u64().crypto_hash(hasher);
    round.crypto_hash(hasher);
    for include in includes {
        include.crypto_hash(hasher);
    }
    for tx in transactions {
        [0].crypto_hash(hasher);
        tx.crypto_hash(hasher);
    }
    creation_time.crypto_hash(hasher);
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
