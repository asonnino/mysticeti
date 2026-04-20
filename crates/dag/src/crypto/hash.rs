// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Traits and type aliases for cryptographic hashing.
//!
//! [`CryptoHash`] is the internal trait that block fields implement to feed themselves into a
//! running [`Digest`]. [`AsBytes`] is its companion for types that can be represented as a byte
//! slice, getting a blanket [`CryptoHash`] impl for free.

use digest::Digest;

/// The concrete hasher used for block digests and signatures (Blake2b truncated to 256 bits).
pub(crate) type BlockHasher = blake2::Blake2b<digest::consts::U32>;

/// Byte-level representation used by the blanket [`CryptoHash`] impl.
///
/// This exists as a separate trait rather than reusing `AsRef<[u8]>` because Rust's coherence
/// rules prevent a blanket `impl<T: AsRef<[u8]>> CryptoHash for T` from coexisting with explicit
/// impls for primitives like `u64`.
pub trait AsBytes {
    fn as_bytes(&self) -> &[u8];
}

impl<const N: usize> AsBytes for [u8; N] {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

/// Feeds a value into a running [`Digest`] for cryptographic hashing. Primitive integers use
/// big-endian encoding; [`AsBytes`] types feed their raw bytes directly.
pub(crate) trait CryptoHash {
    fn crypto_hash(&self, state: &mut impl Digest);
}

impl CryptoHash for u64 {
    fn crypto_hash(&self, state: &mut impl Digest) {
        state.update(self.to_be_bytes());
    }
}

impl CryptoHash for u128 {
    fn crypto_hash(&self, state: &mut impl Digest) {
        state.update(self.to_be_bytes());
    }
}

impl<T: AsBytes> CryptoHash for T {
    fn crypto_hash(&self, state: &mut impl Digest) {
        state.update(self.as_bytes());
    }
}
