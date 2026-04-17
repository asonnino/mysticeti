// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use digest::Digest;

pub(crate) type BlockHasher = blake2::Blake2b<digest::consts::U32>;

/// Byte-level representation trait used by [`CryptoHash`].
///
/// Separate from `AsRef<[u8]>` because Rust reserves that impl for
/// primitives like `u64`, making a blanket `impl<T: AsRef<[u8]>>
/// CryptoHash for T` impossible alongside explicit primitive impls.
pub trait AsBytes {
    fn as_bytes(&self) -> &[u8];
}

impl<const N: usize> AsBytes for [u8; N] {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

/// Domain-separated hashing into a running [`Digest`].
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
