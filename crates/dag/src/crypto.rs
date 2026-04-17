// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Cryptographic primitives for block signing and verification.
//!
//! The two entry points are [`CryptoEngine`] (held by Core, owns the private key) and
//! [`CryptoVerifier`] (cloneable, shared by network tasks). Both can be disabled at runtime
//! for tests that don't need real cryptography.
//!
//! Submodules:
//! - [`digest`] — block digest computation (Blake2b-256)
//! - [`hash`]   — traits for feeding values into a running hasher
//! - [`sign`]   — Ed25519 key and signature types

mod digest;
mod hash;
mod sign;

use crate::{
    authority::Authority,
    block::{Block, BlockReference, RoundNumber, transaction::Transaction},
};

pub use self::digest::{BLOCK_DIGEST_SIZE, BlockDigest};
pub use self::hash::AsBytes;
pub(crate) use self::hash::CryptoHash;
pub use self::sign::{PublicKey, SIGNATURE_SIZE, SignatureBytes, Signer};

/// Signing-side crypto held by [`Core`](crate::core::Core).
///
/// Not `Clone` — owns the private key. Use [`verifier`](Self::verifier) to obtain a shareable
/// [`CryptoVerifier`] for network tasks.
pub struct CryptoEngine {
    signer: Signer,
    enabled: bool,
}

/// Verification-side crypto shared by network tasks.
///
/// Clone + Send + Sync. Does not hold any private key material.
#[derive(Clone)]
pub struct CryptoVerifier {
    enabled: bool,
}

impl CryptoEngine {
    /// Creates an engine backed by `signer`. When `enabled` is false, all signing and digest
    /// operations return default (zero) values.
    pub fn new(signer: Signer, enabled: bool) -> Self {
        Self { signer, enabled }
    }

    /// Creates an engine with crypto disabled and a dummy key.
    pub fn disabled() -> Self {
        Self {
            signer: Signer::dummy(),
            enabled: false,
        }
    }

    /// Derives a [`CryptoVerifier`] that shares the same enabled state.
    pub fn verifier(&self) -> CryptoVerifier {
        CryptoVerifier {
            enabled: self.enabled,
        }
    }

    /// Signs the block fields and returns the signature and digest. Hashes the fields once to
    /// produce the content hash, signs it, then derives the full digest
    /// `H(content_hash || signature)`.
    pub fn sign(
        &self,
        authority: Authority,
        round: RoundNumber,
        includes: &[BlockReference],
        transactions: &[Transaction],
        timestamp_ns: u64,
    ) -> (SignatureBytes, BlockDigest) {
        if !self.enabled {
            return (SignatureBytes::dummy(), BlockDigest::dummy());
        }
        let content_hash = BlockDigest::new(authority, round, includes, transactions, timestamp_ns);
        let signature = self.signer.sign(content_hash.as_ref());
        let digest = content_hash.with_signature(&signature);
        (signature, digest)
    }

    /// Returns the public key corresponding to the held signer.
    pub fn public_key(&self) -> PublicKey {
        self.signer.public_key()
    }
}

impl CryptoVerifier {
    /// Verifies the signature and computes the digest in one pass. Hashes the fields once to
    /// produce the content hash, verifies the signature against it, then derives the full digest.
    pub fn verify(&self, public_key: &PublicKey, block: &Block) -> eyre::Result<BlockDigest> {
        if !self.enabled {
            return Ok(BlockDigest::dummy());
        }
        let digest = BlockDigest::new(
            block.author(),
            block.round(),
            block.includes(),
            block.transactions(),
            block.timestamp_ns(),
        );
        public_key.verify(block.signature(), digest.as_ref())?;
        Ok(digest.with_signature(block.signature()))
    }
}
