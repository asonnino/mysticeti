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
pub use self::hash::CryptoHash;
pub use self::sign::{PublicKey, SIGNATURE_SIZE, SignatureBytes, Signer};

/// Signing-side crypto held by [`Core`](crate::core::Core).
///
/// Not `Clone` — owns the private key. Use [`verifier`](Self::verifier) to obtain a shareable
/// [`CryptoVerifier`] for network tasks.
pub struct CryptoEngine {
    signer: Signer,
    mode: Mode,
}

/// Verification-side crypto shared by network tasks.
///
/// Clone + Send + Sync. Does not hold any private key material.
#[derive(Clone)]
pub struct CryptoVerifier {
    mode: Mode,
}

/// How an engine treats signatures and digests.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Real signatures; the digest is recomputed and checked.
    Enabled,
    /// No signatures; every block must carry the synthetic `(round, authority)` digest, so a
    /// slot holds at most one representable block.
    Disabled,
    /// No signatures and the claimed digest is trusted, so a slot may hold several
    /// distinguishable blocks. Simulator-only: what modelling an equivocating leader needs.
    Simulated,
}

impl CryptoEngine {
    /// Creates an engine that signs outgoing blocks with the given key and verifies incoming
    /// signatures.
    pub fn enabled(signer: Signer) -> Self {
        Self {
            signer,
            mode: Mode::Enabled,
        }
    }

    /// Creates an engine that skips signing and verification. Signing returns default-valued
    /// signatures and synthetic digests; verification accepts exactly those digests.
    pub fn disabled() -> Self {
        Self {
            signer: Signer::dummy(),
            mode: Mode::Disabled,
        }
    }

    /// Like [`disabled`](Self::disabled), but verification trusts whatever digest a block
    /// claims. For the simulator only, which forges same-slot twins to model equivocation.
    pub fn simulated() -> Self {
        Self {
            signer: Signer::dummy(),
            mode: Mode::Simulated,
        }
    }

    /// Derives a [`CryptoVerifier`] that shares the same mode.
    pub fn verifier(&self) -> CryptoVerifier {
        CryptoVerifier { mode: self.mode }
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
        if self.mode != Mode::Enabled {
            return (
                SignatureBytes::dummy(),
                BlockDigest::synthetic(round, authority),
            );
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
        match self.mode {
            Mode::Disabled => return Ok(BlockDigest::synthetic(block.round(), block.author())),
            Mode::Simulated => return Ok(block.digest()),
            Mode::Enabled => {}
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
