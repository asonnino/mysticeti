// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Ed25519 signing primitives.
//!
//! This module provides the low-level key and signature types used by the higher-level
//! [`CryptoEngine`](super::CryptoEngine) and [`CryptoVerifier`](super::CryptoVerifier).
//! Application code should rarely need to call [`Signer::sign`] or [`PublicKey::verify`] directly.

use std::fmt;

use rand::{CryptoRng, Rng, SeedableRng, rngs::StdRng};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use zeroize::Zeroize;

use crate::block::serde::{BytesVisitor, FromBytes};

use super::hash::AsBytes;

/// Length of an Ed25519 signature in bytes.
pub const SIGNATURE_SIZE: usize = 64;

/// A fixed-size Ed25519 signature stored inline.
///
/// This is a value type (Copy + Ord + Hash) suitable for use as a map key or inside serialized
/// blocks. The default value is all zeros, used when crypto is disabled.
#[derive(Clone, Copy, Eq, Ord, PartialOrd, PartialEq, Hash)]
pub struct SignatureBytes([u8; SIGNATURE_SIZE]);

/// An Ed25519 verification (public) key.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub struct PublicKey(pub(super) ed25519_consensus::VerificationKey);

/// An Ed25519 signing (private) key.
///
/// The inner key is boxed so that it stays at a stable heap address and is not inadvertently
/// copied when the [`Signer`] is moved. The key material is zeroized on drop.
#[derive(Serialize, Deserialize)]
pub struct Signer(Box<ed25519_consensus::SigningKey>);

impl Signer {
    /// Generates a new random signing key from the given CSPRNG.
    pub fn new<R: Rng + CryptoRng>(rng: &mut R) -> Self {
        Self(Box::new(ed25519_consensus::SigningKey::new(rng)))
    }

    /// Returns a deterministic key for tests where crypto is disabled and only the type is needed.
    pub(crate) fn dummy() -> Self {
        Self::new(&mut StdRng::seed_from_u64(0))
    }

    /// Derives the corresponding public key.
    pub fn public_key(&self) -> PublicKey {
        PublicKey(self.0.verification_key())
    }

    /// Signs `digest` and returns the raw signature bytes.
    pub(super) fn sign(&self, digest: &[u8]) -> SignatureBytes {
        let signature = self.0.sign(digest);
        SignatureBytes(signature.to_bytes())
    }
}

impl PublicKey {
    /// Returns a deterministic public key matching [`Signer::dummy`].
    pub(crate) fn dummy() -> Self {
        Signer::dummy().public_key()
    }

    /// Verifies `signature` over `digest`, returning an error on mismatch.
    pub(super) fn verify(&self, signature: &SignatureBytes, digest: &[u8]) -> eyre::Result<()> {
        let sig = ed25519_consensus::Signature::from(signature.0);
        self.0
            .verify(&sig, digest)
            .map_err(|e| eyre::eyre!("Signature verification failed: {e:?}"))
    }
}

impl AsRef<[u8]> for SignatureBytes {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsBytes for SignatureBytes {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Default for SignatureBytes {
    #[inline]
    fn default() -> Self {
        Self([0u8; 64])
    }
}

impl FromBytes for SignatureBytes {
    fn try_copy_from_slice<E: de::Error>(v: &[u8]) -> Result<Self, E> {
        if v.len() != SIGNATURE_SIZE {
            return Err(E::custom(format!("Invalid signature length: {}", v.len())));
        }
        let mut inner = [0u8; SIGNATURE_SIZE];
        inner.copy_from_slice(v);
        Ok(Self(inner))
    }
}

impl Serialize for SignatureBytes {
    #[inline]
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for SignatureBytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_bytes(BytesVisitor::new())
    }
}

impl Drop for Signer {
    fn drop(&mut self) {
        self.0.zeroize()
    }
}

impl fmt::Debug for Signer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Signer(public_key={:?})", self.public_key())
    }
}

impl fmt::Display for Signer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Signer(public_key={:?})", self.public_key())
    }
}
