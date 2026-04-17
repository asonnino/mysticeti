// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt;

use rand::{SeedableRng, rngs::StdRng};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use zeroize::Zeroize;

use crate::block::serde::{BytesVisitor, FromBytes};

use super::hash::AsBytes;

pub const SIGNATURE_SIZE: usize = 64;

#[derive(Clone, Copy, Eq, Ord, PartialOrd, PartialEq, Hash)]
pub struct SignatureBytes([u8; SIGNATURE_SIZE]);

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub struct PublicKey(pub(super) ed25519_consensus::VerificationKey);

/// Private signing key.
///
/// Box ensures the key is not copied in memory when [`Signer`] is
/// moved, for better security.
#[derive(Serialize, Deserialize)]
pub struct Signer(Box<ed25519_consensus::SigningKey>);

impl Signer {
    pub fn new_for_test(n: usize) -> Vec<Self> {
        let mut rng = StdRng::seed_from_u64(0);
        (0..n)
            .map(|_| Self(Box::new(ed25519_consensus::SigningKey::new(&mut rng))))
            .collect()
    }

    pub(crate) fn dummy() -> Self {
        Self(Box::new(ed25519_consensus::SigningKey::from([0u8; 32])))
    }

    pub fn public_key(&self) -> PublicKey {
        PublicKey(self.0.verification_key())
    }

    pub(super) fn sign(&self, digest: &[u8]) -> SignatureBytes {
        let signature = self.0.sign(digest);
        SignatureBytes(signature.to_bytes())
    }
}

impl PublicKey {
    pub(crate) fn dummy() -> Self {
        Signer::dummy().public_key()
    }

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
