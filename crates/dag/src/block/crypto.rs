// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt;

use digest::Digest;
use ed25519_consensus::Signature;
use rand::{SeedableRng, rngs::StdRng};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use zeroize::Zeroize;

use super::{
    Block, BlockReference, RoundNumber,
    serde::{BytesVisitor, FromBytes},
    transaction::Transaction,
};
use crate::authority::Authority;

pub const SIGNATURE_SIZE: usize = 64;
pub const BLOCK_DIGEST_SIZE: usize = 32;

#[derive(Clone, Copy, Eq, Ord, PartialOrd, PartialEq, Default, Hash)]
pub struct BlockDigest([u8; BLOCK_DIGEST_SIZE]);

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Debug)]
pub struct PublicKey(ed25519_consensus::VerificationKey);

#[derive(Clone, Copy, Eq, Ord, PartialOrd, PartialEq, Hash)]
pub struct SignatureBytes([u8; SIGNATURE_SIZE]);

// Box ensures value is not copied in memory when Signer itself is moved around for better security
#[derive(Serialize, Deserialize)]
pub struct Signer(Box<ed25519_consensus::SigningKey>);

type BlockHasher = blake2::Blake2b<digest::consts::U32>;

/// Signing-side crypto. Held by Core (not Clone -- owns private key).
pub struct CryptoEngine {
    signer: Signer,
    enabled: bool,
}

/// Verification-side crypto. Clone + Send + Sync. Shared by network tasks.
#[derive(Clone)]
pub struct CryptoVerifier {
    enabled: bool,
}

impl CryptoEngine {
    pub fn new(signer: Signer, enabled: bool) -> Self {
        Self { signer, enabled }
    }

    pub fn disabled() -> Self {
        Self {
            signer: Signer::dummy(),
            enabled: false,
        }
    }

    pub fn verifier(&self) -> CryptoVerifier {
        CryptoVerifier {
            enabled: self.enabled,
        }
    }

    pub fn sign_block(
        &self,
        authority: Authority,
        round: RoundNumber,
        includes: &[BlockReference],
        transactions: &[Transaction],
        creation_time: u64,
    ) -> SignatureBytes {
        if !self.enabled {
            return SignatureBytes::default();
        }
        let mut hasher = BlockHasher::default();
        digest_without_signature(
            &mut hasher,
            authority,
            round,
            includes,
            transactions,
            creation_time,
        );
        let digest: [u8; BLOCK_DIGEST_SIZE] = hasher.finalize().into();
        let signature = self.signer.0.sign(digest.as_ref());
        SignatureBytes(signature.to_bytes())
    }

    pub fn digest(
        &self,
        authority: Authority,
        round: RoundNumber,
        includes: &[BlockReference],
        transactions: &[Transaction],
        creation_time: u64,
        signature: &SignatureBytes,
    ) -> BlockDigest {
        if !self.enabled {
            return BlockDigest::default();
        }
        BlockDigest::compute(
            authority,
            round,
            includes,
            transactions,
            creation_time,
            signature,
        )
    }

    pub fn public_key(&self) -> PublicKey {
        self.signer.public_key()
    }
}

impl CryptoVerifier {
    pub fn verify_signature(&self, public_key: &PublicKey, block: &Block) -> eyre::Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let signature = Signature::from(block.signature().0);
        let mut hasher = BlockHasher::default();
        digest_without_signature(
            &mut hasher,
            block.author(),
            block.round(),
            block.includes(),
            block.transactions(),
            block.creation_time_ns(),
        );
        let digest: [u8; BLOCK_DIGEST_SIZE] = hasher.finalize().into();
        public_key
            .0
            .verify(&signature, digest.as_ref())
            .map_err(|e| eyre::eyre!("Signature verification failed: {e:?}"))
    }

    pub fn digest(
        &self,
        authority: Authority,
        round: RoundNumber,
        includes: &[BlockReference],
        transactions: &[Transaction],
        creation_time: u64,
        signature: &SignatureBytes,
    ) -> BlockDigest {
        if !self.enabled {
            return BlockDigest::default();
        }
        BlockDigest::compute(
            authority,
            round,
            includes,
            transactions,
            creation_time,
            signature,
        )
    }
}

impl BlockDigest {
    fn compute(
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
/// * Block digest(e.g. block.reference.digest) covers all the above **and** block signature
///
/// This is not very beautiful, but it allows to optimize block synchronization,
/// by skipping signature verification for all the descendants of the certified block.
fn digest_without_signature(
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

pub trait AsBytes {
    // This is pretty much same as AsRef<[u8]>
    //
    // We need this separate trait because we want to impl CryptoHash
    // for primitive types(u64, etc) and types like XxxDigest that implement AsRef<[u8]>.
    //
    // Rust unfortunately does not allow to impl trait for AsRef<[u8]> and primitive types like u64.
    //
    // While AsRef<[u8]> is not implemented for u64, it seem to be reserved in compiler,
    // so `impl CryptoHash for u64` and `impl<T: AsRef<[u8]>> CryptoHash for T` collide.
    fn as_bytes(&self) -> &[u8];
}

impl<const N: usize> AsBytes for [u8; N] {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

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

impl Signer {
    pub fn new_for_test(n: usize) -> Vec<Self> {
        let mut rng = StdRng::seed_from_u64(0);
        (0..n)
            .map(|_| Self(Box::new(ed25519_consensus::SigningKey::new(&mut rng))))
            .collect()
    }

    pub fn public_key(&self) -> PublicKey {
        PublicKey(self.0.verification_key())
    }
}

impl AsRef<[u8]> for BlockDigest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8]> for SignatureBytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsBytes for BlockDigest {
    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl AsBytes for SignatureBytes {
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

impl Default for SignatureBytes {
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

impl Drop for Signer {
    fn drop(&mut self) {
        self.0.zeroize()
    }
}

impl Signer {
    pub(crate) fn dummy() -> Self {
        Self(Box::new(ed25519_consensus::SigningKey::from([0u8; 32])))
    }
}

impl PublicKey {
    pub(crate) fn dummy() -> Self {
        Signer::dummy().public_key()
    }
}
