// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod digest;
mod hash;
mod sign;

use ::digest::Digest;

use crate::{
    authority::Authority,
    block::{Block, BlockReference, RoundNumber, transaction::Transaction},
};

pub use self::digest::{BLOCK_DIGEST_SIZE, BlockDigest};
pub use self::hash::AsBytes;
pub(crate) use self::hash::CryptoHash;
pub use self::sign::{PublicKey, SIGNATURE_SIZE, SignatureBytes, Signer};

use self::digest::{BLOCK_DIGEST_SIZE as DIGEST_SIZE, digest_without_signature};
use self::hash::BlockHasher;

/// Signing-side crypto. Held by Core (not Clone -- owns private key).
pub struct CryptoEngine {
    signer: Signer,
    enabled: bool,
}

/// Verification-side crypto. Clone + Send + Sync. Shared by network
/// tasks.
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
        let digest: [u8; DIGEST_SIZE] = hasher.finalize().into();
        self.signer.sign(&digest)
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
        let mut hasher = BlockHasher::default();
        digest_without_signature(
            &mut hasher,
            block.author(),
            block.round(),
            block.includes(),
            block.transactions(),
            block.creation_time_ns(),
        );
        let digest: [u8; DIGEST_SIZE] = hasher.finalize().into();
        public_key.verify(block.signature(), &digest)
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
