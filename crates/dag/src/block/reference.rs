// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt,
    hash::{Hash, Hasher},
};

use digest::Digest;
use serde::{Deserialize, Serialize};

use super::{
    RoundNumber,
    crypto::{BlockDigest, CryptoHash},
};
use crate::authority::Authority;

#[derive(Clone, Copy, Eq, PartialEq, Serialize, Deserialize, Default)]
pub struct BlockReference {
    pub authority: Authority,
    pub round: RoundNumber,
    pub digest: BlockDigest,
}

impl Hash for BlockReference {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(&self.digest.as_ref()[..8]);
    }
}

impl PartialOrd for BlockReference {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BlockReference {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.round, self.authority, self.digest).cmp(&(other.round, other.authority, self.digest))
    }
}

impl BlockReference {
    #[cfg(any(test, feature = "test-utils"))]
    pub fn new_test(authority: u64, round: RoundNumber) -> Self {
        let authority = Authority::new(authority);
        if round == 0 {
            super::Block::new_genesis(authority).reference
        } else {
            Self {
                authority,
                round,
                digest: Default::default(),
            }
        }
    }

    pub fn round(&self) -> RoundNumber {
        self.round
    }

    pub fn author_round(&self) -> (Authority, RoundNumber) {
        (self.authority, self.round)
    }

    pub fn author_digest(&self) -> (Authority, BlockDigest) {
        (self.authority, self.digest)
    }
}

impl fmt::Debug for BlockReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl fmt::Display for BlockReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.authority.with_round(self.round))
    }
}

impl CryptoHash for BlockReference {
    fn crypto_hash(&self, state: &mut impl Digest) {
        self.authority.as_u64().crypto_hash(state);
        self.round.crypto_hash(state);
        self.digest.crypto_hash(state);
    }
}
