// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::types::RoundNumber;

/// Identifies an authority (participant) in the
/// consensus committee. Wraps a zero-based index.
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Default)]
pub struct Authority(u64);

impl Authority {
    pub fn new(index: u64) -> Self {
        Self(index)
    }

    /// Return the index as `usize` for vec/slice indexing.
    #[inline]
    pub fn index(self) -> usize {
        self.0 as usize
    }

    /// Return the raw `u64` value.
    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// Pair with a round number for display (e.g. "A3").
    pub fn with_round(self, round: RoundNumber) -> AuthorityRound {
        AuthorityRound(self, round)
    }
}

impl From<u64> for Authority {
    fn from(index: u64) -> Self {
        Self(index)
    }
}

impl From<usize> for Authority {
    fn from(index: usize) -> Self {
        Self(index as u64)
    }
}

impl fmt::Display for Authority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let i = self.0;
        if i < 26 {
            write!(f, "{}", (b'A' + i as u8) as char)
        } else {
            write!(
                f,
                "{}{}",
                (b'A' + (i / 26 - 1) as u8) as char,
                (b'A' + (i % 26) as u8) as char,
            )
        }
    }
}

impl fmt::Debug for Authority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

/// Zero-allocation display for authority + round
/// (e.g. "A3", "B1").
pub struct AuthorityRound(Authority, RoundNumber);

impl fmt::Display for AuthorityRound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.0, self.1)
    }
}
