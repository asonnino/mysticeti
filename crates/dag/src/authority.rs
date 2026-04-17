// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::block::RoundNumber;

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

#[derive(Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Default)]
pub struct AuthoritySet(u128); // todo - support more then 128 authorities

impl AuthoritySet {
    #[inline]
    pub fn insert(&mut self, v: Authority) -> bool {
        let bit = 1u128 << v.as_u64();
        if self.0 & bit == bit {
            return false;
        }
        self.0 |= bit;
        true
    }

    pub fn present(&self) -> impl Iterator<Item = Authority> + '_ {
        (0..128u64)
            .filter(|bit| (self.0 & 1 << bit) != 0)
            .map(Authority::new)
    }

    #[inline]
    pub fn clear(&mut self) {
        self.0 = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authority_set_test() {
        let mut a = AuthoritySet::default();
        assert!(a.insert(Authority::new(0)));
        assert!(!a.insert(Authority::new(0)));
        assert!(a.insert(Authority::new(1)));
        assert!(a.insert(Authority::new(2)));
        assert!(!a.insert(Authority::new(1)));
        assert!(a.insert(Authority::new(127)));
        assert!(!a.insert(Authority::new(127)));
        assert!(a.insert(Authority::new(3)));
        assert!(!a.insert(Authority::new(3)));
        assert!(!a.insert(Authority::new(2)));
    }

    #[test]
    fn authority_present_test() {
        let mut a = AuthoritySet::default();
        let present: Vec<Authority> = vec![1, 2, 3, 4, 5, 64, 127]
            .into_iter()
            .map(Authority::new)
            .collect();
        for x in &present {
            a.insert(*x);
        }
        assert_eq!(present, a.present().collect::<Vec<_>>());
    }
}
