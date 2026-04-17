// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! DAG block types and verification logic.
//!
//! A [`Block`] is the fundamental unit of the DAG. Each block is proposed by
//! an authority, references blocks from the previous round (via [`BlockReference`]),
//! and carries a batch of [`Transaction`]s. Blocks are signed and their integrity
//! is verified on receipt via [`Block::verify`].
//!
//! # Submodules
//!
//! - [`crypto`] — block digests, signatures, and hash computation.
//! - [`data`] — [`Data<T>`](data::Data), a reference-counted wrapper caching
//!   serialized bytes.
//! - [`reference`] — [`BlockReference`], the `(authority, round, digest)` triple
//!   that uniquely identifies a block.
//! - [`transaction`] — [`Transaction`] and [`TransactionLocator`].

pub mod data;
pub mod reference;
pub(crate) mod serde;
pub mod transaction;

pub use reference::BlockReference;

use std::{
    fmt,
    hash::{Hash, Hasher},
    time::Duration,
};

use ::serde::{Deserialize, Serialize};
use eyre::{bail, ensure};

use self::{
    data::Data,
    transaction::{Transaction, TransactionLocator},
};
use crate::crypto::{BlockDigest, CryptoEngine, CryptoVerifier, SignatureBytes};
use crate::{
    authority::Authority,
    committee::{Committee, Stake},
    core::threshold_clock::threshold_clock_valid_non_genesis,
};

/// Round number within the DAG (0 = genesis).
pub type RoundNumber = u64;

const GENESIS_ROUND: RoundNumber = 0;

/// A block in the DAG. Contains references to prior-round blocks, a batch of
/// transactions, and a signature from the proposing authority.
///
/// **Invariant:** adding or removing fields requires updating
/// [`CryptoEngine::digest`] and [`Block::verify`].
#[derive(Clone, Serialize, Deserialize)]
pub struct Block {
    reference: BlockReference,

    /// References to blocks from earlier rounds. Order matters: when two
    /// blocks from the same `(authority, round)` are included, the first
    /// reference is the one this block votes for.
    includes: Vec<BlockReference>,

    /// Transactions batched into this block.
    transactions: Vec<Transaction>,

    /// Creation time (nanoseconds since epoch) as reported by the proposer.
    /// Informational only — not enforced by the protocol.
    creation_time: u64,

    /// Signature over the block content by the proposing authority.
    signature: SignatureBytes,
}

impl Block {
    /// Create the genesis block for an authority (round 0, no includes,
    /// no transactions, default digest and signature).
    pub fn genesis(authority: Authority) -> Data<Self> {
        Data::new(Self {
            reference: BlockReference {
                authority,
                round: GENESIS_ROUND,
                digest: BlockDigest::default(),
            },
            includes: vec![],
            transactions: vec![],
            creation_time: 0,
            signature: SignatureBytes::default(),
        })
    }

    /// Create a new block, signing it with the provided crypto engine.
    pub fn new_with_crypto(
        authority: Authority,
        round: RoundNumber,
        includes: Vec<BlockReference>,
        transactions: Vec<Transaction>,
        creation_time: u64,
        crypto: &CryptoEngine,
    ) -> Self {
        let signature =
            crypto.sign_block(authority, round, &includes, &transactions, creation_time);
        let digest = crypto.digest(
            authority,
            round,
            &includes,
            &transactions,
            creation_time,
            &signature,
        );
        let reference = BlockReference {
            authority,
            round,
            digest,
        };
        Self {
            reference,
            includes,
            transactions,
            creation_time,
            signature,
        }
    }

    /// Test-only constructor with a default (zero) digest.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn new_for_test(
        authority: Authority,
        round: RoundNumber,
        includes: Vec<BlockReference>,
        transactions: Vec<Transaction>,
        creation_time: u64,
        signature: SignatureBytes,
    ) -> Self {
        Self {
            reference: BlockReference {
                authority,
                round,
                digest: BlockDigest::default(),
            },
            includes,
            transactions,
            creation_time,
            signature,
        }
    }

    /// Verify the integrity and validity of a block received
    /// from the network. This is a security-critical path -- every
    /// check here prevents a Byzantine peer from injecting invalid
    /// blocks into the DAG.
    pub fn verify(
        &self,
        committee: &Committee,
        quorum_threshold: Stake,
        crypto: &CryptoVerifier,
    ) -> eyre::Result<()> {
        let round = self.round();

        // 1. Recompute the digest from the block's fields and
        //    compare it to the claimed digest. This detects any
        //    tampering with the block content.
        let expected = crypto.digest(
            self.author(),
            round,
            &self.includes,
            &self.transactions,
            self.creation_time,
            &self.signature,
        );
        ensure!(
            expected == self.digest(),
            "Digest mismatch: computed {:?}, claimed {:?}",
            expected,
            self.digest()
        );

        // 2. Reject genesis blocks -- they are trusted by
        //    construction and must never arrive over the network.
        ensure!(round != GENESIS_ROUND, "Genesis blocks cannot be verified");

        // 3. Verify the author is a known committee member.
        let Some(public_key) = committee.get_public_key(self.author()) else {
            bail!("Unknown block author {}", self.author());
        };

        // 4. Verify the block signature against the author's
        //    public key.
        crypto.verify_signature(public_key, self)?;

        // 5. Validate each included block reference.
        for include in &self.includes {
            ensure!(
                committee.known_authority(include.authority),
                "Include {include:?} references unknown authority",
            );
            ensure!(
                include.round < round,
                "Include {include:?} round is >= own round {round}",
            );
        }

        // 6. Verify the threshold clock: the block must include
        //    a quorum of blocks from the previous round.
        ensure!(
            threshold_clock_valid_non_genesis(self, committee, quorum_threshold),
            "Threshold clock is not valid"
        );

        Ok(())
    }

    /// The block's unique reference `(authority, round, digest)`.
    pub fn reference(&self) -> &BlockReference {
        &self.reference
    }

    /// Blocks from earlier rounds that this block references.
    pub fn includes(&self) -> &Vec<BlockReference> {
        &self.includes
    }

    /// The transactions batched into this block.
    pub fn transactions(&self) -> &[Transaction] {
        &self.transactions
    }

    /// Iterate transactions paired with their [`TransactionLocator`].
    pub fn located_transactions(&self) -> impl Iterator<Item = (TransactionLocator, &Transaction)> {
        let reference = *self.reference();
        self.transactions.iter().enumerate().map(move |(pos, tx)| {
            let locator = TransactionLocator::new(reference, pos as u64);
            (locator, tx)
        })
    }

    /// The authority that proposed this block.
    pub fn author(&self) -> Authority {
        self.reference.authority
    }

    /// The DAG round this block belongs to.
    pub fn round(&self) -> RoundNumber {
        self.reference.round
    }

    /// The block's content digest.
    pub fn digest(&self) -> BlockDigest {
        self.reference.digest
    }

    /// Return `(authority, round)` for pattern matching and comparison.
    /// For display, use `authority.with_round(round)` instead.
    pub fn author_round(&self) -> (Authority, RoundNumber) {
        self.reference.author_round()
    }

    /// The block's cryptographic signature.
    pub fn signature(&self) -> &SignatureBytes {
        &self.signature
    }

    /// Raw creation time in nanoseconds since epoch.
    pub fn creation_time_ns(&self) -> u64 {
        self.creation_time
    }

    /// Creation time as a [`Duration`] since epoch.
    pub fn creation_time(&self) -> Duration {
        Duration::from_nanos(self.creation_time)
    }
}

/// Compact format with `{:?}`, verbose with `{:#?}`.
impl fmt::Debug for Block {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            write!(f, "Block {:?} {{", self.reference())?;
            write!(
                f,
                "includes({})={:?},",
                self.includes().len(),
                self.includes()
            )?;
            write!(
                f,
                "transactions({})={:?}",
                self.transactions.len(),
                self.transactions()
            )?;
            writeln!(f, "}}")
        } else {
            write!(f, "{}", self)
        }
    }
}

impl fmt::Display for Block {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:[", self.reference)?;
        for (i, include) in self.includes().iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(f, "{}", include)?;
        }
        write!(f, "]({} txs)", self.transactions.len())
    }
}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.reference == other.reference
    }
}

impl Eq for Block {}

impl Hash for Block {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.reference.hash(state);
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    };

    use rand::{Rng, prelude::SliceRandom};

    use super::*;

    /// Test utility for building DAGs from a compact string notation.
    ///
    /// # Format
    ///
    /// ```text
    /// "A1:[A0,B0]; B1:[A0,B0]"
    /// ```
    ///
    /// Each block is `<authority><round>:[<includes>]`, separated by `;`.
    /// Authorities are single uppercase letters (A=0, B=1, ...).
    pub struct Dag(HashMap<BlockReference, Data<Block>>);

    impl Dag {
        /// Parse a DAG from the compact string notation.
        pub fn draw(s: &str) -> Self {
            let mut blocks = HashMap::new();
            for block in s.split(";") {
                let block = Self::draw_block(block);
                blocks.insert(*block.reference(), Data::new(block));
            }
            Self(blocks)
        }

        /// Parse a single block from the compact notation.
        pub fn draw_block(block: &str) -> Block {
            let block = block.trim();
            assert!(block.ends_with(']'), "Invalid block definition: {}", block);
            let block = &block[..block.len() - 1];
            let Some((name, includes)) = block.split_once(":[") else {
                panic!("Invalid block definition: {}", block);
            };
            let reference = Self::parse_name(name);
            let includes = includes.trim();
            let includes = if includes.is_empty() {
                vec![]
            } else {
                let includes = includes.split(',');
                includes.map(Self::parse_name).collect()
            };
            Block {
                reference,
                includes,
                transactions: vec![],
                creation_time: 0,
                signature: Default::default(),
            }
        }

        fn parse_name(s: &str) -> BlockReference {
            let s = s.trim();
            assert!(s.len() >= 2, "Invalid block: {}", s);
            let authority = s.as_bytes()[0];
            let authority = authority.wrapping_sub(b'A');
            assert!(authority < 26, "Invalid block: {}", s);
            let Ok(round): Result<u64, _> = s[1..].parse() else {
                panic!("Invalid block: {}", s);
            };
            BlockReference::new_test(authority as u64, round)
        }

        /// Add genesis blocks (round 0) for every authority referenced
        /// in the DAG, if not already present.
        pub fn add_genesis_blocks(mut self) -> Self {
            for authority in self.authorities() {
                let block = Block::genesis(authority);
                let entry = self.0.entry(*block.reference());
                entry.or_insert_with(move || block);
            }
            self
        }

        /// Iterate blocks in a random order (for testing reorder
        /// tolerance).
        pub fn random_iter(&self, rng: &mut impl Rng) -> RandomDagIter<'_> {
            let mut v: Vec<_> = self.0.keys().cloned().collect();
            v.shuffle(rng);
            RandomDagIter(self, v.into_iter())
        }

        pub fn len(&self) -> usize {
            self.0.len()
        }

        #[allow(dead_code)]
        pub fn is_empty(&self) -> bool {
            self.0.is_empty()
        }

        fn authorities(&self) -> HashSet<Authority> {
            let mut authorities = HashSet::new();
            for (k, v) in &self.0 {
                authorities.insert(k.authority);
                for include in v.includes() {
                    authorities.insert(include.authority);
                }
            }
            authorities
        }

        pub fn committee(&self) -> Arc<Committee> {
            Committee::new_test(vec![1; self.authorities().len()])
        }
    }

    pub struct RandomDagIter<'a>(&'a Dag, std::vec::IntoIter<BlockReference>);

    impl<'a> Iterator for RandomDagIter<'a> {
        type Item = &'a Data<Block>;

        fn next(&mut self) -> Option<Self::Item> {
            let next = self.1.next()?;
            Some(self.0.0.get(&next).unwrap())
        }
    }

    #[test]
    fn test_draw_dag() {
        let d = Dag::draw("A1:[A0, B1]; B2:[B1]").0;
        assert_eq!(d.len(), 2);
        let a0 = BlockReference::new_test(0, 1);
        let b2 = BlockReference::new_test(1, 2);
        assert_eq!(&d.get(&a0).unwrap().reference, &a0);
        assert_eq!(
            &d.get(&a0).unwrap().includes,
            &vec![
                BlockReference::new_test(0, 0),
                BlockReference::new_test(1, 1)
            ]
        );
        assert_eq!(&d.get(&b2).unwrap().reference, &b2);
        assert_eq!(
            &d.get(&b2).unwrap().includes,
            &vec![BlockReference::new_test(1, 1)]
        );
    }
}
