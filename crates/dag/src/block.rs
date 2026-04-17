// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub(crate) mod crypto;
pub mod data;
pub mod reference;
pub(crate) mod serde;
pub mod transaction;

pub use reference::BlockReference;
use std::{fmt, time::Duration};

use ::serde::{Deserialize, Serialize};
use eyre::{bail, ensure};

use self::{
    crypto::{BlockDigest, SignatureBytes, Signer},
    data::Data,
    transaction::{Transaction, TransactionLocator},
};
use crate::{
    authority::Authority,
    committee::{Committee, Stake},
    core::threshold_clock::threshold_clock_valid_non_genesis,
};

pub type RoundNumber = u64;
pub type TimestampNs = u128;
const NANOS_IN_SEC: u128 = Duration::from_secs(1).as_nanos();
const GENESIS_ROUND: RoundNumber = 0;

#[derive(Clone, Serialize, Deserialize)]
// Important. Adding fields here requires updating
// BlockDigest::new, and Block::verify
pub struct Block {
    reference: BlockReference,

    //  A list of block references to other blocks that this
    //  block includes. Note that the order matters: if a
    //  reference to two blocks from the same round and same
    //  authority are included, then the first reference is
    //  the one that this block votes for.
    includes: Vec<BlockReference>,

    // A list of transactions in order.
    transactions: Vec<Transaction>,

    // Creation time of the block as reported by creator,
    // currently not enforced.
    meta_creation_time_ns: TimestampNs,

    // Signature by the block author
    signature: SignatureBytes,
}

impl Block {
    pub fn new_genesis(authority: Authority) -> Data<Self> {
        Data::new(Self::new(
            authority,
            GENESIS_ROUND,
            vec![],
            vec![],
            0,
            SignatureBytes::default(),
        ))
    }

    pub fn new_with_signer(
        authority: Authority,
        round: RoundNumber,
        includes: Vec<BlockReference>,
        transactions: Vec<Transaction>,
        meta_creation_time_ns: TimestampNs,
        signer: &Signer,
    ) -> Self {
        let signature = signer.sign_block(
            authority,
            round,
            &includes,
            &transactions,
            meta_creation_time_ns,
        );
        Self::new(
            authority,
            round,
            includes,
            transactions,
            meta_creation_time_ns,
            signature,
        )
    }

    pub fn new(
        authority: Authority,
        round: RoundNumber,
        includes: Vec<BlockReference>,
        transactions: Vec<Transaction>,
        meta_creation_time_ns: TimestampNs,
        signature: SignatureBytes,
    ) -> Self {
        Self {
            reference: BlockReference {
                authority,
                round,
                digest: BlockDigest::new(
                    authority,
                    round,
                    &includes,
                    &transactions,
                    meta_creation_time_ns,
                    &signature,
                ),
            },
            includes,
            transactions,
            meta_creation_time_ns,
            signature,
        }
    }

    pub fn reference(&self) -> &BlockReference {
        &self.reference
    }

    pub fn includes(&self) -> &Vec<BlockReference> {
        &self.includes
    }

    pub fn transactions(&self) -> &[Transaction] {
        &self.transactions
    }

    pub fn shared_transactions(&self) -> impl Iterator<Item = (TransactionLocator, &Transaction)> {
        let reference = *self.reference();
        self.transactions.iter().enumerate().map(move |(pos, tx)| {
            let locator = TransactionLocator::new(reference, pos as u64);
            (locator, tx)
        })
    }

    pub fn author(&self) -> Authority {
        self.reference.authority
    }

    pub fn round(&self) -> RoundNumber {
        self.reference.round
    }

    pub fn digest(&self) -> BlockDigest {
        self.reference.digest
    }

    pub fn author_round(&self) -> (Authority, RoundNumber) {
        self.reference.author_round()
    }

    pub fn signature(&self) -> &SignatureBytes {
        &self.signature
    }

    pub fn meta_creation_time_ns(&self) -> TimestampNs {
        self.meta_creation_time_ns
    }

    pub fn meta_creation_time(&self) -> Duration {
        // Some context:
        // https://github.com/rust-lang/rust/issues/51107
        let secs = self.meta_creation_time_ns / NANOS_IN_SEC;
        let nanos = self.meta_creation_time_ns % NANOS_IN_SEC;
        Duration::new(secs as u64, nanos as u32)
    }

    pub fn verify(&self, committee: &Committee, quorum_threshold: Stake) -> eyre::Result<()> {
        let round = self.round();
        let digest = BlockDigest::new(
            self.author(),
            round,
            &self.includes,
            &self.transactions,
            self.meta_creation_time_ns,
            &self.signature,
        );
        ensure!(
            digest == self.digest(),
            "Digest does not match, calculated {:?}, provided {:?}",
            digest,
            self.digest()
        );
        let pub_key = committee.get_public_key(self.author());
        let Some(pub_key) = pub_key else {
            bail!("Unknown block author {}", self.author())
        };
        if round == GENESIS_ROUND {
            bail!("Genesis block should not go through verification");
        }
        if let Err(e) = pub_key.verify_block(self) {
            bail!("Block signature verification has failed: {:?}", e);
        }
        for include in &self.includes {
            // Also check duplicate includes?
            ensure!(
                committee.known_authority(include.authority),
                "Include {:?} references unknown authority",
                include
            );
            ensure!(
                include.round < round,
                "Include {:?} round is >= own round {}",
                include,
                round
            );
        }
        ensure!(
            threshold_clock_valid_non_genesis(self, committee, quorum_threshold,),
            "Threshold clock is not valid"
        );
        Ok(())
    }

    pub fn detailed(&self) -> Detailed<'_> {
        Detailed(self)
    }
}

impl fmt::Debug for Block {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

pub struct Detailed<'a>(&'a Block);

impl<'a> fmt::Debug for Detailed<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Block {:?} {{", self.0.reference())?;
        write!(
            f,
            "includes({})={:?},",
            self.0.includes().len(),
            self.0.includes()
        )?;
        write!(
            f,
            "transactions({})={:?}",
            self.0.transactions.len(),
            self.0.transactions()
        )?;
        writeln!(f, "}}")
    }
}

impl fmt::Display for Block {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:[", self.reference)?;
        for include in self.includes() {
            write!(f, "{},", include)?;
        }
        write!(f, "](")?;
        for tx in self.transactions() {
            write!(f, "{},", tx)?;
        }
        write!(f, ")")
    }
}

impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.reference == other.reference
    }
}

impl Eq for Block {}

impl std::hash::Hash for Block {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
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

    pub struct Dag(HashMap<BlockReference, Data<Block>>);

    #[cfg(test)]
    impl Dag {
        /// Takes a string in form
        /// "Block:[Dependencies, ...]; ..."
        /// Where Block is one letter denoting a node and a
        /// number denoting a round. For example B3 is a block
        /// for round 3 made by validator index 2.
        /// Note that blocks are separated with semicolon(;)
        /// and dependencies within a block are separated with
        /// comma(,).
        pub fn draw(s: &str) -> Self {
            let mut blocks = HashMap::new();
            for block in s.split(";") {
                let block = Self::draw_block(block);
                blocks.insert(*block.reference(), Data::new(block));
            }
            Self(blocks)
        }

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
                meta_creation_time_ns: 0,
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

        /// For each authority add a 0 round block if not
        /// present.
        pub fn add_genesis_blocks(mut self) -> Self {
            for authority in self.authorities() {
                let block = Block::new_genesis(authority);
                let entry = self.0.entry(*block.reference());
                entry.or_insert_with(move || block);
            }
            self
        }

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
