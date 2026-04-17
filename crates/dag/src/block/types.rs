// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub use crate::authority::Authority;

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Default)]
pub struct Transaction {
    data: Bytes,
}

pub type RoundNumber = u64;
pub type BlockDigest = super::crypto::BlockDigest;
pub type Stake = u64;
pub type KeyPair = u64;
pub type PublicKey = super::crypto::PublicKey;

use std::{
    fmt,
    hash::{Hash, Hasher},
    time::Duration,
};

use digest::Digest;
use eyre::{bail, ensure};
use minibytes::Bytes;
use serde::{Deserialize, Serialize};
#[cfg(test)]
pub use test::Dag;

use super::{
    crypto::{AsBytes, CryptoHash, SignatureBytes, Signer},
    data::Data,
};
use crate::{committee::Committee, core::threshold_clock::threshold_clock_valid_non_genesis};

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

#[derive(Clone, Serialize, Deserialize)]
// Important. Adding fields here requires updating BlockDigest::new, and StatementBlock::verify
pub struct StatementBlock {
    reference: BlockReference,

    //  A list of block references to other blocks that this block includes
    //  Note that the order matters: if a reference to two blocks
    //  from the same round and same authority are included, then
    //  the first reference is the one that this block votes for.
    includes: Vec<BlockReference>,

    // A list of transactions in order.
    transactions: Vec<Transaction>,

    // Creation time of the block as reported by creator, currently not enforced
    meta_creation_time_ns: TimestampNs,

    // Signature by the block author
    signature: SignatureBytes,
}

#[derive(Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Default)]
pub struct AuthoritySet(u128); // todo - support more then 128 authorities

pub type TimestampNs = u128;
const NANOS_IN_SEC: u128 = Duration::from_secs(1).as_nanos();

const GENESIS_ROUND: RoundNumber = 0;

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

impl StatementBlock {
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
        // Some context: https://github.com/rust-lang/rust/issues/51107
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
                "Include {:?} round is greater or equal to own round {}",
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

#[derive(Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Default)]
pub struct TransactionLocator {
    block: BlockReference,
    offset: u64,
}

impl TransactionLocator {
    pub(crate) fn new(block: BlockReference, offset: u64) -> Self {
        Self { block, offset }
    }

    pub fn block(&self) -> &BlockReference {
        &self.block
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }
}

impl BlockReference {
    #[cfg(any(test, feature = "test-utils"))]
    pub fn new_test(authority: u64, round: RoundNumber) -> Self {
        let authority = Authority::new(authority);
        if round == 0 {
            StatementBlock::new_genesis(authority).reference
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

impl fmt::Debug for StatementBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

pub struct Detailed<'a>(&'a StatementBlock);

impl<'a> fmt::Debug for Detailed<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StatementBlock {:?} {{", self.0.reference())?;
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

impl fmt::Display for StatementBlock {
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

impl fmt::Debug for TransactionLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl fmt::Display for TransactionLocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.block, self.offset)
    }
}

impl PartialEq for StatementBlock {
    fn eq(&self, other: &Self) -> bool {
        self.reference == other.reference
    }
}

impl Eq for StatementBlock {}

impl std::hash::Hash for StatementBlock {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.reference.hash(state);
    }
}

impl CryptoHash for BlockReference {
    fn crypto_hash(&self, state: &mut impl Digest) {
        self.authority.as_u64().crypto_hash(state);
        self.round.crypto_hash(state);
        self.digest.crypto_hash(state);
    }
}

impl CryptoHash for TransactionLocator {
    fn crypto_hash(&self, state: &mut impl Digest) {
        self.block.crypto_hash(state);
        self.offset.crypto_hash(state);
    }
}

impl fmt::Display for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tx")
    }
}

impl fmt::Debug for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tx({}B)", self.data.len())
    }
}

impl Transaction {
    pub fn new(data: Bytes) -> Self {
        Self { data }
    }

    #[allow(dead_code)]
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    #[allow(dead_code)]
    pub fn into_data(self) -> Bytes {
        self.data
    }

    pub fn extract_timestamp(&self) -> Duration {
        let bytes = self.data[0..8]
            .try_into()
            .expect("Transaction should be at least 8 bytes");
        Duration::from_millis(u64::from_le_bytes(bytes))
    }
}

impl AsBytes for Transaction {
    fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    };

    use rand::{Rng, prelude::SliceRandom};

    use super::*;

    pub struct Dag(HashMap<BlockReference, Data<StatementBlock>>);

    #[cfg(test)]
    impl Dag {
        /// Takes a string in form "Block:[Dependencies, ...]; ..."
        /// Where Block is one letter denoting a node and a number denoting a round
        /// For example B3 is a block for round 3 made by validator index 2
        /// Note that blocks are separated with semicolon(;) and
        /// dependencies within a block are separated with comma(,)
        pub fn draw(s: &str) -> Self {
            let mut blocks = HashMap::new();
            for block in s.split(";") {
                let block = Self::draw_block(block);
                blocks.insert(*block.reference(), Data::new(block));
            }
            Self(blocks)
        }

        pub fn draw_block(block: &str) -> StatementBlock {
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
            StatementBlock {
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

        /// For each authority add a 0 round block if not present
        pub fn add_genesis_blocks(mut self) -> Self {
            for authority in self.authorities() {
                let block = StatementBlock::new_genesis(authority);
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
        type Item = &'a Data<StatementBlock>;

        fn next(&mut self) -> Option<Self::Item> {
            let next = self.1.next()?;
            Some(self.0.0.get(&next).unwrap())
        }
    }

    #[test]
    fn test_draw_dag() {
        let d = Dag::draw("A1:[A0, B1]; B2:[B1]").0;
        assert_eq!(d.len(), 2);
        let a0: BlockReference = BlockReference::new_test(0, 1);
        let b2: BlockReference = BlockReference::new_test(1, 2);
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
