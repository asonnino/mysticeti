// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, fmt};

use crate::{
    authority::Authority,
    block::{Block, BlockReference, RoundNumber},
    committee::Stake,
    data::Data,
    storage::BlockReader,
};

/// Trait that a consensus protocol must implement to
/// work with the DAG layer. The DAG layer calls these
/// methods to determine which leaders to commit or skip,
/// and which leaders the syncer should wait for
/// (liveness).
pub trait DagConsensus: Send + 'static {
    /// The quorum threshold for the DAG's threshold
    /// clock. A block is valid only if it includes
    /// blocks from authorities whose total stake meets
    /// this threshold.
    fn quorum_threshold(&self) -> Stake;

    /// Decide leaders. Returns an ordered sequence of
    /// decided leaders. Idempotent for the same DAG
    /// state. `last_decided` is the slot
    /// `(round, authority)` of the most recently consumed
    /// decision — the caller's cursor. `None` means "no
    /// decision consumed yet".
    fn try_commit(
        &mut self,
        last_decided: Option<(RoundNumber, Authority)>,
    ) -> impl Iterator<Item = LeaderStatus>;

    /// Return the leaders for a given round. The syncer
    /// may give those leaders extra time for liveness.
    /// Returns `None` for asynchronous protocols where
    /// the DAG should wait for all authorities equally.
    fn get_leaders(&self, round: RoundNumber) -> Option<impl Iterator<Item = Authority>>;

    /// The fast direct-commit quorum when it exceeds the threshold-clock quorum, i.e. when
    /// a fast commit can become possible between two own proposals. `None` otherwise.
    fn fast_commit_quorum_above_clock(&self) -> Option<Stake> {
        None
    }
}

/// Which rule of the direct decision committed a leader.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DirectCommitPath {
    /// A `fast_path.commit_quorum` of votes at the voting round.
    Fast,
    /// A `direct_commit_quorum` of certificates at the decision round.
    Slow,
}

/// Which rung of the graded indirect rule committed a leader.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum IndirectCommitPath {
    /// A certificate linked to the anchor.
    Certificate,
    /// A `fast_path.weak_indirect_quorum` of anchor-linked votes.
    WeakQuorum,
}

/// The status of every leader output by the committers. While the core only cares about committed
/// leaders, providing a richer status allows for easier debugging, testing, and composition with
/// advanced commit strategies.
#[derive(Debug, Eq, PartialEq)]
pub enum LeaderStatus {
    DirectCommit(Data<Block>, DirectCommitPath),
    IndirectCommit(Data<Block>, IndirectCommitPath),
    DirectSkip(Authority, RoundNumber),
    IndirectSkip(Authority, RoundNumber),
    Undecided(Authority, RoundNumber),
}

// The committer buffers statuses in a `VecDeque`; the path tags must fit in the padding.
const _: () = assert!(std::mem::size_of::<LeaderStatus>() == 24);

impl LeaderStatus {
    pub fn round(&self) -> RoundNumber {
        match self {
            Self::DirectCommit(block, _) | Self::IndirectCommit(block, _) => block.round(),
            Self::DirectSkip(_, round)
            | Self::IndirectSkip(_, round)
            | Self::Undecided(_, round) => *round,
        }
    }

    pub fn authority(&self) -> Authority {
        match self {
            Self::DirectCommit(block, _) | Self::IndirectCommit(block, _) => block.author(),
            Self::DirectSkip(authority, _)
            | Self::IndirectSkip(authority, _)
            | Self::Undecided(authority, _) => *authority,
        }
    }

    /// True for every variant except `Undecided`.
    pub fn is_decided(&self) -> bool {
        !matches!(self, Self::Undecided(..))
    }

    pub fn into_decided_block(self) -> Option<Data<Block>> {
        match self {
            Self::DirectCommit(block, _) | Self::IndirectCommit(block, _) => Some(block),
            Self::DirectSkip(..) | Self::IndirectSkip(..) => None,
            Self::Undecided(..) => {
                panic!(
                    "Decided block must be DirectCommit/IndirectCommit or DirectSkip/IndirectSkip"
                )
            }
        }
    }
}

impl PartialOrd for LeaderStatus {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LeaderStatus {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.round(), self.authority()).cmp(&(other.round(), other.authority()))
    }
}

impl fmt::Display for LeaderStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DirectCommit(block, path) => {
                write!(f, "DirectCommit({}, {path:?})", block.reference())
            }
            Self::IndirectCommit(block, path) => {
                write!(f, "IndirectCommit({}, {path:?})", block.reference())
            }
            Self::DirectSkip(a, r) => write!(f, "DirectSkip({})", a.with_round(*r)),
            Self::IndirectSkip(a, r) => write!(f, "IndirectSkip({})", a.with_round(*r)),
            Self::Undecided(a, r) => write!(f, "Undecided({})", a.with_round(*r)),
        }
    }
}

/// The output of consensus is an ordered list of [`CommittedSubDag`]. The
/// application can arbitrarily sort the blocks within each sub-dag (but using
/// a deterministic algorithm).
pub struct CommittedSubDag {
    /// A reference to the anchor of the sub-dag
    pub anchor: BlockReference,
    /// All the committed blocks that are part of this sub-dag
    pub blocks: Vec<Data<Block>>,
}

impl CommittedSubDag {
    /// Create new (empty) sub-dag.
    pub fn new(anchor: BlockReference, blocks: Vec<Data<Block>>) -> Self {
        Self { anchor, blocks }
    }

    /// Sort the blocks of the sub-dag by round number. Any deterministic
    /// algorithm works.
    pub fn sort(&mut self) {
        self.blocks.sort_by_key(|x| x.round());
    }

    /// Number of transactions produced by `Transaction::new_for_test` in this sub-dag.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn count_test_transactions(&self) -> usize {
        self.blocks
            .iter()
            .flat_map(|block| block.transactions())
            .filter(|transaction| transaction.has_test_marker())
            .count()
    }
}

impl fmt::Debug for CommittedSubDag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.anchor)?;
        for block in &self.blocks {
            write!(f, "{}, ", block.reference())?;
        }
        write!(f, ")")
    }
}

/// Expand a committed sequence of leader into a sequence of sub-dags.
#[derive(Default)]
pub struct Linearizer {
    /// Keep track of all committed blocks to avoid committing the same block
    /// twice.
    pub committed: HashSet<BlockReference>,
}

impl Linearizer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Collect the sub-dag from a specific anchor excluding any duplicates or
    /// blocks that have already been committed (within previous sub-dags).
    fn collect_sub_dag(
        &mut self,
        block_reader: &BlockReader,
        leader_block: Data<Block>,
    ) -> CommittedSubDag {
        let mut to_commit = Vec::new();

        let leader_block_ref = *leader_block.reference();
        let mut buffer = vec![leader_block];
        assert!(self.committed.insert(leader_block_ref));
        while let Some(x) = buffer.pop() {
            to_commit.push(x.clone());
            for reference in x.includes() {
                let block = block_reader
                    .get_block(*reference)
                    .expect("We should have the whole sub-dag by now");

                if self.committed.insert(*reference) {
                    buffer.push(block);
                }
            }
        }
        CommittedSubDag::new(leader_block_ref, to_commit)
    }

    pub fn handle_commit(
        &mut self,
        block_reader: &BlockReader,
        committed_leaders: Vec<Data<Block>>,
    ) -> Vec<CommittedSubDag> {
        let mut committed = vec![];
        for leader_block in committed_leaders {
            let mut sub_dag = self.collect_sub_dag(block_reader, leader_block);
            sub_dag.sort();
            committed.push(sub_dag);
        }
        committed
    }
}
