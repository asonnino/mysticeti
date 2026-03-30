// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, HashSet, VecDeque};

use minibytes::Bytes;

use crate::{
    block_store::{CommitData, OwnBlockData},
    core::MetaStatement,
    data::Data,
    types::{BlockReference, StatementBlock},
    wal::WalPosition,
};

pub struct RecoveredState {
    pub last_own_block: Option<OwnBlockData>,
    pub pending: VecDeque<(WalPosition, MetaStatement)>,
    pub state: Option<Bytes>,
    pub unprocessed_blocks: Vec<Data<StatementBlock>>,

    pub last_committed_leader: Option<BlockReference>,
    pub committed_blocks: HashSet<BlockReference>,
    pub committed_state: Option<Bytes>,
}

#[derive(Default)]
pub(super) struct RecoveredStateBuilder {
    pending: BTreeMap<WalPosition, RawMetaStatement>,
    last_own_block: Option<OwnBlockData>,
    state: Option<Bytes>,
    unprocessed_blocks: Vec<Data<StatementBlock>>,

    last_committed_leader: Option<BlockReference>,
    committed_blocks: HashSet<BlockReference>,
    committed_state: Option<Bytes>,
}

impl RecoveredStateBuilder {
    pub(super) fn new() -> Self {
        Self::default()
    }

    pub(super) fn block(&mut self, pos: WalPosition, block: &Data<StatementBlock>) {
        self.pending
            .insert(pos, RawMetaStatement::Include(*block.reference()));
        self.unprocessed_blocks.push(block.clone());
    }

    pub(super) fn payload(&mut self, pos: WalPosition, payload: Bytes) {
        self.pending.insert(pos, RawMetaStatement::Payload(payload));
    }

    pub(super) fn own_block(&mut self, own_block_data: OwnBlockData) {
        // Edge case of WalPosition::MAX is automatically handled here, empty map is returned
        self.pending = self.pending.split_off(&own_block_data.next_entry);
        self.unprocessed_blocks.push(own_block_data.block.clone());
        self.last_own_block = Some(own_block_data);
    }

    pub(super) fn state(&mut self, state: Bytes) {
        self.state = Some(state);
        self.unprocessed_blocks.clear();
    }

    pub(super) fn commit_data(&mut self, commits: Vec<CommitData>, committed_state: Bytes) {
        for commit_data in commits {
            self.last_committed_leader = Some(commit_data.leader);
            self.committed_blocks
                .extend(commit_data.sub_dag.into_iter());
        }
        self.committed_state = Some(committed_state);
    }

    pub(super) fn build(self) -> RecoveredState {
        let pending = self
            .pending
            .into_iter()
            .map(|(pos, raw)| (pos, raw.into_meta_statement()))
            .collect();
        RecoveredState {
            pending,
            last_own_block: self.last_own_block,
            state: self.state,
            unprocessed_blocks: self.unprocessed_blocks,
            last_committed_leader: self.last_committed_leader,
            committed_blocks: self.committed_blocks,
            committed_state: self.committed_state,
        }
    }
}

enum RawMetaStatement {
    Include(BlockReference),
    Payload(Bytes),
}

impl RawMetaStatement {
    fn into_meta_statement(self) -> MetaStatement {
        match self {
            RawMetaStatement::Include(include) => MetaStatement::Include(include),
            RawMetaStatement::Payload(payload) => MetaStatement::Payload(
                bincode::deserialize(&payload).expect("Failed to deserialize payload"),
            ),
        }
    }
}
