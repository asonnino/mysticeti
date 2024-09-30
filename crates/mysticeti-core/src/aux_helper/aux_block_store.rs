// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use parking_lot::RwLock;

use crate::{
    aux_node::aux_config::{AuxNodeParameters, AuxiliaryCommittee},
    crypto::BlockDigest,
    data::Data,
    types::{AuthorityIndex, BlockReference, RoundNumber, Stake, StatementBlock},
};

type BlockData = HashMap<(AuthorityIndex, BlockDigest), Data<StatementBlock>>;

/// Use by Core validators to track of certified auxiliary stake.
/// NOTE: The threshold can be met multiple times if `auxiliary_committee.liveness_threshold`
/// is less than half the total stake.
#[derive(Default)]
struct Aggregator {
    total_stake: Stake,
    authorities: HashSet<AuthorityIndex>,
    references: Vec<BlockReference>,
}

impl Aggregator {
    pub fn add(
        &mut self,
        reference: BlockReference,
        aux_committee: &AuxiliaryCommittee,
        aux_node_parameters: &AuxNodeParameters,
    ) -> bool {
        let authority = reference.authority;
        if self.authorities.insert(authority) {
            self.references.push(reference);
            let stake = aux_committee
                .get_stake(authority)
                .expect("Authority should be known");
            self.total_stake += stake;
        }
        self.total_stake >= aux_node_parameters.liveness_threshold
    }

    pub fn clear(&mut self) {
        self.total_stake = 0;
        self.authorities.clear();
        self.references.clear();
    }
}

/// Blocks indexed by round number and authority.
/// TODO: Persist the blocks through the WAL.
#[derive(Default)]
pub struct AuxiliaryBlockStoreInner {
    blocks: HashMap<RoundNumber, (Aggregator, BlockData)>,
    highest_threshold_round: RoundNumber,
    next_weak_links: Vec<BlockReference>,
}

impl AuxiliaryBlockStoreInner {
    pub fn certify_block(
        &mut self,
        reference: BlockReference,
        auxiliary_committee: &Arc<AuxiliaryCommittee>,
        aux_node_parameters: &AuxNodeParameters,
    ) {
        let round = reference.round;
        let (aggregator, _) = self.blocks.get_mut(&round).expect("Block should exist");
        let threshold = aggregator.add(reference, auxiliary_committee, aux_node_parameters);

        // Weak links are replaced every time the aggregator reaches the threshold.
        if threshold {
            if round > self.highest_threshold_round {
                self.highest_threshold_round = round;
                self.next_weak_links = aggregator.references.drain(..).collect();
            }
            aggregator.clear();
        }
    }
}

#[derive(Clone)]
pub struct AuxiliaryBlockStore {
    aux_committee: Arc<AuxiliaryCommittee>,
    aux_node_parameters: AuxNodeParameters,
    inner: Arc<RwLock<AuxiliaryBlockStoreInner>>,
}

impl AuxiliaryBlockStore {
    pub fn new(
        aux_committee: Arc<AuxiliaryCommittee>,
        aux_node_parameters: AuxNodeParameters,
    ) -> Self {
        Self {
            aux_committee,
            aux_node_parameters,
            inner: Arc::new(RwLock::new(AuxiliaryBlockStoreInner::default())),
        }
    }

    pub fn get_weak_links(&self, round: RoundNumber) -> Option<Vec<BlockReference>> {
        if !self.aux_node_parameters.inclusion_round(round) {
            return Some(vec![]);
        }

        let inner = self.inner.read();
        // NOTE: Do not ebforce weak links.
        // if inner.highest_threshold_round == 0 || round < inner.highest_threshold_round {
        //     return None;
        // }

        Some(inner.next_weak_links.clone())
    }

    pub fn block_exists(&self, reference: &BlockReference) -> bool {
        self.inner
            .read()
            .blocks
            .get(&reference.round)
            .map(|(_, block_data)| {
                block_data.contains_key(&(reference.authority, reference.digest))
            })
            .unwrap_or(false)
    }

    pub fn add_auxiliary_block(&self, block: Data<StatementBlock>) {
        let mut inner = self.inner.write();
        let (_, block_data) = inner
            .blocks
            .entry(block.round())
            .or_insert_with(|| (Aggregator::default(), BlockData::new()));
        block_data.insert(block.reference().author_digest(), block);
    }

    pub fn get_block_includes(&self, reference: &BlockReference) -> Option<Vec<BlockReference>> {
        self.inner
            .read()
            .blocks
            .get(&reference.round)
            .map(|(_, block_data)| block_data.get(&reference.author_digest()))
            .flatten()
            .map(|block| block.includes().clone())
    }

    pub fn add_certificate(&self, reference: BlockReference) {
        self.inner
            .write()
            .certify_block(reference, &self.aux_committee, &self.aux_node_parameters);
    }

    pub fn safe_to_vote(&self, reference: &BlockReference) -> bool {
        let inner = self.inner.read();
        let (_, block_data) = inner
            .blocks
            .get(&reference.round)
            .expect("Block should exist");
        let equivocation = block_data
            .keys()
            .find(|(authority, _)| authority == &reference.authority);
        equivocation.is_none()
    }
}
