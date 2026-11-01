// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

use dag::{
    authority::Authority,
    block::{BlockReference, RoundNumber},
};

/// Memoizes which block of a slot (`author`, `round`) a given block supports.
///
/// A block never changes and its causal history is complete once it is stored, so the answer
/// for a (block, slot) pair is final and never needs invalidating. Every base committer asks
/// the same question about the same blocks, so they share one cache. Entries are grouped by
/// slot round to drop everything below the last decided round in one step.
#[derive(Clone, Default)]
pub(crate) struct SupportCache {
    entries: Arc<Mutex<Entries>>,
}

/// Slot round, then (block, slot author), to the block of the slot that the block supports.
type Entries = BTreeMap<RoundNumber, HashMap<(BlockReference, Authority), Option<BlockReference>>>;

impl SupportCache {
    /// The cached support of `from` for the slot, or `None` if it was not computed yet.
    pub(crate) fn get(
        &self,
        (author, round): (Authority, RoundNumber),
        from: &BlockReference,
    ) -> Option<Option<BlockReference>> {
        let entries = self.entries.lock().expect("support cache poisoned");
        entries.get(&round)?.get(&(*from, author)).copied()
    }

    pub(crate) fn insert(
        &self,
        (author, round): (Authority, RoundNumber),
        from: BlockReference,
        support: Option<BlockReference>,
    ) {
        let mut entries = self.entries.lock().expect("support cache poisoned");
        entries
            .entry(round)
            .or_default()
            .insert((from, author), support);
    }

    /// Forget the slots below `round`.
    pub(crate) fn prune_below(&self, round: RoundNumber) {
        let mut entries = self.entries.lock().expect("support cache poisoned");
        *entries = entries.split_off(&round);
    }
}
