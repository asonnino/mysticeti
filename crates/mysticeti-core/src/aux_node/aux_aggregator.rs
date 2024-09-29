// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;

use super::aux_message::PartialAuxiliaryCertificate;
use crate::{committee::Committee, types::BlockReference};

#[derive(Clone)]
pub struct AuxiliaryAggregator {
    core_committee: Arc<Committee>,
    aggregators: Arc<RwLock<HashMap<BlockReference, PartialAuxiliaryCertificate>>>,
}

impl AuxiliaryAggregator {
    pub fn new(core_committee: Arc<Committee>) -> Self {
        Self {
            core_committee,
            aggregators: Default::default(),
        }
    }

    /// Add a vote to the aggregator and return a certificate if the threshold is reached.
    pub fn add_vote(
        &self,
        vote: PartialAuxiliaryCertificate,
    ) -> Option<PartialAuxiliaryCertificate> {
        let reference = vote.block_reference;

        let mut write_guard = self.aggregators.write();
        let complete = match write_guard.get_mut(&reference) {
            Some(aggregator) => {
                *aggregator = PartialAuxiliaryCertificate::merge(vec![aggregator.clone(), vote])
                    .expect("Aggregators should be mergeable");
                aggregator.is_full_certificate(&self.core_committee).is_ok()
            }
            None => {
                let complete = vote.is_full_certificate(&self.core_committee).is_ok();
                write_guard.insert(reference.clone(), vote);
                complete
            }
        };

        if complete {
            Some(write_guard.remove(&reference).expect("Already checked"))
        } else {
            None
        }
    }
}
