// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, ops::Range};

use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::{
    certifier::{CertificateType, Vote},
    committee::Committee,
    crypto::Signer,
    types::{AuthorityIndex, BlockReference, RoundNumber},
};

pub struct VoterMessage {
    reference: BlockReference,
    parents: Vec<BlockReference>,
}

impl VoterMessage {
    pub fn own_block(reference: BlockReference, parents: Vec<BlockReference>) -> Self {
        Self { reference, parents }
    }

    pub fn others_block(reference: BlockReference) -> Self {
        Self {
            reference,
            parents: vec![],
        }
    }
}

pub struct Voter {
    authority_index: AuthorityIndex,
    authority_indices: Range<AuthorityIndex>,
    cannot_vote: HashSet<(AuthorityIndex, RoundNumber)>,
    min_voting_round: RoundNumber,
    signer: Signer,
    tx_vote: Sender<Vote>,
}

impl Voter {
    const MAX_VOTING_DEPTH: RoundNumber = 100;

    pub fn new(
        authority_index: AuthorityIndex,
        signer: Signer,
        committee: &Committee,
        tx_vote: Sender<Vote>,
    ) -> Self {
        let authority_indices = 0..committee.len() as AuthorityIndex;
        let cannot_vote = HashSet::new();
        let min_voting_round = 0;

        Self {
            authority_index,
            authority_indices,
            cannot_vote,
            min_voting_round,
            signer,
            tx_vote,
        }
    }

    pub fn process_own_block(&mut self, reference: BlockReference, parents: Vec<BlockReference>) {
        let round = reference.round;

        // We do not vote for blocks that we blamed.
        let parents_authors = parents.iter().map(|p| &p.authority).collect::<HashSet<_>>();
        for i in self.authority_indices.clone() {
            if !parents_authors.contains(&i) {
                self.cannot_vote.insert((i, round));
            }
        }
    }

    pub async fn process_others_block(&mut self, reference: BlockReference) {
        let round = reference.round;

        // Check if the block is too old to vote for.
        if round < self.min_voting_round {
            tracing::debug!("Block is too old to vote for: {:?}", reference);
            return;
        }

        // Check if we can vote for this block and insert it into the set.
        let author_round = reference.author_round();
        if !self.cannot_vote.insert(author_round) {
            tracing::debug!(
                "Cannot vote for blamed or conflicting block: {:?}",
                author_round
            );
            return;
        }

        // Clean up.
        if round > self.min_voting_round + Self::MAX_VOTING_DEPTH {
            self.min_voting_round = round - Self::MAX_VOTING_DEPTH;
            self.cannot_vote
                .retain(|(_, r)| *r >= self.min_voting_round);
        }

        // Send the vote.
        let vote = Vote::new(
            &self.signer,
            self.authority_index,
            reference,
            CertificateType::C0,
        );

        self.tx_vote.send(vote).await.expect("Failed to send vote");
    }

    pub async fn run(mut self, mut receiver: Receiver<VoterMessage>) {
        while let Some(message) = receiver.recv().await {
            if message.reference.authority == self.authority_index {
                self.process_own_block(message.reference, message.parents);
            } else {
                self.process_others_block(message.reference).await;
            }
        }
    }
}

impl Voter {
    pub fn spawn(self, receiver: Receiver<VoterMessage>) -> JoinHandle<()> {
        tokio::spawn(async move {
            self.run(receiver).await;
        })
    }
}
