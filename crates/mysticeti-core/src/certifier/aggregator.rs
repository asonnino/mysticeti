// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, HashSet};

use super::{Certificate, Vote};
use crate::{
    certifier::CertificateType,
    committee::Committee,
    types::{AuthorityIndex, RoundNumber, Stake},
};

struct Aggregator {
    initial_stake: Stake,
    total_stake: Stake,
    voters: HashSet<AuthorityIndex>,
    certificate: Certificate,
}

impl Aggregator {
    pub fn new(certificate: Certificate, committee: &Committee) -> Self {
        let initial_stake = committee
            .get_stake(certificate.reference.authority)
            .expect("Our id is not in the committee");

        Self {
            initial_stake,
            total_stake: initial_stake,
            voters: HashSet::new(),
            certificate,
        }
    }

    pub fn add(&mut self, vote: &Vote, committee: &Committee) -> Option<Certificate> {
        let voter = vote.authority;
        if self.voters.insert(voter) {
            let stake = committee
                .get_stake(voter)
                .expect("Authority should be known");
            self.total_stake += stake;

            self.certificate.add(vote);
        }
        if committee.is_quorum(self.total_stake) {
            Some(self.certificate.clone())
        } else {
            None
        }
    }

    pub fn take_certificate(self) -> Certificate {
        self.certificate
    }

    pub fn certificate_type(&self) -> CertificateType {
        self.certificate.certificate_type
    }

    pub fn collect_c1(&mut self) {
        self.certificate.certificate_type = CertificateType::C1;
        self.voters.clear();
        self.total_stake = self.initial_stake;
    }
}

pub struct AggregatorCollection {
    aggregators: HashMap<RoundNumber, Aggregator>,
}

impl AggregatorCollection {
    const GC_ROUND: RoundNumber = 100;

    pub fn new() -> Self {
        Self {
            aggregators: HashMap::with_capacity(2 * Self::GC_ROUND as usize),
        }
    }

    /// Add a vote to the aggregator collection. This function assumes that the vote is valid, that is,
    /// (1) vote.verify() succeeds, and (2) the vote is for a block authored by this authority.
    pub fn add(&mut self, vote: Vote, committee: &Committee) -> Option<Certificate> {
        let round = vote.round();

        // Add the vote to the aggregator. A new aggregator is initialized if it does not exist.
        let aggregator = self.aggregators.entry(round).or_insert_with(|| {
            let reference = vote.reference.clone();
            let certificate = Certificate::new(reference, vote.certificate_type);
            Aggregator::new(certificate, committee)
        });

        // Check if the certificate type matches. It is impossible to receive a vote for a C1
        // certificate before creating a C0 certificate.
        if aggregator.certificate_type() != vote.certificate_type {
            return None;
        }

        // Check if the vote allows to create a certificate.
        if let Some(certificate) = aggregator.add(&vote, committee) {
            // The aggregator now collects C1 certificates.
            if aggregator.certificate_type() == CertificateType::C0 {
                aggregator.collect_c1();
            }

            // Cleanup
            self.aggregators.retain(|r, _| *r + Self::GC_ROUND > round);

            return Some(certificate);
        }
        return None;
    }
}
