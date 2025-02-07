// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::{self, Debug, Formatter};

use eyre::{bail, ensure};
use serde::{Deserialize, Serialize};

use crate::{
    committee::Committee,
    crypto::{SignatureBytes, Signer},
    types::{AuthorityIndex, BlockReference, RoundNumber, Stake},
};

mod aggregator;
mod core;
mod voter;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CertifierMessage {
    OwnBlock(BlockReference, Vec<BlockReference>),
    OthersBlock(BlockReference),
    Vote(Vote),
    Certificate(Certificate),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CertificateType {
    C0,
    C1,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Vote {
    /// The authority that voted.
    authority: AuthorityIndex,
    /// The reference of the block that was voted for.
    reference: BlockReference,
    /// The signature over the block digest.
    signature: SignatureBytes,
    /// Set to true if this is a C1 certificate and set to false if it is a C0 certificate.
    certificate_type: CertificateType,
}

impl Debug for Vote {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "V{}([{}]->[{}], {})",
            self.reference.round, self.authority, self.reference.authority, self.reference.digest
        )
    }
}

impl Vote {
    pub fn new(
        signer: &Signer,
        authority: AuthorityIndex,
        reference: BlockReference,
        certificate_type: CertificateType,
    ) -> Self {
        let signature = signer.sign_digest(&reference.digest);
        Self {
            authority,
            reference,
            signature,
            certificate_type,
        }
    }

    pub fn round(&self) -> RoundNumber {
        self.reference.round
    }

    pub fn verify(&self, committee: &Committee) -> eyre::Result<()> {
        // Authors do not vote for their block (this is implicit in the protocol).
        ensure!(
            self.authority != self.reference.authority,
            "Authors do not vote for their block"
        );

        // Ensure the vote is valid.
        let public_key = committee
            .get_public_key(self.authority)
            .ok_or_else(|| eyre::eyre!("Unknown authority {}", self.authority))?;
        public_key.verify_digest(&self.reference.digest, &self.signature)?;

        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Certificate {
    /// The block that was certified.
    reference: BlockReference,
    /// The type of certificate.
    certificate_type: CertificateType,
    /// The signatures of the voters.
    signatures: Vec<(AuthorityIndex, SignatureBytes)>,
}

impl Debug for Certificate {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "C{}([{}], {})",
            self.reference.round, self.reference.authority, self.reference.digest
        )
    }
}

impl Certificate {
    pub fn new(reference: BlockReference, certificate_type: CertificateType) -> Self {
        Self {
            reference,
            certificate_type,
            signatures: Vec::new(),
        }
    }

    pub fn add(&mut self, vote: &Vote) {
        self.signatures.push((vote.authority, vote.signature));
    }

    fn has_quorum(&self, committee: &Committee) -> eyre::Result<()> {
        // The stake of the block creator.
        let Some(initial_stake) = committee.get_stake(self.reference.authority) else {
            bail!("Unknown author");
        };

        // The total stake of the signers.
        let total_stake = self
            .signatures
            .iter()
            .map(|(authority, _)| {
                let Some(stake) = committee.get_stake(*authority) else {
                    bail!("Unknown authority {authority}");
                };
                Ok(stake)
            })
            .sum::<Result<Stake, _>>()?;

        // Ensure that the total stake is a quorum.
        ensure!(
            committee.is_quorum(initial_stake + total_stake),
            "Certificate has no quorum threshold"
        );
        Ok(())
    }

    pub fn verify(&self, committee: &Committee) -> eyre::Result<()> {
        // Ensure that the certificate has a quorum.
        self.has_quorum(committee)?;

        // Verify the signatures.
        let signatures = self
            .signatures
            .iter()
            .map(|(a, s)| {
                let Some(public_key) = committee.get_public_key(*a) else {
                    bail!("Unknown core validator signer {a}")
                };
                Ok((public_key, s))
            })
            .collect::<Result<Vec<_>, _>>()?;

        for (public_key, signature) in signatures {
            if let Err(e) = public_key.verify_digest(&self.reference.digest, signature) {
                bail!("Certificate signature verification has failed: {e:?}");
            }
        }

        Ok(())
    }
}
