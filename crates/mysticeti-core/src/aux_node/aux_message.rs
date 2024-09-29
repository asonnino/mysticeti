// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt, fmt::Debug};

use eyre::{bail, ensure};
use serde::{Deserialize, Serialize};

use crate::{
    committee::Committee,
    crypto::{SignatureBytes, Signer},
    types::{AuthorityIndex, BlockDigest, BlockReference, PublicKey, RoundNumber, Stake},
};

#[derive(Clone, Serialize, Deserialize)]
pub struct PartialAuxiliaryCertificate {
    pub block_reference: BlockReference,
    pub signatures: Vec<(AuthorityIndex, SignatureBytes)>,
}

impl Debug for PartialAuxiliaryCertificate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "C{}", self.block_reference)
    }
}

impl PartialAuxiliaryCertificate {
    pub fn from_block_reference(
        block_reference: BlockReference,
        authority: AuthorityIndex,
        signer: &Signer,
    ) -> Self {
        let signature = signer.counter_sign_block(&block_reference);
        Self {
            block_reference,
            signatures: vec![(authority, signature)],
        }
    }

    pub fn author(&self) -> AuthorityIndex {
        self.block_reference.authority
    }

    pub fn round(&self) -> RoundNumber {
        self.block_reference.round
    }

    pub fn block_digest(&self) -> BlockDigest {
        self.block_reference.digest
    }

    pub fn verify(&self, core_committee: &Committee) -> eyre::Result<()> {
        let authority_signatures = self
            .signatures
            .iter()
            .map(|(authority, signature)| {
                let Some(public_key) = core_committee.get_public_key(*authority) else {
                    bail!("Unknown core validator signer {authority}")
                };
                Ok((public_key, signature))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if let Err(e) = PublicKey::verify_certificate(self, &authority_signatures) {
            bail!("Certificate signature verification has failed: {e:?}");
        }
        Ok(())
    }

    pub fn is_full_certificate(&self, committee: &Committee) -> eyre::Result<()> {
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
        ensure!(
            committee.is_quorum(total_stake),
            "Certificate has no quorum threshold"
        );
        Ok(())
    }

    pub fn merge(mut certificates: Vec<Self>) -> eyre::Result<Self> {
        let Some(mut certificate) = certificates.pop() else {
            bail!("Empty list of certificates");
        };
        for c in certificates {
            ensure!(
                c.block_reference == certificate.block_reference,
                "Cannot merge certificates over different blocks"
            );
            certificate.signatures.extend(c.signatures);
        }

        Ok(certificate)
    }
}
