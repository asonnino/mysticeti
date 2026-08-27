import Hydrozoan.Optimal.SlotAgreement.Statement
import Hydrozoan.Optimal.Helpers.SlotAgreement

/-!
# Optimal-Hydrozoan: slot agreement — proof

Generated proof layer; not part of the audit surface. Hydrozoan's
structural induction on the first derivation, constructor by
constructor: direct-vs-direct pairings close by the direct-safety cores;
direct-vs-indirect by the "rung fires" / starvation / skip lemmas of
`Optimal/Helpers/SlotAgreement.lean`; indirect-vs-indirect by the anchor
comparison followed by the shared anchor's own premises. The
evidence/evidence diagonal closes by `evidenceLinked_unique` — no
tie-break premise is needed.
-/

namespace Hydrozoan

namespace Optimal

namespace SlotAgreement

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [O : OptimalFaults Replica] [S : Slots Replica]
  {U : OptUniverse Replica BlockId}

theorem decided_unique {V₁ : View U.toBlockUniverse} {k : ℕ} {v₁ : Option BlockId}
    (h₁ : DecidedOpt U V₁ k v₁) :
    ∀ (V₂ : View U.toBlockUniverse) (v₂ : Option BlockId),
      DecidedOpt U V₂ k v₂ → v₁ = v₂ := by
  induction h₁ with
  | @directFast k L hL h =>
    intro V₂ v₂ h₂
    cases h₂ with
    | directFast hL₂ h₂' =>
        exact congrArg some (eq_of_fastCommitOpt_leader hL hL₂
          (fastCommitOpt_of_fastCommitOptInView h)
          (fastCommitOpt_of_fastCommitOptInView h₂'))
    | directSlow hL₂ h₂' =>
        exact congrArg some
          (DirectSafety.eq_of_fastCommitOpt_of_slowCommit (by rw [hL.2.2, hL₂.2.2])
            (fastCommitOpt_of_fastCommitOptInView h)
            (slowCommit_of_slowCommitInView h₂'))
    | directSkip h₂' =>
        exact absurd (qCert_le_blames_of_skippedLeaderOptInView h₂')
          (Nat.not_le.mpr (DirectSafety.blames_lt_of_fastCommitOpt hL
            (fastCommitOpt_of_fastCommitOptInView h)))
    | indirectCert hkj₂ helig₂ hj₂ hmid₂ hL₂ hcert₂ =>
        refine congrArg some (Classical.byContradiction fun hne => ?_)
        exact absurd hcert₂ (not_certifiedIn_of_fastCommitOpt
          (fun hh => hne hh.symm) hL hL₂ (fastCommitOpt_of_fastCommitOptInView h))
    | indirectEvidence hkj₂ helig₂ hj₂ hmid₂ hnocert₂ hL₂ hev₂ =>
        refine congrArg some (Classical.byContradiction fun hne => ?_)
        exact absurd hev₂ (not_evidenceLinked_of_fastCommitOpt
          (fun hh => hne hh.symm) hL hL₂ (fastCommitOpt_of_fastCommitOptInView h))
    | indirectSkip hkj₂ helig₂ hj₂ hmid₂ hnocert₂ hnoev₂ =>
        exact absurd (evidenceLinked_of_fastCommitOptInView_at_anchor hL h hj₂ helig₂)
          (hnoev₂ _ hL)
  | @directSlow k L hL h =>
    intro V₂ v₂ h₂
    cases h₂ with
    | directFast hL₂ h₂' =>
        exact congrArg some
          (DirectSafety.eq_of_fastCommitOpt_of_slowCommit (by rw [hL₂.2.2, hL.2.2])
            (fastCommitOpt_of_fastCommitOptInView h₂')
            (slowCommit_of_slowCommitInView h)).symm
    | directSlow hL₂ h₂' =>
        exact congrArg some
          (Hydrozoan.DirectSafety.eq_of_certificates_nonempty (by rw [hL.2.2, hL₂.2.2])
            (certificates_nonempty_of_slowCommit (slowCommit_of_slowCommitInView h))
            (certificates_nonempty_of_slowCommit (slowCommit_of_slowCommitInView h₂')))
    | directSkip h₂' =>
        exact absurd (qCert_le_blames_of_skippedLeaderOptInView h₂')
          (Nat.not_le.mpr (DirectSafety.blames_lt_of_slowCommit hL
            (slowCommit_of_slowCommitInView h)))
    | indirectCert hkj₂ helig₂ hj₂ hmid₂ hL₂ hcert₂ =>
        exact congrArg some
          (Hydrozoan.DirectSafety.eq_of_certificates_nonempty (by rw [hL.2.2, hL₂.2.2])
            (certificates_nonempty_of_slowCommit (slowCommit_of_slowCommitInView h))
            (certificates_nonempty_of_certifiedIn hcert₂))
    | indirectEvidence hkj₂ helig₂ hj₂ hmid₂ hnocert₂ hL₂ hev₂ =>
        exact absurd (certifiedIn_of_slowCommitInView_at_anchor_opt h hj₂ helig₂)
          (hnocert₂ _ hL)
    | indirectSkip hkj₂ helig₂ hj₂ hmid₂ hnocert₂ hnoev₂ =>
        exact absurd (certifiedIn_of_slowCommitInView_at_anchor_opt h hj₂ helig₂)
          (hnocert₂ _ hL)
  | @directSkip k h =>
    intro V₂ v₂ h₂
    cases h₂ with
    | directFast hL₂ h₂' =>
        exact absurd (qCert_le_blames_of_skippedLeaderOptInView h)
          (Nat.not_le.mpr (DirectSafety.blames_lt_of_fastCommitOpt hL₂
            (fastCommitOpt_of_fastCommitOptInView h₂')))
    | directSlow hL₂ h₂' =>
        exact absurd (qCert_le_blames_of_skippedLeaderOptInView h)
          (Nat.not_le.mpr (DirectSafety.blames_lt_of_slowCommit hL₂
            (slowCommit_of_slowCommitInView h₂')))
    | directSkip _ => rfl
    | indirectCert hkj₂ helig₂ hj₂ hmid₂ hL₂ hcert₂ =>
        exact absurd hcert₂ (not_certifiedIn_of_skippedOpt hL₂
          (skippedLeaderOpt_of_skippedLeaderOptInView h))
    | indirectEvidence hkj₂ helig₂ hj₂ hmid₂ hnocert₂ hL₂ hev₂ =>
        exact absurd hev₂ (not_evidenceLinked_of_skippedOpt hL₂
          (skippedLeaderOpt_of_skippedLeaderOptInView h))
    | indirectSkip _ _ _ _ _ _ => rfl
  | @indirectCert k j A L hkj helig hj hmid hL hcert ihj ihmid =>
    intro V₂ v₂ h₂
    cases h₂ with
    | directFast hL₂ h₂' =>
        refine congrArg some (Classical.byContradiction fun hne => ?_)
        exact absurd hcert (not_certifiedIn_of_fastCommitOpt hne hL₂ hL
          (fastCommitOpt_of_fastCommitOptInView h₂'))
    | directSlow hL₂ h₂' =>
        exact congrArg some
          (Hydrozoan.DirectSafety.eq_of_certificates_nonempty (by rw [hL.2.2, hL₂.2.2])
            (certificates_nonempty_of_certifiedIn hcert)
            (certificates_nonempty_of_slowCommit (slowCommit_of_slowCommitInView h₂')))
    | directSkip h₂' =>
        exact absurd hcert (not_certifiedIn_of_skippedOpt hL
          (skippedLeaderOpt_of_skippedLeaderOptInView h₂'))
    | indirectCert hkj₂ helig₂ hj₂ hmid₂ hL₂ hcert₂ =>
        exact congrArg some
          (Hydrozoan.DirectSafety.eq_of_certificates_nonempty (by rw [hL.2.2, hL₂.2.2])
            (certificates_nonempty_of_certifiedIn hcert)
            (certificates_nonempty_of_certifiedIn hcert₂))
    | indirectEvidence hkj₂ helig₂ hj₂ hmid₂ hnocert₂ hL₂ hev₂ =>
        obtain ⟨rfl, rfl⟩ :=
          anchor_eq (Dec := fun (V : View U.toBlockUniverse) n v => DecidedOpt U V n v)
            (Elig := EligibleAsAnchor Replica k)
            hkj helig hkj₂ helig₂ hj₂ hmid₂ ihj ihmid
        exact absurd hcert (hnocert₂ _ hL)
    | indirectSkip hkj₂ helig₂ hj₂ hmid₂ hnocert₂ hnoev₂ =>
        obtain ⟨rfl, rfl⟩ :=
          anchor_eq (Dec := fun (V : View U.toBlockUniverse) n v => DecidedOpt U V n v)
            (Elig := EligibleAsAnchor Replica k)
            hkj helig hkj₂ helig₂ hj₂ hmid₂ ihj ihmid
        exact absurd hcert (hnocert₂ _ hL)
  | @indirectEvidence k j A L hkj helig hj hmid hnocert hL hev ihj ihmid =>
    intro V₂ v₂ h₂
    cases h₂ with
    | directFast hL₂ h₂' =>
        refine congrArg some (Classical.byContradiction fun hne => ?_)
        exact absurd hev (not_evidenceLinked_of_fastCommitOpt hne hL₂ hL
          (fastCommitOpt_of_fastCommitOptInView h₂'))
    | directSlow hL₂ h₂' =>
        exact absurd (certifiedIn_of_slowCommitInView_at_anchor_opt h₂' hj helig)
          (hnocert _ hL₂)
    | directSkip h₂' =>
        exact absurd hev (not_evidenceLinked_of_skippedOpt hL
          (skippedLeaderOpt_of_skippedLeaderOptInView h₂'))
    | indirectCert hkj₂ helig₂ hj₂ hmid₂ hL₂ hcert₂ =>
        obtain ⟨rfl, rfl⟩ :=
          anchor_eq (Dec := fun (V : View U.toBlockUniverse) n v => DecidedOpt U V n v)
            (Elig := EligibleAsAnchor Replica k)
            hkj helig hkj₂ helig₂ hj₂ hmid₂ ihj ihmid
        exact absurd hcert₂ (hnocert _ hL₂)
    | @indirectEvidence _ j₂ A₂ L₂ hkj₂ helig₂ hj₂ hmid₂ hnocert₂ hL₂ hev₂ =>
        obtain ⟨rfl, rfl⟩ :=
          anchor_eq (Dec := fun (V : View U.toBlockUniverse) n v => DecidedOpt U V n v)
            (Elig := EligibleAsAnchor Replica k)
            hkj helig hkj₂ helig₂ hj₂ hmid₂ ihj ihmid
        exact congrArg some (evidenceLinked_unique hL hL₂ hev hev₂)
    | indirectSkip hkj₂ helig₂ hj₂ hmid₂ hnocert₂ hnoev₂ =>
        obtain ⟨rfl, rfl⟩ :=
          anchor_eq (Dec := fun (V : View U.toBlockUniverse) n v => DecidedOpt U V n v)
            (Elig := EligibleAsAnchor Replica k)
            hkj helig hkj₂ helig₂ hj₂ hmid₂ ihj ihmid
        exact absurd hev (hnoev₂ _ hL)
  | @indirectSkip k j A hkj helig hj hmid hnocert hnoev ihj ihmid =>
    intro V₂ v₂ h₂
    cases h₂ with
    | directFast hL₂ h₂' =>
        exact absurd (evidenceLinked_of_fastCommitOptInView_at_anchor hL₂ h₂' hj helig)
          (hnoev _ hL₂)
    | directSlow hL₂ h₂' =>
        exact absurd (certifiedIn_of_slowCommitInView_at_anchor_opt h₂' hj helig)
          (hnocert _ hL₂)
    | directSkip _ => rfl
    | indirectCert hkj₂ helig₂ hj₂ hmid₂ hL₂ hcert₂ =>
        obtain ⟨rfl, rfl⟩ :=
          anchor_eq (Dec := fun (V : View U.toBlockUniverse) n v => DecidedOpt U V n v)
            (Elig := EligibleAsAnchor Replica k)
            hkj helig hkj₂ helig₂ hj₂ hmid₂ ihj ihmid
        exact absurd hcert₂ (hnocert _ hL₂)
    | indirectEvidence hkj₂ helig₂ hj₂ hmid₂ hnocert₂ hL₂ hev₂ =>
        obtain ⟨rfl, rfl⟩ :=
          anchor_eq (Dec := fun (V : View U.toBlockUniverse) n v => DecidedOpt U V n v)
            (Elig := EligibleAsAnchor Replica k)
            hkj helig hkj₂ helig₂ hj₂ hmid₂ ihj ihmid
        exact absurd hev₂ (hnoev _ hL₂)
    | indirectSkip _ _ _ _ _ _ => rfl

theorem decidedUnique : DecidedUnique U := fun _ V₂ _ _ v₂ h₁ h₂ =>
  decided_unique h₁ V₂ v₂ h₂

theorem holds : Statement := by
  intro Replica BlockId _ _ _ _ _ U
  exact decidedUnique

end SlotAgreement

end Optimal

end Hydrozoan
