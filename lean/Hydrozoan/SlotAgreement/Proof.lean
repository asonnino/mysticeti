import Hydrozoan.SlotAgreement.Statement
import Hydrozoan.Helpers.SlotAgreement
import Hydrozoan.DirectSafety.Proof

/-!
# Slot agreement — proof

Generated proof layer; not part of the audit surface. Structural
induction on the first derivation (lean-dag's M6/O5 architecture): the
motive quantifies over the second view and verdict, so the induction
hypotheses cover any other replica's derivation. Direct-vs-direct
pairings close by `DirectSafety`; direct-vs-indirect by the rung
fires/starvation/skip lemmas of `Helpers/SlotAgreement.lean`;
indirect-vs-indirect by the anchor comparison followed by the shared
anchor's own premises. The weak-vs-weak diagonal closes by the two
canonicity premises and antisymmetry.
-/

namespace Hydrozoan

namespace SlotAgreement

open DirectSafety

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [LinearOrder BlockId] [F : Faults Replica]
  [S : Slots Replica] {U : BlockUniverse Replica BlockId}

theorem decided_unique {V₁ : View U} {k : ℕ} {v₁ : Option BlockId}
    (h₁ : Decided U V₁ k v₁) :
    ∀ (V₂ : View U) (v₂ : Option BlockId), Decided U V₂ k v₂ → v₁ = v₂ := by
  induction h₁ with
  | @directFast k L hL h =>
    intro V₂ v₂ h₂
    cases h₂ with
    | directFast hL₂ h₂' =>
        exact congrArg some (eq_of_fastCommit (by rw [hL.2.2, hL₂.2.2])
          (fastCommit_of_fastCommitInView h)
          (fastCommit_of_fastCommitInView h₂'))
    | directSlow hL₂ h₂' =>
        exact congrArg some
          (eq_of_fastCommit_of_slowCommit (by rw [hL.2.2, hL₂.2.2])
            (fastCommit_of_fastCommitInView h)
            (slowCommit_of_slowCommitInView h₂'))
    | directSkip h₂' =>
        exact absurd (skippedLeader_of_skippedLeaderInView h₂')
          (not_skippedLeader_of_fastCommit hL (fastCommit_of_fastCommitInView h))
    | indirectCert hkj₂ helig₂ hj₂ hmid₂ hL₂ hcert₂ =>
        refine congrArg some (Classical.byContradiction fun hne => ?_)
        exact absurd hcert₂ (not_certifiedIn_of_fastCommit
          (fun hh => hne hh.symm) (by rw [hL₂.2.2, hL.2.2])
          (fastCommit_of_fastCommitInView h))
    | indirectWeak hkj₂ helig₂ hj₂ hmid₂ hnocert₂ hL₂ hweak₂ hmin₂ =>
        refine congrArg some (Classical.byContradiction fun hne => ?_)
        exact absurd hweak₂ (not_weakLinked_of_fastCommit
          (fun hh => hne hh.symm) (by rw [hL₂.2.2, hL.2.2])
          (fastCommit_of_fastCommitInView h))
    | indirectSkip hkj₂ helig₂ hj₂ hmid₂ hnocert₂ hnoweak₂ =>
        exact absurd (weakLinked_of_fastCommitInView_at_anchor h hj₂ helig₂)
          (hnoweak₂ _ hL)
  | @directSlow k L hL h =>
    intro V₂ v₂ h₂
    cases h₂ with
    | directFast hL₂ h₂' =>
        exact congrArg some
          (eq_of_fastCommit_of_slowCommit (by rw [hL₂.2.2, hL.2.2])
            (fastCommit_of_fastCommitInView h₂')
            (slowCommit_of_slowCommitInView h)).symm
    | directSlow hL₂ h₂' =>
        exact congrArg some
          (eq_of_certificates_nonempty (by rw [hL.2.2, hL₂.2.2])
            (certificates_nonempty_of_slowCommit
              (slowCommit_of_slowCommitInView h))
            (certificates_nonempty_of_slowCommit
              (slowCommit_of_slowCommitInView h₂')))
    | directSkip h₂' =>
        exact absurd (skippedLeader_of_skippedLeaderInView h₂')
          (not_skippedLeader_of_slowCommit hL (slowCommit_of_slowCommitInView h))
    | indirectCert hkj₂ helig₂ hj₂ hmid₂ hL₂ hcert₂ =>
        exact congrArg some
          (eq_of_certificates_nonempty (by rw [hL.2.2, hL₂.2.2])
            (certificates_nonempty_of_slowCommit
              (slowCommit_of_slowCommitInView h))
            (certificates_nonempty_of_certifiedIn hcert₂))
    | indirectWeak hkj₂ helig₂ hj₂ hmid₂ hnocert₂ hL₂ hweak₂ hmin₂ =>
        exact absurd (certifiedIn_of_slowCommitInView_at_anchor h hj₂ helig₂)
          (hnocert₂ _ hL)
    | indirectSkip hkj₂ helig₂ hj₂ hmid₂ hnocert₂ hnoweak₂ =>
        exact absurd (certifiedIn_of_slowCommitInView_at_anchor h hj₂ helig₂)
          (hnocert₂ _ hL)
  | @directSkip k h =>
    intro V₂ v₂ h₂
    cases h₂ with
    | directFast hL₂ h₂' =>
        exact absurd (skippedLeader_of_skippedLeaderInView h)
          (not_skippedLeader_of_fastCommit hL₂
            (fastCommit_of_fastCommitInView h₂'))
    | directSlow hL₂ h₂' =>
        exact absurd (skippedLeader_of_skippedLeaderInView h)
          (not_skippedLeader_of_slowCommit hL₂
            (slowCommit_of_slowCommitInView h₂'))
    | directSkip _ => rfl
    | indirectCert hkj₂ helig₂ hj₂ hmid₂ hL₂ hcert₂ =>
        exact absurd hcert₂ (not_certifiedIn_of_skipped hL₂
          (skippedLeader_of_skippedLeaderInView h))
    | indirectWeak hkj₂ helig₂ hj₂ hmid₂ hnocert₂ hL₂ hweak₂ hmin₂ =>
        exact absurd hweak₂ (not_weakLinked_of_skipped hL₂
          (skippedLeader_of_skippedLeaderInView h))
    | indirectSkip _ _ _ _ _ _ => rfl
  | @indirectCert k j A L hkj helig hj hmid hL hcert ihj ihmid =>
    intro V₂ v₂ h₂
    cases h₂ with
    | directFast hL₂ h₂' =>
        refine congrArg some (Classical.byContradiction fun hne => ?_)
        exact absurd hcert (not_certifiedIn_of_fastCommit hne
          (by rw [hL.2.2, hL₂.2.2]) (fastCommit_of_fastCommitInView h₂'))
    | directSlow hL₂ h₂' =>
        exact congrArg some
          (eq_of_certificates_nonempty (by rw [hL.2.2, hL₂.2.2])
            (certificates_nonempty_of_certifiedIn hcert)
            (certificates_nonempty_of_slowCommit
              (slowCommit_of_slowCommitInView h₂')))
    | directSkip h₂' =>
        exact absurd hcert (not_certifiedIn_of_skipped hL
          (skippedLeader_of_skippedLeaderInView h₂'))
    | indirectCert hkj₂ helig₂ hj₂ hmid₂ hL₂ hcert₂ =>
        exact congrArg some
          (eq_of_certificates_nonempty (by rw [hL.2.2, hL₂.2.2])
            (certificates_nonempty_of_certifiedIn hcert)
            (certificates_nonempty_of_certifiedIn hcert₂))
    | indirectWeak hkj₂ helig₂ hj₂ hmid₂ hnocert₂ hL₂ hweak₂ hmin₂ =>
        obtain ⟨rfl, rfl⟩ :=
          anchor_eq (Dec := fun (V : View U) n v => Decided U V n v)
            (Elig := EligibleAsAnchor Replica k)
            hkj helig hkj₂ helig₂ hj₂ hmid₂ ihj ihmid
        exact absurd hcert (hnocert₂ _ hL)
    | indirectSkip hkj₂ helig₂ hj₂ hmid₂ hnocert₂ hnoweak₂ =>
        obtain ⟨rfl, rfl⟩ :=
          anchor_eq (Dec := fun (V : View U) n v => Decided U V n v)
            (Elig := EligibleAsAnchor Replica k)
            hkj helig hkj₂ helig₂ hj₂ hmid₂ ihj ihmid
        exact absurd hcert (hnocert₂ _ hL)
  | @indirectWeak k j A L hkj helig hj hmid hnocert hL hweak hmin ihj ihmid =>
    intro V₂ v₂ h₂
    cases h₂ with
    | directFast hL₂ h₂' =>
        refine congrArg some (Classical.byContradiction fun hne => ?_)
        exact absurd hweak (not_weakLinked_of_fastCommit hne
          (by rw [hL.2.2, hL₂.2.2]) (fastCommit_of_fastCommitInView h₂'))
    | directSlow hL₂ h₂' =>
        exact absurd (certifiedIn_of_slowCommitInView_at_anchor h₂' hj helig)
          (hnocert _ hL₂)
    | directSkip h₂' =>
        exact absurd hweak (not_weakLinked_of_skipped hL
          (skippedLeader_of_skippedLeaderInView h₂'))
    | indirectCert hkj₂ helig₂ hj₂ hmid₂ hL₂ hcert₂ =>
        obtain ⟨rfl, rfl⟩ :=
          anchor_eq (Dec := fun (V : View U) n v => Decided U V n v)
            (Elig := EligibleAsAnchor Replica k)
            hkj helig hkj₂ helig₂ hj₂ hmid₂ ihj ihmid
        exact absurd hcert₂ (hnocert _ hL₂)
    | @indirectWeak _ j₂ A₂ L₂ hkj₂ helig₂ hj₂ hmid₂ hnocert₂ hL₂ hweak₂ hmin₂ =>
        obtain ⟨rfl, rfl⟩ :=
          anchor_eq (Dec := fun (V : View U) n v => Decided U V n v)
            (Elig := EligibleAsAnchor Replica k)
            hkj helig hkj₂ helig₂ hj₂ hmid₂ ihj ihmid
        exact congrArg some (le_antisymm
          (not_lt.mp (hmin L₂ hL₂ hweak₂)) (not_lt.mp (hmin₂ L hL hweak)))
    | indirectSkip hkj₂ helig₂ hj₂ hmid₂ hnocert₂ hnoweak₂ =>
        obtain ⟨rfl, rfl⟩ :=
          anchor_eq (Dec := fun (V : View U) n v => Decided U V n v)
            (Elig := EligibleAsAnchor Replica k)
            hkj helig hkj₂ helig₂ hj₂ hmid₂ ihj ihmid
        exact absurd hweak (hnoweak₂ _ hL)
  | @indirectSkip k j A hkj helig hj hmid hnocert hnoweak ihj ihmid =>
    intro V₂ v₂ h₂
    cases h₂ with
    | directFast hL₂ h₂' =>
        exact absurd (weakLinked_of_fastCommitInView_at_anchor h₂' hj helig)
          (hnoweak _ hL₂)
    | directSlow hL₂ h₂' =>
        exact absurd (certifiedIn_of_slowCommitInView_at_anchor h₂' hj helig)
          (hnocert _ hL₂)
    | directSkip _ => rfl
    | indirectCert hkj₂ helig₂ hj₂ hmid₂ hL₂ hcert₂ =>
        obtain ⟨rfl, rfl⟩ :=
          anchor_eq (Dec := fun (V : View U) n v => Decided U V n v)
            (Elig := EligibleAsAnchor Replica k)
            hkj helig hkj₂ helig₂ hj₂ hmid₂ ihj ihmid
        exact absurd hcert₂ (hnocert _ hL₂)
    | indirectWeak hkj₂ helig₂ hj₂ hmid₂ hnocert₂ hL₂ hweak₂ hmin₂ =>
        obtain ⟨rfl, rfl⟩ :=
          anchor_eq (Dec := fun (V : View U) n v => Decided U V n v)
            (Elig := EligibleAsAnchor Replica k)
            hkj helig hkj₂ helig₂ hj₂ hmid₂ ihj ihmid
        exact absurd hweak₂ (hnoweak _ hL₂)
    | indirectSkip _ _ _ _ _ _ => rfl

theorem decidedUnique : DecidedUnique U := fun _ V₂ _ _ v₂ h₁ h₂ =>
  decided_unique h₁ V₂ v₂ h₂

theorem holds : Statement := by
  intro Replica BlockId _ _ _ _ _ _ U
  exact decidedUnique

end SlotAgreement

end Hydrozoan
