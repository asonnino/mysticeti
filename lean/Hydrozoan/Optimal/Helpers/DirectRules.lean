import Hydrozoan.Optimal.Model.DirectRules
import Hydrozoan.Optimal.Helpers.Universe
import Hydrozoan.Helpers.DirectRules

/-!
# Optimal-Hydrozoan: direct-rule instances and bridges

Generated proof infrastructure over `Optimal/Model/DirectRules.lean`; not
part of the audit surface. `Decidable` instances so the witness models can
`decide` the rules, the filter characterizations of the existential
quorums (the canonical witness set is the filter), and the plain-case
reading of fast evidence when no equivocation is witnessed.

The quorum instances go through `decidable_of_iff` on the filter form on
purpose: an `unfold; infer_instance` on `∃ s : Finset BlockId, …` would
also succeed — enumerating every subset of `BlockId` — and `decide` would
never terminate.
-/

namespace Hydrozoan

namespace Optimal

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [O : OptimalFaults Replica]
  {U : BlockUniverse Replica BlockId}

/-- Hydrozoan's certificate, read through `votesFor`. -/
theorem isCertificate_iff_votesFor (C L : BlockId) :
    IsCertificate U C L ↔ qCert Replica ≤ (votesFor U C L).card :=
  Iff.rfl

instance decidableFastCommitOpt (L : BlockId) (r : ℕ) :
    Decidable (FastCommitOpt U L r) :=
  inferInstanceAs (Decidable (qFastOpt Replica ≤ (supporters U L (r + 1)).card))

instance decidableFastCommitOptInView (V : View U) (L : BlockId) (r : ℕ) :
    Decidable (FastCommitOptInView U V L r) :=
  inferInstanceAs (Decidable (qFastOpt Replica ≤ (supportersInView U V L (r + 1)).card))

section Slots

variable [S : Slots Replica]

/-- Fast evidence when no equivocation is witnessed: just the plain-case
threshold. -/
theorem isFastEvidence_iff_plain {k : ℕ} {C L : BlockId}
    (h : ¬ WitnessesEquivocation U k C) :
    IsFastEvidence U k C L ↔ tPlain Replica ≤ (votesFor U C L).card :=
  ⟨fun hE => hE.1 h, fun ht => ⟨fun _ => ht, fun hw => absurd hw h⟩⟩

variable [Fintype BlockId]

instance decidableIsFastEvidence (k : ℕ) (C L : BlockId) :
    Decidable (IsFastEvidence U k C L) := by
  unfold IsFastEvidence; infer_instance

instance decidableIsNoFastEvidence (k : ℕ) (C : BlockId) :
    Decidable (IsNoFastEvidence U k C) := by
  unfold IsNoFastEvidence; infer_instance

/-- The no-evidence quorum through its canonical witness set: the filter
of no-evidence decision-round blocks. -/
theorem noEvidenceQuorum_iff_filter {k : ℕ} :
    NoEvidenceQuorum U k ↔
      qCert Replica ≤ (authorsOf U.block ((blocksAt U (decisionRound Replica k)).filter
        fun b => IsNoFastEvidence U k b)).card := by
  constructor
  · rintro ⟨s, hs, hcard⟩
    refine le_trans hcard (Finset.card_le_card (Finset.image_subset_image ?_))
    intro b hb
    obtain ⟨h1, h2⟩ := hs b hb
    exact Finset.mem_filter.mpr ⟨h1, h2⟩
  · intro h
    refine ⟨(blocksAt U (decisionRound Replica k)).filter fun b => IsNoFastEvidence U k b,
      fun b hb => ?_, h⟩
    exact Finset.mem_filter.mp hb

/-- The in-view no-evidence quorum through its canonical witness set. -/
theorem noEvidenceQuorumInView_iff_filter {V : View U} {k : ℕ} :
    NoEvidenceQuorumInView U V k ↔
      qCert Replica ≤ (authorsOf U.block ((blocksAt U (decisionRound Replica k)).filter
        fun b => b ∈ V.ids ∧ IsNoFastEvidence U k b)).card := by
  constructor
  · rintro ⟨s, hs, hcard⟩
    refine le_trans hcard (Finset.card_le_card (Finset.image_subset_image ?_))
    intro b hb
    obtain ⟨h1, h2, h3⟩ := hs b hb
    exact Finset.mem_filter.mpr ⟨h1, h2, h3⟩
  · intro h
    refine ⟨(blocksAt U (decisionRound Replica k)).filter
      fun b => b ∈ V.ids ∧ IsNoFastEvidence U k b, fun b hb => ?_, h⟩
    obtain ⟨h1, h2, h3⟩ := Finset.mem_filter.mp hb
    exact ⟨h1, h2, h3⟩

instance decidableNoEvidenceQuorum (k : ℕ) : Decidable (NoEvidenceQuorum U k) :=
  decidable_of_iff _ noEvidenceQuorum_iff_filter.symm

instance decidableNoEvidenceQuorumInView (V : View U) (k : ℕ) :
    Decidable (NoEvidenceQuorumInView U V k) :=
  decidable_of_iff _ noEvidenceQuorumInView_iff_filter.symm

instance decidableSkippedLeaderOpt (k : ℕ) : Decidable (SkippedLeaderOpt U k) :=
  inferInstanceAs (Decidable (qCert Replica ≤ (blames U k).card ∧ NoEvidenceQuorum U k))

instance decidableSkippedLeaderOptInView (V : View U) (k : ℕ) :
    Decidable (SkippedLeaderOptInView U V k) :=
  inferInstanceAs
    (Decidable (qCert Replica ≤ (blamesInView U V k).card ∧ NoEvidenceQuorumInView U V k))

end Slots

end Optimal

end Hydrozoan
