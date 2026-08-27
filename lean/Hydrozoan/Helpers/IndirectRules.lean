import Hydrozoan.Model.IndirectRules
import Hydrozoan.Helpers.History

/-!
# Indirect-rule instances and the history characterizations

Generated: decidability for `EligibleAsAnchor`, its arithmetic reading, and the
decidable characterizations of both rung tests through the computable
`history` surrogate — this is what lets witness models settle
`CertifiedIn` / `WeakLinked` (positively and negatively) by `decide`.
Nothing here is part of the audit surface.
-/

namespace Hydrozoan

section Eligibility

variable (Replica : Type*) [S : Slots Replica]

instance decidableEligibleAsAnchor (k j : ℕ) : Decidable (EligibleAsAnchor Replica k j) :=
  inferInstanceAs (Decidable (decisionRound Replica k < S.slotRound j))

/-- Eligibility in propose-round arithmetic: the anchor's round is at
least three past the candidate's. -/
theorem eligibleAsAnchor_iff {k j : ℕ} :
    EligibleAsAnchor Replica k j ↔ S.slotRound k + 3 ≤ S.slotRound j := by
  simp only [EligibleAsAnchor, decisionRound]
  omega

end Eligibility

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [F : Faults Replica]
  {U : BlockUniverse Replica BlockId}

/-- Rung 1 through the history surrogate: decidable on concrete data. -/
theorem certifiedIn_iff_history {A L : BlockId} {r : ℕ} (hA : A ∈ U.ids) :
    CertifiedIn U A L r ↔ (certificates U L r ∩ history U A).Nonempty := by
  constructor
  · rintro ⟨C, hC, hR⟩
    exact ⟨C, Finset.mem_inter.mpr ⟨hC, (mem_history_iff hA).mpr hR⟩⟩
  · rintro ⟨C, hC⟩
    obtain ⟨h1, h2⟩ := Finset.mem_inter.mp hC
    exact ⟨C, h1, (mem_history_iff hA).mp h2⟩

/-- Rung 2 through the history surrogate: the anchor-linked vote filter
is the canonical witness set, so the existential form collapses to a
decidable cardinality bound. -/
theorem weakLinked_iff_history {A L : BlockId} {r : ℕ} (hA : A ∈ U.ids) :
    WeakLinked U A L r ↔
      qWeak Replica ≤ (authorsOf U.block ((blocksAt U (r + 1)).filter
        fun b => IsVote U b L ∧ b ∈ history U A)).card := by
  constructor
  · rintro ⟨s, hs, hcard⟩
    refine le_trans hcard (Finset.card_le_card (Finset.image_subset_image ?_))
    intro b hb
    obtain ⟨h1, h2, h3⟩ := hs b hb
    exact Finset.mem_filter.mpr ⟨h1, h2, (mem_history_iff hA).mpr h3⟩
  · intro h
    refine ⟨(blocksAt U (r + 1)).filter fun b => IsVote U b L ∧ b ∈ history U A,
      fun b hb => ?_, h⟩
    obtain ⟨h1, h2, h3⟩ := Finset.mem_filter.mp hb
    exact ⟨h1, h2, (mem_history_iff hA).mp h3⟩

end Hydrozoan
