import Hydrozoan.Optimal.Model.IndirectRules
import Hydrozoan.Optimal.Helpers.DirectRules
import Hydrozoan.Helpers.IndirectRules

/-!
# Optimal-Hydrozoan: the evidence rung through the history surrogate

Generated proof infrastructure over `Optimal/Model/IndirectRules.lean`;
not part of the audit surface. `evidenceLinked_iff_history` is the mirror
of `weakLinked_iff_history`: the anchor-linked evidence filter is the
canonical witness set, so the existential form collapses to a decidable
cardinality bound on concrete data. No `Decidable (EvidenceLinked …)`
instance — the bridge needs `A ∈ U.ids`, so witnesses go through the iff.
-/

namespace Hydrozoan

namespace Optimal

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [Fintype BlockId] [O : OptimalFaults Replica]
  [S : Slots Replica] {U : BlockUniverse Replica BlockId}

/-- Rung 2 through the history surrogate. -/
theorem evidenceLinked_iff_history {A L : BlockId} {k : ℕ} (hA : A ∈ U.ids) :
    EvidenceLinked U A L k ↔
      qCert Replica ≤ (authorsOf U.block ((blocksAt U (decisionRound Replica k)).filter
        fun b => IsFastEvidence U k b L ∧ b ∈ history U A)).card := by
  constructor
  · rintro ⟨s, hs, hcard⟩
    refine le_trans hcard (Finset.card_le_card (Finset.image_subset_image ?_))
    intro b hb
    obtain ⟨h1, h2, h3⟩ := hs b hb
    exact Finset.mem_filter.mpr ⟨h1, h2, (mem_history_iff hA).mpr h3⟩
  · intro h
    refine ⟨(blocksAt U (decisionRound Replica k)).filter
      fun b => IsFastEvidence U k b L ∧ b ∈ history U A, fun b hb => ?_, h⟩
    obtain ⟨h1, h2, h3⟩ := Finset.mem_filter.mp hb
    exact ⟨h1, h2, (mem_history_iff hA).mp h3⟩

end Optimal

end Hydrozoan
