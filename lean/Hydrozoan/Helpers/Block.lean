import Hydrozoan.Model.BlockUniverse

/-!
# Block lemmas and decidability

Generated infrastructure over `Model/Block.lean` and
`Model/BlockUniverse.lean`. Nothing here is part of the audit surface.
-/

namespace Hydrozoan

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [F : Faults Replica]

/-- `ValidWrt` is decidable on concrete data: lets hand-built witness
DAGs be checked by `decide`. Not needed by any proof — a wrong instance
could not smuggle anything in anyway, since it still has to
kernel-check. -/
instance [DecidableEq BlockId] (blk : BlockId → Block Replica BlockId)
    (b : Block Replica BlockId) : Decidable (ValidWrt blk b) :=
  decidable_of_iff
    ((∀ i ∈ b.parents, (blk i).round + 1 = b.round) ∧
      (∀ i ∈ b.parents, ∀ j ∈ b.parents,
        (blk i).author = (blk j).author → i = j) ∧
      (0 < b.round → q Replica ≤ (authors blk b).card))
    ⟨fun h => ⟨h.1, h.2.1, h.2.2⟩,
      fun h => ⟨h.predecessor, h.distinct_authors, h.quorum⟩⟩

/-- Genesis blocks reference nothing: at round `0` the (additive)
predecessor condition is unsatisfiable — the derivation promised in the
`ValidWrt` docstring. -/
theorem ValidWrt.parents_empty_of_round_zero
    {blk : BlockId → Block Replica BlockId} {b : Block Replica BlockId}
    (hv : ValidWrt blk b) (h0 : b.round = 0) : b.parents = ∅ := by
  by_contra h
  obtain ⟨i, hi⟩ := Finset.nonempty_iff_ne_empty.mpr h
  have := hv.predecessor i hi
  omega

end Hydrozoan
