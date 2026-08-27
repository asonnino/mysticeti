import Hydrozoan.PrefixAgreement.Statement
import Hydrozoan.SlotAgreement.Proof

/-!
# Prefix agreement — proof

Generated proof layer; not part of the audit surface. Everything is a
harvest of `SlotAgreement.decided_unique`: pointwise verdict agreement
below the horizon, then list plumbing (`filterMap` congruence over
`range`, `range`/`filterMap` distribution over append, and prefix
preservation under `flatMap`).
-/

namespace Hydrozoan

namespace PrefixAgreement

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [LinearOrder BlockId] [F : Faults Replica]
  [S : Slots Replica] {U : BlockUniverse Replica BlockId}

/-- Pointwise verdict agreement below a shared horizon. -/
theorem decidesBelow_eq {V₁ V₂ : View U} {g₁ g₂ : ℕ → Option BlockId}
    {n : ℕ} (h₁ : DecidesBelow U V₁ g₁ n) (h₂ : DecidesBelow U V₂ g₂ n) :
    ∀ k < n, g₁ k = g₂ k := fun k hk =>
  SlotAgreement.decided_unique (h₁ k hk) V₂ (g₂ k) (h₂ k hk)

theorem seqAgreement : SeqAgreement U := by
  intro V₁ V₂ g₁ g₂ n h₁ h₂
  unfold commitSeq
  apply List.filterMap_congr
  intro k hk
  exact decidesBelow_eq h₁ h₂ k (List.mem_range.mp hk)

omit [DecidableEq BlockId] [LinearOrder BlockId] in
/-- A single replica's sequence grows monotonically with the horizon. -/
theorem commitSeq_prefix {g : ℕ → Option BlockId} {n₁ n₂ : ℕ}
    (h : n₁ ≤ n₂) : commitSeq g n₁ <+: commitSeq g n₂ := by
  unfold commitSeq
  obtain ⟨d, rfl⟩ := Nat.exists_eq_add_of_le h
  rw [List.range_add, List.filterMap_append]
  exact ⟨_, rfl⟩

theorem prefixConsistency : PrefixConsistency U := by
  intro V₁ V₂ g₁ g₂ n₁ n₂ hle h₁ h₂
  have hagree : commitSeq g₁ n₁ = commitSeq g₂ n₁ :=
    seqAgreement V₁ V₂ g₁ g₂ n₁ h₁ (fun k hk => h₂ k (by omega))
  rw [hagree]
  exact commitSeq_prefix hle

/-- Prefixes survive flattening. -/
theorem isPrefix_flatMap {α β : Type*} {l₁ l₂ : List α}
    (f : α → List β) (h : l₁ <+: l₂) : l₁.flatMap f <+: l₂.flatMap f := by
  obtain ⟨t, rfl⟩ := h
  rw [List.flatMap_append]
  exact ⟨_, rfl⟩

theorem ledgerPrefixConsistency : LedgerPrefixConsistency U := by
  intro lin V₁ V₂ g₁ g₂ n₁ n₂ hle h₁ h₂
  exact isPrefix_flatMap lin (prefixConsistency V₁ V₂ g₁ g₂ n₁ n₂ hle h₁ h₂)

theorem holds : Statement := by
  intro Replica BlockId _ _ _ _ _ _ U
  exact ⟨seqAgreement, prefixConsistency, ledgerPrefixConsistency⟩

end PrefixAgreement

end Hydrozoan
