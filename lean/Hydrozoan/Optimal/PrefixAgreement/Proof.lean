import Hydrozoan.Optimal.PrefixAgreement.Statement
import Hydrozoan.Optimal.SlotAgreement.Proof
import Hydrozoan.PrefixAgreement.Proof

/-!
# Optimal-Hydrozoan: prefix agreement — proof

Generated proof layer; not part of the audit surface. A harvest of
`Optimal.SlotAgreement.decided_unique` — pointwise verdict agreement
below the horizon — followed by Hydrozoan's list plumbing
(`commitSeq_prefix`, `isPrefix_flatMap`), which is generic in the
verdict function and reused as is.
-/

namespace Hydrozoan

namespace Optimal

namespace PrefixAgreement

open Hydrozoan.PrefixAgreement (commitSeq ledger commitSeq_prefix isPrefix_flatMap)

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [O : OptimalFaults Replica] [S : Slots Replica]
  {U : OptUniverse Replica BlockId}

/-- Pointwise verdict agreement below a shared horizon. -/
theorem decidesBelow_eq {V₁ V₂ : View U.toBlockUniverse} {g₁ g₂ : ℕ → Option BlockId}
    {n : ℕ} (h₁ : DecidesBelow U V₁ g₁ n) (h₂ : DecidesBelow U V₂ g₂ n) :
    ∀ k < n, g₁ k = g₂ k := fun k hk =>
  SlotAgreement.decided_unique (h₁ k hk) V₂ (g₂ k) (h₂ k hk)

theorem seqAgreement : SeqAgreement U := by
  intro V₁ V₂ g₁ g₂ n h₁ h₂
  unfold commitSeq
  apply List.filterMap_congr
  intro k hk
  exact decidesBelow_eq h₁ h₂ k (List.mem_range.mp hk)

theorem prefixConsistency : PrefixConsistency U := by
  intro V₁ V₂ g₁ g₂ n₁ n₂ hle h₁ h₂
  have hagree : commitSeq g₁ n₁ = commitSeq g₂ n₁ :=
    seqAgreement V₁ V₂ g₁ g₂ n₁ h₁ (fun k hk => h₂ k (by omega))
  rw [hagree]
  exact commitSeq_prefix hle

theorem ledgerPrefixConsistency : LedgerPrefixConsistency U := by
  intro lin V₁ V₂ g₁ g₂ n₁ n₂ hle h₁ h₂
  exact isPrefix_flatMap lin (prefixConsistency V₁ V₂ g₁ g₂ n₁ n₂ hle h₁ h₂)

theorem holds : Statement := by
  intro Replica BlockId _ _ _ _ _ U
  exact ⟨seqAgreement, prefixConsistency, ledgerPrefixConsistency⟩

end PrefixAgreement

end Optimal

end Hydrozoan
