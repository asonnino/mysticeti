import Hydrozoan.Model.Decided

/-!
# Prefix agreement — statement

The output guarantee: replicas' committed sequences are consistent.
`commitSeq` is the shape of the paper's `ExtendCommitSeq` result — the
committed leaders in slot order, skipped slots dropped — and `ledger`
applies a linearizer to every committed leader, the paper's
`LinearizeSubDags` **abstracted to an arbitrary function**: determinism
is the only property the results use, so the claims hold for any
concrete traversal.

Three claims: equal horizons give equal sequences; different horizons
give a prefix (`<+:` is `List.IsPrefix`); and ledgers inherit prefix
consistency for every linearizer. `DecidesBelow` requires a derivation
for every slot below the horizon — an undecided slot has none — so the
claims speak exactly where replicas have produced output, matching the
no-conflicting-decision reading of slot agreement.
-/

namespace Hydrozoan

namespace PrefixAgreement

section Sequences

variable {BlockId : Type*}

/-- The committed leaders below slot `n`, in slot order, skips
dropped — the output shape of the paper's `ExtendCommitSeq`. -/
def commitSeq (g : ℕ → Option BlockId) (n : ℕ) : List BlockId :=
  (List.range n).filterMap g

/-- A ledger: every committed leader flattened by a linearizer — the
paper's `LinearizeSubDags`, abstracted to an arbitrary function. -/
def ledger (lin : BlockId → List BlockId) (g : ℕ → Option BlockId)
    (n : ℕ) : List BlockId :=
  (commitSeq g n).flatMap lin

end Sequences

section Claims

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [LinearOrder BlockId] [F : Faults Replica]
  [S : Slots Replica]

/-- `g` records a decided verdict for every slot below `n`, as judged
from `V`. -/
def DecidesBelow (U : BlockUniverse Replica BlockId) (V : View U)
    (g : ℕ → Option BlockId) (n : ℕ) : Prop :=
  ∀ k < n, Decided U V k (g k)

/-- **Sequence agreement**: at equal horizons, two replicas output the
same committed-leader sequence. -/
def SeqAgreement (U : BlockUniverse Replica BlockId) : Prop :=
  ∀ (V₁ V₂ : View U) (g₁ g₂ : ℕ → Option BlockId) (n : ℕ),
    DecidesBelow U V₁ g₁ n → DecidesBelow U V₂ g₂ n →
    commitSeq g₁ n = commitSeq g₂ n

/-- **Prefix consistency**: at different horizons, the shorter output
is a prefix of the longer. -/
def PrefixConsistency (U : BlockUniverse Replica BlockId) : Prop :=
  ∀ (V₁ V₂ : View U) (g₁ g₂ : ℕ → Option BlockId) (n₁ n₂ : ℕ),
    n₁ ≤ n₂ → DecidesBelow U V₁ g₁ n₁ → DecidesBelow U V₂ g₂ n₂ →
    commitSeq g₁ n₁ <+: commitSeq g₂ n₂

/-- **Ledger prefix consistency**, for any linearizer whatsoever. -/
def LedgerPrefixConsistency (U : BlockUniverse Replica BlockId) : Prop :=
  ∀ (lin : BlockId → List BlockId) (V₁ V₂ : View U)
    (g₁ g₂ : ℕ → Option BlockId) (n₁ n₂ : ℕ),
    n₁ ≤ n₂ → DecidesBelow U V₁ g₁ n₁ → DecidesBelow U V₂ g₂ n₂ →
    ledger lin g₁ n₁ <+: ledger lin g₂ n₂

/-- Output safety over every fault configuration, schedule, tie-break
order, and block universe the model admits. -/
def Statement : Prop :=
  ∀ (Replica BlockId : Type) [Fintype Replica] [DecidableEq Replica]
    [DecidableEq BlockId] [LinearOrder BlockId] [Faults Replica]
    [Slots Replica] (U : BlockUniverse Replica BlockId),
    SeqAgreement U ∧ PrefixConsistency U ∧ LedgerPrefixConsistency U

end Claims

end PrefixAgreement

end Hydrozoan
