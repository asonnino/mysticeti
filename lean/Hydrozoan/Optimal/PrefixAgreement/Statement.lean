import Hydrozoan.Optimal.Model.Decided
import Hydrozoan.PrefixAgreement.Statement

/-!
# Optimal-Hydrozoan: prefix agreement — statement

The output guarantee, the safety headline of the arc: replicas' committed
sequences are consistent. Hydrozoan's `PrefixAgreement` read over
`DecidedOpt`: the sequence shapes `commitSeq` (committed leaders in slot
order, skipped slots dropped — the paper's `ExtendCommitSeq`) and `ledger`
(a *memoryless per-leader* linearizer `BlockId → List BlockId` applied to
it) are reused as they are, being generic in the verdict function; only
`DecidesBelow` and the three claims are re-stated, over an `OptUniverse`
and views of its underlying universe.

The linearizer abstraction is narrower than the paper's
`LinearizeSubDags`, which is stateful: its persistent set `H` suppresses
blocks already output under an earlier leader, so what it emits for a
leader depends on the committed prefix before it. That procedure is
prefix-monotone and therefore also prefix-consistent, but the formal
ledger claim covers only linearizers with no such memory. (Inherited
from Hydrozoan's `PrefixAgreement`, which states the same claim.)

Three claims: equal horizons give equal sequences; different horizons
give a prefix (`<+:` is `List.IsPrefix`); and ledgers inherit prefix
consistency for every linearizer. `DecidesBelow` requires a derivation
for every slot below the horizon — an undecided slot has none — so the
claims speak exactly where replicas have produced output. Everything
here is a harvest of `Optimal.SlotAgreement`; no arithmetic row enters
directly. No `LinearOrder BlockId` (decision D3).
-/

namespace Hydrozoan

namespace Optimal

namespace PrefixAgreement

open Hydrozoan.PrefixAgreement (commitSeq ledger)

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [O : OptimalFaults Replica] [S : Slots Replica]

/-- `g` records a decided verdict for every slot below `n`, as judged
from `V`. -/
def DecidesBelow (U : OptUniverse Replica BlockId) (V : View U.toBlockUniverse)
    (g : ℕ → Option BlockId) (n : ℕ) : Prop :=
  ∀ k < n, DecidedOpt U V k (g k)

/-- **Sequence agreement**: at equal horizons, two replicas output the
same committed-leader sequence. -/
def SeqAgreement (U : OptUniverse Replica BlockId) : Prop :=
  ∀ (V₁ V₂ : View U.toBlockUniverse) (g₁ g₂ : ℕ → Option BlockId) (n : ℕ),
    DecidesBelow U V₁ g₁ n → DecidesBelow U V₂ g₂ n →
    commitSeq g₁ n = commitSeq g₂ n

/-- **Prefix consistency**: at different horizons, the shorter output
is a prefix of the longer. -/
def PrefixConsistency (U : OptUniverse Replica BlockId) : Prop :=
  ∀ (V₁ V₂ : View U.toBlockUniverse) (g₁ g₂ : ℕ → Option BlockId) (n₁ n₂ : ℕ),
    n₁ ≤ n₂ → DecidesBelow U V₁ g₁ n₁ → DecidesBelow U V₂ g₂ n₂ →
    commitSeq g₁ n₁ <+: commitSeq g₂ n₂

/-- **Ledger prefix consistency**, for every memoryless per-leader
linearizer (see the module docstring for the paper's stateful one). -/
def LedgerPrefixConsistency (U : OptUniverse Replica BlockId) : Prop :=
  ∀ (lin : BlockId → List BlockId) (V₁ V₂ : View U.toBlockUniverse)
    (g₁ g₂ : ℕ → Option BlockId) (n₁ n₂ : ℕ),
    n₁ ≤ n₂ → DecidesBelow U V₁ g₁ n₁ → DecidesBelow U V₂ g₂ n₂ →
    ledger lin g₁ n₁ <+: ledger lin g₂ n₂

/-- Output safety over every fault configuration, schedule, and universe
the Optimal model admits. -/
def Statement : Prop :=
  ∀ (Replica BlockId : Type) [Fintype Replica] [DecidableEq Replica]
    [DecidableEq BlockId] [OptimalFaults Replica] [Slots Replica]
    (U : OptUniverse Replica BlockId),
    SeqAgreement U ∧ PrefixConsistency U ∧ LedgerPrefixConsistency U

end PrefixAgreement

end Optimal

end Hydrozoan
