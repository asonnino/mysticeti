import Hydrozoan.Model.Decided

/-!
# Slot agreement — statement

The headline safety claim for a single slot: **any two verdicts agree** —
across views, across routes. Whether a replica commits via the fast
path, the slow path, the certificate rung, or the weak rung, and whether
another skips directly or indirectly, two derivable verdicts for one
slot are equal. Undecided replicas assert nothing, so this is
no-conflicting-decision, not termination.

This is the paper's two-case consistency argument in one statement. The
proof (generated) consumes: `DirectSafety` (the direct-vs-direct
pairings), certificate uniqueness (rung 1 agreement), the starvation
invariant `q_fast + q_weak > n + f` and the rung ordering
`q_weak ≤ q_cert` (a fast commit starves every conflicting candidate off
both rungs), the "anchor sees any slow commit" row `q + q_slow > n + f`
(rung 1 fires at every eligible anchor), and a **strengthened** form of
the "anchor sees the fast footprint" row — `q_fast + q − n − f ≥ q_weak`
— because a Byzantine author's block in the anchor's history may be its
non-voting equivocation, so only the non-Byzantine overlap contributes
anchor-linked votes.
-/

namespace Hydrozoan

namespace SlotAgreement

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [LinearOrder BlockId] [F : Faults Replica]
  [S : Slots Replica]

/-- Any two verdicts on one slot agree: across views, across routes
(fast, slow, direct skip, certificate rung, weak rung, indirect
skip). -/
def DecidedUnique (U : BlockUniverse Replica BlockId) : Prop :=
  ∀ (V₁ V₂ : View U) (k : ℕ) (v₁ v₂ : Option BlockId),
    Decided U V₁ k v₁ → Decided U V₂ k v₂ → v₁ = v₂

/-- Slot agreement, over every fault configuration, schedule, tie-break
order, and block universe the model admits. -/
def Statement : Prop :=
  ∀ (Replica BlockId : Type) [Fintype Replica] [DecidableEq Replica]
    [DecidableEq BlockId] [LinearOrder BlockId] [Faults Replica]
    [Slots Replica] (U : BlockUniverse Replica BlockId),
    DecidedUnique U

end SlotAgreement

end Hydrozoan
