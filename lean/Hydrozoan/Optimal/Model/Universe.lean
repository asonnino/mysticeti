import Hydrozoan.Model.DirectRules

/-!
# Optimal-Hydrozoan: the block universe with leader exclusion

Trusted core of the Optimal-Hydrozoan arc: the "DAG-building layer"
paragraph of `sections/optimal-protocol.tex`, i.e. FinWhale's validity
rule, as one extra well-formedness condition on the block universe.
Definitions only.

`OptUniverse` *extends* Hydrozoan's `BlockUniverse` (frozen, untouched):
every condition of the base — completeness, validity, non-equivocation
of non-Byzantine authors — is inherited, and every Hydrozoan lemma
applies to `U.toBlockUniverse`. Views are Hydrozoan's `View` over that
projection; nothing new is needed there.

The extra condition is what buys the arc its extra fast-path fault: a
decision-round block that has *seen* a leader equivocate (its parents
vote for two distinct blocks of that leader's slot) must not reference
that leader's own block. The leader is then a **detected** Byzantine
replica, and the seam proof (O6) counts at most `f − 1` undetected
Byzantine replicas among the block's parents. The rule constrains which
blocks a replica references — not how votes are counted; the counting
dividend is a theorem, not a definition.

**Fidelity** (decision D6): "a parent votes for `L`" is the direct
reference `L ∈ parents`, exactly the paper's `Votes(b, ·)` read at wave
length 3; and the paper's `WitnessesEquivocation(b, w)` quantifies over
the leader blocks in the local DAG, which coincides with the
universe-level form here because the two blocks a witnessing block sees
are parents of its parents, hence held by any view holding the block.
-/

namespace Hydrozoan

namespace Optimal

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [F : Faults Replica] [S : Slots Replica]

/-- `b` witnesses an equivocation in slot `k` (the paper's
`WitnessesEquivocation(b, w)`, Algorithm 3): two *distinct* candidates of
slot `k` — two blocks by `k`'s leader at `k`'s propose round — are each
voted for by some parent of `b`. Stated for any block `b`; the round at
which the rule applies is fixed by `OptUniverse.leader_excluded`.

A plain definition, like the top-level rules of `Model/DirectRules.lean`:
its `Decidable` instance (over a `Fintype` of ids) lives in
`Optimal/Helpers/Universe.lean`. -/
def WitnessesEquivocation (U : BlockUniverse Replica BlockId) (k : ℕ)
    (b : BlockId) : Prop :=
  ∃ L₁ L₂, IsLeaderBlock U k L₁ ∧ IsLeaderBlock U k L₂ ∧ L₁ ≠ L₂ ∧
    (∃ j ∈ (U.block b).parents, IsVote U j L₁) ∧
    (∃ j ∈ (U.block b).parents, IsVote U j L₂)

/-- Hydrozoan's block universe plus the leader-exclusion rule. -/
structure OptUniverse (Replica BlockId : Type*) [Fintype Replica]
    [DecidableEq Replica] [DecidableEq BlockId] [F : Faults Replica]
    [S : Slots Replica] extends BlockUniverse Replica BlockId where
  /-- **Leader exclusion** — the validity rule of `sections/optimal-protocol.tex`:
  a block at the decision round of slot `k` that witnesses an equivocation
  in `k` references no block authored by `k`'s leader. The round guard is
  stated explicitly (decision D4) although it is *redundant* for
  `b ∈ ids`: witnessing already forces `b`'s round to be `k`'s decision
  round (twice `predecessor`, from a voted candidate at `k`'s propose
  round), so no witness can tell its presence — it is here so the rule
  reads as the paper states it. With several slots per round the rule
  applies to each slot separately, which the `∀ k` gives for free
  (pinned by the two-slots-per-round schedule of the witness file). -/
  leader_excluded : ∀ b ∈ ids, ∀ k,
    (block b).round = decisionRound Replica k →
    WitnessesEquivocation toBlockUniverse k b →
    ∀ j ∈ (block b).parents, (block j).author ≠ S.leader k

end Optimal

end Hydrozoan
