import Hydrozoan.Optimal.EventualDecision.Statement
import Hydrozoan.Grounding.Statement

/-!
# Optimal-Hydrozoan: grounding — the liveness hypotheses are dischargeable

Hydrozoan's `Grounding` read over the Optimal arc. The liveness theorems
consume three kinds of assumed hypotheses: schedule fairness, and the
synchrony and population package of `Model/Liveness.lean`. Fairness is a
schedule-only notion, so `WaveRobinFair` — the wave-aligned rotation is
fair with no premise beyond the fault model — is Hydrozoan's, reused
verbatim (it quantifies over every `Faults` instance, hence over every
`OptimalFaults` one). The two universe-level conjuncts are re-stated
because their universes must now be `OptUniverse`s:

- `HypothesesRealizable`: for every quorum-sized `T`, every horizon `N`
  and — new — every schedule, some `OptUniverse` authored entirely by
  `T` satisfies the whole package up to `N`. FinWhale's leader-exclusion
  rule is implied by the package (a good-case universe contains no
  equivocation), so the conjunct fixes the witness's type rather than
  adding an obligation.
- `GroundedProgress`: under wave-aligned round-robin, past every slot
  some bound is committed with every slot below it decided, in the
  sense of `DecidedOpt`, by a universe authored by correct replicas
  alone — the composed conclusion of the Optimal liveness arc,
  satisfiable with no premise beyond the fault model.

As in Hydrozoan, `SpansEligible` is grounded only implicitly (discharged
inside the generated proof for the pipelined `waveRobin` at run length
3), and message delivery, GST and timeouts are out of scope: this claim
grounds satisfiability, not operational realizability. `FastLatency`
(outside the OL1 `Statement`) is not grounded either. Imports Hydrozoan's
`Grounding.Statement` for `waveRobin` and `WaveRobinFair` (the sanctioned
reviewed-imports-reviewed exception).
-/

namespace Hydrozoan

namespace Optimal

namespace Grounding

open Hydrozoan.Grounding (waveRobin WaveRobinFair)

/-- **The liveness hypothesis package is realizable at every horizon,
under every schedule, by an Optimal universe.** For any set `T` of at
least DAG-quorum size and any horizon `N`, some `OptUniverse` authored
ENTIRELY by `T` has `T` filling every round up to `N` and internally
synchronised from round 0.

Hydrozoan's reading carries over: the good-period scenario the liveness
theorems condition on is a CONSISTENT scenario of the model at every
scale and length, and the `T`-only clause is what earns the
`q ≤ T.card` premise (past genesis, a `T`-only universe cannot validly
populate a round below quorum size). The Optimal reading fixes the
witness's TYPE: the universe is an `OptUniverse`, so leader exclusion
holds in it, under whatever schedule the rule is read against. This is
not an extra obligation — it is implied by the package itself. In a
`T`-only universe synchronised from round 0, two blocks of one author in
one round would both be parents of every `T`-block above, against
`distinct_authors`; and a block witnessing an equivocation has
`T`-authored parents above the two candidates. So no block of any
universe meeting the package witnesses anything, and the rule is inert
in every such universe: the good case never triggers it. Where the rule
bites is a separate matter (`HydrozoanTest/Optimal/Universe.lean`
exhibits a block universe over which NO `OptUniverse` exists). -/
def HypothesesRealizable : Prop :=
  ∀ (Replica : Type) [Fintype Replica] [DecidableEq Replica]
    [OptimalFaults Replica] [Slots Replica] (T : Finset Replica) (N : ℕ),
    q Replica ≤ T.card →                   -- a quorum-sized T:
    ∃ U : OptUniverse Replica ℕ,           -- some Optimal universe is
      (∀ b ∈ U.ids, (U.block b).author ∈ T) ∧  -- authored by T alone,
      (∀ r, r ≤ N → PopulatedOn U.toBlockUniverse T r) ∧  -- populated to N
      SynchronisedOn U.toBlockUniverse T 0  -- and synchronised throughout.

/-- **Grounded progress.** Under wave-aligned round-robin, the composed
Optimal liveness conclusion is achievable with no premise at all: past
every point, some Optimal universe commits a bound with every slot below
it decided. An achievability claim, as in Hydrozoan — the statement
asserts the conclusion's satisfiability, not the route to it (a universe
may reach these verdicts by any of the six `DecidedOpt` routes). Each
horizon is witnessed by its own finite universe, and the bound `b`
itself must COMMIT — an all-skip universe does not qualify.

The universe is authored by CORRECT replicas alone. Without that clause
the claim would never consult the fault sets: a universe in which every
replica, faulty or not, authors every round satisfies the conclusion at
any configuration. With it, the faulty replicas contribute nothing, and
the claim is that the correct ones suffice — which is what "no premise
beyond the fault model" is meant to say. -/
def GroundedProgress : Prop :=
  ∀ (n : ℕ) (hn : 0 < n) [OptimalFaults (Fin n)],
    letI : Slots (Fin n) := waveRobin n hn    -- under wave-aligned rotation,
    ∀ k : ℕ, ∃ b, k ≤ b ∧                     -- past any slot k,
      ∃ U : OptUniverse (Fin n) ℕ,            -- some Optimal universe
        (∀ i ∈ U.ids, (U.block i).author ∈ Correct) ∧  -- of correct authors only
        (∃ L, DecidedOpt U (View.full U.toBlockUniverse) b (some L)) ∧  -- commits b
        ∀ i, i < b → ∃ v,                     -- with every slot below
          DecidedOpt U (View.full U.toBlockUniverse) i v  -- decided.

/-- Grounding of the Optimal arc, over every replica count and fault
configuration the model admits: Hydrozoan's fairness claim, and the two
universe-level claims over `OptUniverse`. -/
def Statement : Prop :=
  WaveRobinFair ∧ HypothesesRealizable ∧ GroundedProgress

end Grounding

end Optimal

end Hydrozoan
