import Hydrozoan.EventualDecision.Statement

/-!
# Statement: grounding — the liveness hypotheses are dischargeable

The liveness arc consumes three kinds of assumed hypotheses: synchrony
and population (the `Model/Liveness.lean` package, witnessed by finite
tables) and schedule fairness. This claim grounds them systematically:

- `WaveRobinFair`: SOME schedule provably satisfies the fairness
  hypothesis — the wave-aligned rotation, where the same leader holds
  for a whole wave before the schedule rotates, is fair with NO
  premise at all: a single correct leader produces a full run by
  itself, and the fault bounds always guarantee one. One schedule is
  all grounding needs; the liveness theorems quantify over every
  `Slots` instance, so any other fair schedule inherits them.
- `HypothesesRealizable`: for every quorum-sized `T` and every horizon
  `N`, some universe authored ENTIRELY by `T` satisfies the whole
  liveness-hypothesis package up to `N` — the hypotheses cohere at
  arbitrary size, not only in the pinned tables.
- `GroundedProgress`: under wave-aligned round-robin, past every slot
  some bound is COMMITTED with every slot below it decided — the
  composed conclusion of the liveness arc, satisfiable with no premise
  beyond the fault model itself.

One statement-level liveness hypothesis is grounded only implicitly:
`SpansEligible`, the schedule-shape premise of the descent. It holds
for any pipelined schedule at run length 3 (`waveRobin` included), but
no Prop here records that — it is discharged inside the generated
proof, not on the audit surface.

Deliberately NOT here: message delivery, GST, timeouts. `SynchronisedOn`
records that deriving synchrony from delivery primitives is future
work; this claim grounds satisfiability, not operational realizability.
-/

namespace Hydrozoan
namespace Grounding

/-- The wave-aligned round-robin schedule on `n` replicas: one slot
per round (pipelined), with the leader holding for a whole wave —
`waveLength = 3` consecutive slots — before the rotation advances.
One concrete fair schedule, which is all grounding needs; leader
election in a deployment is a separate, pluggable concern outside
this model, and the liveness theorems quantify over every `Slots`
instance. Self-contained rather than built from the schedule
constructors, which live outside the audit surface. -/
@[instance_reducible]
def waveRobin (n : ℕ) (hn : 0 < n) : Slots (Fin n) where
  slotRound k := k                            -- slot k proposes at round k,
  leader k := ⟨k / 3 % n, Nat.mod_lt _ hn⟩    -- leader holds for a wave;
  mono := fun _ _ h => h                      -- rounds are slot order,
  unbounded := fun m => ⟨m, le_refl m⟩        -- reach every round,
  keyed := fun _ _ h => congrArg Prod.fst h   -- and identify the slot.

/-- **A fair schedule exists — wave-aligned rotation, unconditionally.**
One correct leader's wave is a full correct 3-run all by itself, it
recurs every cycle, and the fault bounds guarantee a correct replica
exists — so no premise is needed beyond the fault model. (Per-slot
rotation would NOT do: it needs `n` to exceed three times the actual
fault count — the pigeonhole finding recorded on `FairRunOn` — which
is exactly why the wave-aligned schedule is the canonical witness.) -/
def WaveRobinFair : Prop :=
  ∀ (n : ℕ) (hn : 0 < n) [Faults (Fin n)],
    EventualDecision.FairRunOn (Fin n) (S := waveRobin n hn)
      (Correct : Finset (Fin n)) 3            -- correct 3-runs recur.

/-- **The liveness hypothesis package is realizable at every horizon.** For
any set `T` of at least DAG-quorum size and any horizon `N`, some
universe authored ENTIRELY by `T` has `T` filling every round up to `N`
and internally synchronised from round 0.

In the paper's terms: "a quorum of steady replicas produces a block
every round and hears each other's blocks in time" — the good-period
scenario the liveness theorems condition on — is a CONSISTENT scenario
of the model, at every scale and length, not only in the pinned finite
tables. The point is joint satisfiability: the universe must meet
population, synchrony, validity, and non-equivocation all at once.

The `T`-only clause makes the claim self-supporting — no outside
authors pad the DAG — and it is what earns the `q ≤ T.card` premise:
past genesis, a `T`-only universe cannot validly populate any round
below quorum size (`ValidWrt` demands `q` distinct-author parents per
block, and here every parent is `T`'s). Without the clause the premise
would be dead weight, dischargeable by non-`T` padding. -/
def HypothesesRealizable : Prop :=
  ∀ (Replica : Type) [Fintype Replica] [DecidableEq Replica]
    [Faults Replica] (T : Finset Replica) (N : ℕ),
    q Replica ≤ T.card →                   -- a quorum-sized T:
    ∃ U : BlockUniverse Replica ℕ,         -- some universe is
      (∀ b ∈ U.ids, (U.block b).author ∈ T) ∧  -- authored by T alone,
      (∀ r, r ≤ N → PopulatedOn U T r) ∧   -- populated to the horizon
      SynchronisedOn U T 0                 -- and synchronised throughout.

/-- **Grounded progress.** Under wave-aligned round-robin, the
composed liveness conclusion is achievable with no premise at all:
past every point, some universe commits a bound with every slot below
it decided. An achievability claim — the statement asserts the
conclusion's satisfiability, not the route to it (a universe may reach
these verdicts by any rule). Each horizon is witnessed by its own
finite universe (`U.ids` is a `Finset`, so no single universe decides
all slots), and the bound `b` itself must COMMIT — an all-skip
universe, where every slot is decided by blame alone, does not
qualify. -/
def GroundedProgress : Prop :=
  ∀ (n : ℕ) (hn : 0 < n) [Faults (Fin n)],
    ∀ k : ℕ, ∃ b, k ≤ b ∧                     -- past any slot k,
      ∃ U : BlockUniverse (Fin n) ℕ,          -- some universe commits b
        (∃ L, Decided (S := waveRobin n hn) U (View.full U) b (some L)) ∧
        ∀ i, i < b → ∃ v,                     -- with every slot below
          Decided (S := waveRobin n hn) U (View.full U) i v  -- decided.

/-- Grounding, over every replica count and fault configuration the
model admits. -/
def Statement : Prop :=
  WaveRobinFair ∧ HypothesesRealizable ∧ GroundedProgress

end Grounding
end Hydrozoan
