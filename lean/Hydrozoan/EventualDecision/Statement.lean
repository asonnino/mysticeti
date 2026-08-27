import Hydrozoan.IndirectLiveness.Statement
import Hydrozoan.Model.Liveness

/-!
# Statement: eventual decision — the ledger does not stall

The liveness headline, composing the two previous claims. Direct
liveness commits any synchronised, populated, correct-led wave; indirect
liveness settles everything below a committed run. The one ingredient
still missing is fairness: the schedule must actually *offer* runs of
correct-led slots.

Two Props, factored so each is about one thing:

- `RunDecidesBelow`: the per-universe workhorse with the run location
  `b` explicit — a synchronised quorum whose members lead the `c` slots
  `b, …, b + c − 1` and fill every round of the run's span decides every
  slot below `b`. No fairness: where the run sits is a hypothesis.
- `RunsRecur`: the schedule-only claim — fairness places a `T`-led run
  past any slot and any round. No universe: pure `Slots` arithmetic.

The two compose by direct application (get the run location from
`RunsRecur`, hand it to `RunDecidesBelow`): for every slot `k` there is
a bound `b ≥ k` with every slot below `b` decided at the eventual view —
verdicts march past any point, which is exactly "the ledger does not
stall". That composed form is stated and proven on the generated side
(`ledgerProgress` in `Proof.lean`); the audited content is exactly the
two Props above.
-/

namespace Hydrozoan
namespace EventualDecision

section Schedule

variable (Replica : Type*) [S : Slots Replica]

/-- Fair leader election, in the only form liveness needs: the schedule
places `c` consecutive `T`-led slots arbitrarily far out (`k` is
universal, so such runs recur forever). A round-robin schedule satisfies
this exactly when its rotation contains `c` consecutive `T`-members —
always true for `c = 3` at the classical bound `n = 3f + 1`, but NOT
guaranteed at the hybrid bound (many crashed replicas can be spaced so
no three correct ones are adjacent) — which is why fairness is a stated
hypothesis on the schedule rather than a theorem about it. Which leader
schedules provide it is a separate concern, outside this development. -/
def FairRunOn (T : Finset Replica) (c : ℕ) : Prop :=
  ∀ k, ∃ k', k ≤ k' ∧ ∀ i, i < c → S.leader (k' + i) ∈ T

/-- **Fairness places a run wherever needed**: past any slot `k` and any
round `R`, some run of `c` consecutive `T`-led slots begins. Pure
schedule arithmetic — no universe appears; feeding the produced location
to `RunDecidesBelow` is the liveness composition. -/
def RunsRecur : Prop :=
  ∀ (T : Finset Replica) (c k R : ℕ),
    FairRunOn Replica T c →              -- given a fair schedule:
    ∃ b, k ≤ b ∧                         -- a run location past k ...
      R ≤ S.slotRound b ∧                -- ... at or after round R ...
      ∀ i, i < c → S.leader (b + i) ∈ T  -- ... with every slot T-led.

end Schedule

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [LinearOrder BlockId] [F : Faults Replica]
  [S : Slots Replica]

/-- **A committed-to-be run decides everything below it.** The workhorse
with the run location `b` explicit: direct liveness commits each of the
`c` run slots, and the indirect descent settles every slot below. -/
def RunDecidesBelow (U : BlockUniverse Replica BlockId) : Prop :=
  ∀ (T : Finset Replica) (R b c : ℕ),
    T ⊆ (Correct : Finset Replica) →     -- a set of correct replicas ...
    q Replica ≤ T.card →                 -- ... of at least a DAG quorum,
    SynchronisedOn U T R →               -- internally synchronised from R,
    0 < c →                              -- a nonempty run of slots ...
    IndirectLiveness.SpansEligible Replica c →  -- ... every run's end anchoring all below,
    R ≤ S.slotRound b →                  -- lying at or after R,
    (∀ i, i < c → S.leader (b + i) ∈ T) →  -- every run slot T-led,
    (∀ r, S.slotRound b ≤ r →            -- and T fills every round from
      r ≤ S.slotRound (b + c - 1) + 2 →  -- the run's propose round to its
      PopulatedOn U T r) →               -- last decision round:
    ∀ i, i < b → ∃ v, Decided U (View.full U) i v  -- all below decided.

/-- Eventual decision, over every fault configuration, schedule,
tie-break order, and block universe the model admits. -/
def Statement : Prop :=
  ∀ (Replica BlockId : Type) [Fintype Replica] [DecidableEq Replica]
    [DecidableEq BlockId] [LinearOrder BlockId] [Faults Replica]
    [Slots Replica],
    (∀ U : BlockUniverse Replica BlockId, RunDecidesBelow U) ∧
      RunsRecur Replica

end EventualDecision
end Hydrozoan
