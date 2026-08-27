import Hydrozoan.Optimal.IndirectLiveness.Statement
import Hydrozoan.EventualDecision.Statement
import Hydrozoan.Model.Liveness

/-!
# Optimal-Hydrozoan: eventual decision — the ledger does not stall

Hydrozoan's `EventualDecision` read over `DecidedOpt`: the liveness
headline, composing direct liveness (a synchronised, populated,
correct-led wave commits) with indirect liveness (a committed run settles
everything below). The one ingredient still missing is fairness — the
schedule must actually *offer* runs of correct-led slots — and that is a
schedule-only notion, so `FairRunOn` and `RunsRecur` are Hydrozoan's,
reused verbatim; only the per-universe workhorse is re-stated:

- `RunDecidesBelow`: a synchronised quorum whose members lead the `c`
  slots `b, …, b + c − 1` and fill every round of the run's span decides
  every slot below `b` at the eventual view. No fairness: where the run
  sits is a hypothesis.
- `RunsRecur` (Hydrozoan's): fairness places a `T`-led run past any slot
  and any round.

The two compose by direct application; the composed form is stated and
proven on the generated side (`ledgerProgress` in `Proof.lean`), as in
Hydrozoan. Imports Hydrozoan's `EventualDecision.Statement` and the
Optimal `IndirectLiveness.Statement` for the reused shapes (the
sanctioned reviewed-imports-reviewed exception).
-/

namespace Hydrozoan

namespace Optimal

namespace EventualDecision

open Hydrozoan.IndirectLiveness (SpansEligible)
open Hydrozoan.EventualDecision (FairRunOn RunsRecur)

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [O : OptimalFaults Replica] [S : Slots Replica]

/-- **A committed-to-be run decides everything below it.** The workhorse
with the run location `b` explicit: direct liveness commits each of the
`c` run slots (through the unchanged slow path), and the indirect descent
settles every slot below. -/
def RunDecidesBelow (U : OptUniverse Replica BlockId) : Prop :=
  ∀ (T : Finset Replica) (R b c : ℕ),
    T ⊆ (Correct : Finset Replica) →     -- a set of correct replicas ...
    q Replica ≤ T.card →                 -- ... of at least a DAG quorum,
    SynchronisedOn U.toBlockUniverse T R →  -- internally synchronised from R,
    0 < c →                              -- a nonempty run of slots ...
    SpansEligible Replica c →            -- ... every run's end anchoring all below,
    R ≤ S.slotRound b →                  -- lying at or after R,
    (∀ i, i < c → S.leader (b + i) ∈ T) →  -- every run slot T-led,
    (∀ r, S.slotRound b ≤ r →            -- and T fills every round from
      r ≤ S.slotRound (b + c - 1) + 2 →  -- the run's propose round to its
      PopulatedOn U.toBlockUniverse T r) →  -- last decision round:
    ∀ i, i < b → ∃ v, DecidedOpt U (View.full U.toBlockUniverse) i v  -- all below decided.

/-- Eventual decision of Optimal-Hydrozoan, over every fault
configuration, schedule, and universe the model admits — together with
Hydrozoan's schedule-only fairness claim. -/
def Statement : Prop :=
  ∀ (Replica BlockId : Type) [Fintype Replica] [DecidableEq Replica]
    [DecidableEq BlockId] [OptimalFaults Replica] [Slots Replica],
    (∀ U : OptUniverse Replica BlockId, RunDecidesBelow U) ∧
      RunsRecur Replica

end EventualDecision

end Optimal

end Hydrozoan
