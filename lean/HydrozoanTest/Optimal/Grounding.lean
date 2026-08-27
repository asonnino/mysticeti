import Hydrozoan.Optimal.Grounding.Proof
import HydrozoanTest.Optimal.Universe

/-!
# Witness: Optimal-Hydrozoan grounding

End-to-end applications of the Optimal grounding theorem — the
mechanical guard against silently strengthened hypotheses (any added
explicit premise breaks an application by arity; the spelled-out example
types break conclusion weakenings by type mismatch, in particular a
retreat from `OptUniverse` to `BlockUniverse`):

* `WaveRobinFair` applied premise-free at `sevenReplicasOpt`
  (Byzantine `{0}`, crashed `{1}`, `q = 5`), at `fourReplicasOpt`
  (Byzantine `{0}`, `q = 3`) and at `threeReplicasCrashOnly` — the
  smallest committee carrying any `OptimalFaults` instance, and the
  `f = 0` branch — including a run extracted past slot 5;
* `HypothesesRealizable`, its conclusion typed at `OptUniverse`: on
  `Fin 7` at `T = univ` — a `T` containing the Byzantine replica 0 and
  the crashed replica 1, killing any `T ⊆ Correct` strengthening — and
  at `T = Correct` with `|T| = 5 = q`, the exact-quorum boundary, both
  under the wave-aligned schedule (`Fin 7` carries no `Slots` instance,
  so the schedule is passed explicitly); on `Fin 4` at `T = Correct`,
  `|T| = 3 = q`, under the two-slots-per-round schedule
  `fourSlotsTwoPerRound` — the `Slots` quantifier is real, not fixed to
  `waveRobin`; and on `Fin 20` at `|T| = 13 = q < qFastOpt = 16`, the
  one configuration here whose quorums separate, so a strengthening of
  the premise to `qFastOpt ≤ |T|` fails;
* the premise's negative: on `Fin 4`, a `T`-only universe with
  `|T| = 2 < q` cannot populate round 1 — the prose "past genesis, a
  `T`-only universe cannot validly populate a round below quorum size"
  made a theorem;
* `GroundedProgress` applied at `k = 5` (`Fin 7`), at the `k = 0`
  boundary (`Fin 4`) and at `Fin 3`, its three conclusion clauses —
  correct authors only, the commit at the bound, the decisions below
  it — spelled out over `DecidedOpt`.

Disclosed: in every universe realizing the package there is no
equivocation, so leader exclusion is inert — `HypothesesRealizable`
says the rule is compatible with the good case, not that it bites. Where
it bites is pinned in `HydrozoanTest/Optimal/Universe.lean` (`UbadX`, a
block universe admitting no `OptUniverse`). Two `Faults (Fin 7)`
instances are in scope (`sevenReplicas` and `sevenReplicasOpt.toFaults`);
they are definitionally the same structure, so `Correct : Finset (Fin 7)`
names one pool. The pointwise pins of `waveRobin`, the `T = ∅` fairness
negative and the per-slot-rotation starvation contrast are Hydrozoan's
(`HydrozoanTest/Grounding.lean`) and are not repeated: the schedule and
the fairness claim are reused verbatim.
-/

namespace HydrozoanTest

open Hydrozoan
open Hydrozoan.Optimal
open Hydrozoan.Grounding (waveRobin)

-- The configurations, pinned: the correct pools the applications below
-- name are the ones the instances define.
example : (Correct : Finset (Fin 7)) = {2, 3, 4, 5, 6} := by decide
example : (Correct : Finset (Fin 4)) = {1, 2, 3} := by decide
example : (Correct : Finset (Fin 3)) = {1, 2} := by decide

-- End-to-end: fairness, premise-free, at both configurations — the
-- reused Hydrozoan claim read at an `OptimalFaults` instance.
example : EventualDecision.FairRunOn (Fin 7)
    (S := waveRobin 7 (by omega)) (Correct : Finset (Fin 7)) 3 :=
  Optimal.Grounding.holds.1 7 (by omega)

example : EventualDecision.FairRunOn (Fin 4)
    (S := waveRobin 4 (by omega)) (Correct : Finset (Fin 4)) 3 :=
  Optimal.Grounding.holds.1 4 (by omega)

-- ... and at the smallest committee the class admits (n = 1, 2 carry no
-- `OptimalFaults` instance), crash-only.
example : EventualDecision.FairRunOn (Fin 3)
    (S := waveRobin 3 (by omega)) (Correct : Finset (Fin 3)) 3 :=
  Optimal.Grounding.holds.1 3 (by omega)

-- ... and a run extracted past slot 5, with the conclusion's shape
-- spelled out.
example : ∃ k', 5 ≤ k' ∧ ∀ i, i < 3 →
    (waveRobin 7 (by omega)).leader (k' + i) ∈ (Correct : Finset (Fin 7)) :=
  Optimal.Grounding.holds.1 7 (by omega) 5

-- End-to-end: realizability at T = univ — a T containing the Byzantine
-- replica 0 and the crashed replica 1 (q = 5 ≤ 7 = |univ|) — by an
-- `OptUniverse`. A `T ⊆ Correct` strengthening would break this
-- application; a retreat to `BlockUniverse` would break the type.
example :
    letI := waveRobin 7 (by omega)
    ∃ U : OptUniverse (Fin 7) ℕ,
    (∀ b ∈ U.ids, (U.block b).author ∈ (Finset.univ : Finset (Fin 7))) ∧
    (∀ r, r ≤ 10 → PopulatedOn U.toBlockUniverse (Finset.univ : Finset (Fin 7)) r) ∧
    SynchronisedOn U.toBlockUniverse (Finset.univ : Finset (Fin 7)) 0 :=
  letI := waveRobin 7 (by omega)
  Optimal.Grounding.holds.2.1 (Fin 7) Finset.univ 10 (by decide)

-- ... at the exact-quorum boundary: T = Correct with |T| = 5 = q, the
-- T-only clause biting hardest.
example :
    letI := waveRobin 7 (by omega)
    ∃ U : OptUniverse (Fin 7) ℕ,
    (∀ b ∈ U.ids, (U.block b).author ∈ (Correct : Finset (Fin 7))) ∧
    (∀ r, r ≤ 6 → PopulatedOn U.toBlockUniverse (Correct : Finset (Fin 7)) r) ∧
    SynchronisedOn U.toBlockUniverse (Correct : Finset (Fin 7)) 0 :=
  letI := waveRobin 7 (by omega)
  Optimal.Grounding.holds.2.1 (Fin 7) (Correct : Finset (Fin 7)) 6 (by decide)

-- ... under a schedule with two slots per round, at T = Correct with
-- |T| = 3 = q: the claim's `Slots` quantifier is real — it is not fixed
-- to the wave-aligned schedule.
example :
    letI := fourSlotsTwoPerRound
    ∃ U : OptUniverse (Fin 4) ℕ,
    (∀ b ∈ U.ids, (U.block b).author ∈ (Correct : Finset (Fin 4))) ∧
    (∀ r, r ≤ 8 → PopulatedOn U.toBlockUniverse (Correct : Finset (Fin 4)) r) ∧
    SynchronisedOn U.toBlockUniverse (Correct : Finset (Fin 4)) 0 :=
  letI := fourSlotsTwoPerRound
  Optimal.Grounding.holds.2.1 (Fin 4) (Correct : Finset (Fin 4)) 8 (by decide)

-- ... and where the quorums separate: on Fin 20 (f = 3, c = 4, k = 2),
-- T = the first 13 replicas has |T| = 13 = q, below qFastOpt = 16 — a
-- strengthening of the premise to `qFastOpt ≤ |T|` fails here.
example :
    (Finset.univ.filter fun v : Fin 20 => v.val < 13).card = 13 ∧
      q (Fin 20) = 13 ∧ qFastOpt (Fin 20) = 16 := by
  decide

example :
    letI := waveRobin 20 (by omega)
    ∃ U : OptUniverse (Fin 20) ℕ,
    (∀ b ∈ U.ids, (U.block b).author ∈ (Finset.univ.filter fun v : Fin 20 => v.val < 13)) ∧
    (∀ r, r ≤ 5 →
      PopulatedOn U.toBlockUniverse (Finset.univ.filter fun v : Fin 20 => v.val < 13) r) ∧
    SynchronisedOn U.toBlockUniverse (Finset.univ.filter fun v : Fin 20 => v.val < 13) 0 :=
  letI := waveRobin 20 (by omega)
  Optimal.Grounding.holds.2.1 (Fin 20) (Finset.univ.filter fun v : Fin 20 => v.val < 13) 5
    (by decide)

-- Negative for the premise: a T-only universe with |T| = 2 < q = 3
-- cannot populate round 1 — its round-1 blocks would need three distinct
-- T authors among their parents.
example : ¬ ∃ U : OptUniverse (Fin 4) ℕ,
    (∀ b ∈ U.ids, (U.block b).author ∈ ({1, 2} : Finset (Fin 4))) ∧
    PopulatedOn U.toBlockUniverse {1, 2} 1 := by
  rintro ⟨U, hT, hpop⟩
  obtain ⟨b, hb, hround, -⟩ := hpop 1 (by decide)
  have hq := (U.valid b hb).quorum (by rw [hround]; exact Nat.one_pos)
  have hsub : authors U.block (U.block b) ⊆ {1, 2} := by
    intro a ha
    obtain ⟨j, hj, rfl⟩ := Finset.mem_image.mp ha
    exact hT j (U.complete b hb j hj)
  have hcard := Finset.card_le_card hsub
  have hq3 : q (Fin 4) = 3 := by decide
  have h2 : ({1, 2} : Finset (Fin 4)).card = 2 := by decide
  omega

-- End-to-end: grounded progress past slot 5, with the three conclusion
-- clauses — correct authors only, the commit at the bound, the decisions
-- below it — spelled out over `DecidedOpt`.
example :
    letI := waveRobin 7 (by omega)
    ∃ b, 5 ≤ b ∧ ∃ U : OptUniverse (Fin 7) ℕ,
    (∀ i ∈ U.ids, (U.block i).author ∈ (Correct : Finset (Fin 7))) ∧
    (∃ L, DecidedOpt U (View.full U.toBlockUniverse) b (some L)) ∧
    ∀ i, i < b → ∃ v, DecidedOpt U (View.full U.toBlockUniverse) i v :=
  Optimal.Grounding.holds.2.2 7 (by omega) 5

-- ... at the k = 0 boundary, second configuration. The bound is opaque:
-- under `waveRobin 4` slots 0–2 are led by the Byzantine replica 0, so a
-- correct-authored universe commits no earlier than slot 3 and the
-- slots below are settled candidate-less.
example :
    letI := waveRobin 4 (by omega)
    ∃ b, 0 ≤ b ∧ ∃ U : OptUniverse (Fin 4) ℕ,
    (∀ i ∈ U.ids, (U.block i).author ∈ (Correct : Finset (Fin 4))) ∧
    (∃ L, DecidedOpt U (View.full U.toBlockUniverse) b (some L)) ∧
    ∀ i, i < b → ∃ v, DecidedOpt U (View.full U.toBlockUniverse) i v :=
  Optimal.Grounding.holds.2.2 4 (by omega) 0

-- ... and at the smallest committee, crash-only: two correct replicas
-- make the DAG quorum q = 2 by themselves.
example :
    letI := waveRobin 3 (by omega)
    ∃ b, 0 ≤ b ∧ ∃ U : OptUniverse (Fin 3) ℕ,
    (∀ i ∈ U.ids, (U.block i).author ∈ (Correct : Finset (Fin 3))) ∧
    (∃ L, DecidedOpt U (View.full U.toBlockUniverse) b (some L)) ∧
    ∀ i, i < b → ∃ v, DecidedOpt U (View.full U.toBlockUniverse) i v :=
  Optimal.Grounding.holds.2.2 3 (by omega) 0

end HydrozoanTest
