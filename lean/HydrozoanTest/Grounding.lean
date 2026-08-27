import Hydrozoan.Grounding.Proof
import HydrozoanTest.Model
import HydrozoanTest.DirectLiveness

/-!
# Witness: grounding

End-to-end applications of the grounding theorem — the mechanical
guard against silently strengthened hypotheses (any added EXPLICIT
premise breaks an application by arity; the spelled-out example types
break conclusion weakenings by type mismatch):

* the wave-aligned schedule pinned pointwise: slot `k` proposes at
  round `k`, the leader holds for a whole three-slot wave, and the
  rotation re-enters after `3 · 7` slots;
* `WaveRobinFair` applied premise-free at TWO configurations — the
  pinned `Fin 7` table (Byzantine `{0}`, crashed `{1}`, `q = 5`) and
  the low-fault `Fin 4` one — including extracting a run past slot 5;
  plus the `T = ∅` negative (fairness is not trivially true of every
  `T`);
* `HypothesesRealizable` applied at `T = univ` — a `T` containing the
  Byzantine replica 0 and the crashed replica 1 — killing any
  `T ⊆ Correct` strengthening, and at `T = Correct` with
  `|T| = 5 = q`, the exact-quorum boundary;
* `GroundedProgress` applied at `k = 5` (`Fin 7`) and at the `k = 0`
  boundary (`Fin 4`), its commit clause and everything-below clause
  both spelled out;
* the pigeonhole contrast made concrete: PER-SLOT rotation at the
  tight `Fin 5` configuration (`f = 0`, `c = 2`, `k = 0`, crashed
  `{0, 3}`) provably starves every correct 3-run — the wave-aligned
  grain is necessary for premise-free fairness, not a stylistic
  choice.
-/

namespace HydrozoanTest

open Hydrozoan

-- The schedule is what its docstring says: pipelined rounds, leader
-- constant across each wave of three slots, rotation advancing per wave.
example : (Grounding.waveRobin 7 (by omega)).slotRound 5 = 5 := by decide
example : (Grounding.waveRobin 7 (by omega)).leader 0 = 0 := by decide
example : (Grounding.waveRobin 7 (by omega)).leader 2 = 0 := by decide
example : (Grounding.waveRobin 7 (by omega)).leader 3 = 1 := by decide
example : (Grounding.waveRobin 7 (by omega)).leader 20 = 6 := by decide
-- The cycle re-enters: slot 21 opens replica 0's next wave.
example : (Grounding.waveRobin 7 (by omega)).leader 21 = 0 := by decide

-- A concrete correct wave, checked pointwise: slots 6, 7, 8 are all
-- led by replica 2, and 2 ∈ Correct.
example : (Grounding.waveRobin 7 (by omega)).leader 6 = 2 := by decide
example : (Grounding.waveRobin 7 (by omega)).leader 7 = 2 := by decide
example : (Grounding.waveRobin 7 (by omega)).leader 8 = 2 := by decide
example : (Grounding.waveRobin 7 (by omega)).leader 6
    ∈ (Correct : Finset (Fin 7)) := by decide

-- End-to-end: fairness at the pinned configuration, premise-free.
example : EventualDecision.FairRunOn (Fin 7)
    (S := Grounding.waveRobin 7 (by omega)) (Correct : Finset (Fin 7)) 3 :=
  Grounding.holds.1 7 (by omega)

-- ... at a second configuration, pinning the ∀-over-n generality.
example : EventualDecision.FairRunOn (Fin 4)
    (S := Grounding.waveRobin 4 (by omega)) (Correct : Finset (Fin 4)) 3 :=
  Grounding.holds.1 4 (by omega)

-- ... and a run extracted past slot 5, with the conclusion's shape
-- spelled out.
example : ∃ k', 5 ≤ k' ∧ ∀ i, i < 3 →
    (Grounding.waveRobin 7 (by omega)).leader (k' + i)
      ∈ (Correct : Finset (Fin 7)) :=
  Grounding.holds.1 7 (by omega) 5

-- Negative: fairness is a claim about the leader set, not trivially
-- true of every `T` — the empty set starves.
example : ¬ EventualDecision.FairRunOn (Fin 7)
    (S := Grounding.waveRobin 7 (by omega)) (∅ : Finset (Fin 7)) 3 := by
  intro h
  obtain ⟨k', -, hlead⟩ := h 0
  have := hlead 0 (by omega)
  simp at this

-- End-to-end: realizability at T = univ — a T containing the Byzantine
-- replica 0 and the crashed replica 1 (q = 5 ≤ 7 = |univ|). A
-- `T ⊆ Correct` strengthening of the claim would break this
-- application.
example : ∃ U : BlockUniverse (Fin 7) ℕ,
    (∀ b ∈ U.ids, (U.block b).author ∈ (Finset.univ : Finset (Fin 7))) ∧
    (∀ r, r ≤ 10 → PopulatedOn U (Finset.univ : Finset (Fin 7)) r) ∧
    SynchronisedOn U (Finset.univ : Finset (Fin 7)) 0 :=
  Grounding.holds.2.1 (Fin 7) Finset.univ 10 (by decide)

-- ... and at the exact-quorum boundary: T = Correct with |T| = 5 = q,
-- the T-only clause biting hardest (five authors must sustain every
-- round's quorum by themselves).
example : ∃ U : BlockUniverse (Fin 7) ℕ,
    (∀ b ∈ U.ids, (U.block b).author ∈ (Correct : Finset (Fin 7))) ∧
    (∀ r, r ≤ 6 → PopulatedOn U (Correct : Finset (Fin 7)) r) ∧
    SynchronisedOn U (Correct : Finset (Fin 7)) 0 :=
  Grounding.holds.2.1 (Fin 7) (Correct : Finset (Fin 7)) 6 (by decide)

-- End-to-end: grounded progress past slot 5, with both conclusion
-- clauses — the commit at the bound and the decisions below it —
-- spelled out.
example : ∃ b, 5 ≤ b ∧ ∃ U : BlockUniverse (Fin 7) ℕ,
    (∃ L, Decided (S := Grounding.waveRobin 7 (by omega)) U
      (View.full U) b (some L)) ∧
    ∀ i, i < b → ∃ v, Decided (S := Grounding.waveRobin 7 (by omega)) U
      (View.full U) i v :=
  Grounding.holds.2.2 7 (by omega) 5

-- ... and at the k = 0 boundary, second configuration: even with
-- nothing below to decide, the commit clause still demands a real
-- verdict.
example : ∃ b, 0 ≤ b ∧ ∃ U : BlockUniverse (Fin 4) ℕ,
    (∃ L, Decided (S := Grounding.waveRobin 4 (by omega)) U
      (View.full U) b (some L)) ∧
    ∀ i, i < b → ∃ v, Decided (S := Grounding.waveRobin 4 (by omega)) U
      (View.full U) i v :=
  Grounding.holds.2.2 4 (by omega) 0

/-! ## The pigeonhole contrast: per-slot rotation starves at the bound

`WaveRobinFair`'s docstring says per-slot rotation needs the replica
count to exceed three times the actual fault count. The tiniest
counterexample inside the fault bounds: `n = 5`, `f = 0`, `c = 2`,
`k = 0` (tight: `3·0 + 2·2 + 0 + 1 = 5`), crashed at positions 0 and 3.
Every window of three consecutive residues mod 5 hits 0 or 3, so no
correct 3-run ever forms — per-slot rotation is UNFAIR at the hybrid
bound, which is exactly why `waveRobin` rotates at wave grain. -/

instance fiveCrashy : Faults (Fin 5) where
  f := 0
  c := 2
  k := 0
  byzantine := ∅
  crashed := {0, 3}
  byzantine_disjoint_crashed := by decide
  card_replicas := by decide
  card_byzantine := by decide
  card_crashed := by decide

/-- Per-slot round-robin on five replicas: slot `k` at round `k`, led
by replica `k mod 5` — the rotation `waveRobin` deliberately does NOT
use. -/
@[instance_reducible]
def slotRobin : Slots (Fin 5) where
  slotRound k := k
  leader k := ⟨k % 5, Nat.mod_lt _ (by omega)⟩
  mono := fun _ _ h => h
  unbounded := fun m => ⟨m, le_refl m⟩
  keyed := fun _ _ h => congrArg Prod.fst h

-- Correct = {1, 2, 4}: arcs of length 2 and 1 — no room for a 3-run.
example : (Correct : Finset (Fin 5)) = {1, 2, 4} := by decide

example : ¬ EventualDecision.FairRunOn (Fin 5) (S := slotRobin)
    (Correct : Finset (Fin 5)) 3 := by
  intro h
  obtain ⟨k', -, hlead⟩ := h 0
  have hval : ∀ m : Fin 5, m ∈ (Correct : Finset (Fin 5)) →
      m.val ≠ 0 ∧ m.val ≠ 3 := by decide
  have h0 := hval _ (hlead 0 (by omega))
  have h1 := hval _ (hlead 1 (by omega))
  have h2 := hval _ (hlead 2 (by omega))
  have e0 : (slotRobin.leader (k' + 0)).val = (k' + 0) % 5 := rfl
  have e1 : (slotRobin.leader (k' + 1)).val = (k' + 1) % 5 := rfl
  have e2 : (slotRobin.leader (k' + 2)).val = (k' + 2) % 5 := rfl
  rw [e0] at h0
  rw [e1] at h1
  rw [e2] at h2
  omega

end HydrozoanTest
