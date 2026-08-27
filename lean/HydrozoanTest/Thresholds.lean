import Hydrozoan.Model.Faults

/-!
# Witness: the slack-cap threshold table

The three-quorums threshold table at `f = 10, c = 34` — three quorums
that coincide at `k = 0` and fan out with `k`, encoding different
invariants — pinned row by row with `decide`. The instances live at the
tight replica count `n = 3f + 2c + k + 1` with empty actual fault sets:
the thresholds read only the bounds `f`, `c`, `k`, and
`HydrozoanTest.Model` already witnesses non-empty fault sets.

What this guards: the growth shape of the three quorums — `q` climbs with
`k`, `q_cert` with `⌈k/2⌉`, `q_slow` stays flat — the `k ≥ 1` divergence
that makes them non-interchangeable. A drive-by change to any threshold
definition breaks a pinned row here before it silently weakens a theorem.
-/

namespace HydrozoanTest

open Hydrozoan

/-- A fault configuration at the tight replica count with empty actual
fault sets — the thresholds only read the bounds `f`, `c`, `k`. -/
@[instance_reducible]
def tight (f c k : ℕ) : Faults (Fin (3 * f + 2 * c + k + 1)) where
  f := f
  c := c
  k := k
  byzantine := ∅
  crashed := ∅
  byzantine_disjoint_crashed := Finset.disjoint_empty_left _
  card_replicas := by simp
  card_byzantine := by simp
  card_crashed := by simp

instance : Faults (Fin 99) := tight 10 34 0
instance : Faults (Fin 100) := tight 10 34 1
instance : Faults (Fin 101) := tight 10 34 2
instance : Faults (Fin 103) := tight 10 34 4
instance : Faults (Fin 109) := tight 10 34 10

-- k = 0 (n = 99): the three quorums coincide.
example : q (Fin 99) = 55 ∧ qCert (Fin 99) = 55 ∧ qSlow (Fin 99) = 55 := by decide

-- k = 1 (n = 100): the note's running example, all six thresholds.
example :
    p (Fin 100) = 17 ∧ q (Fin 100) = 56 ∧ qFast (Fin 100) = 83 ∧
      qCert (Fin 100) = 56 ∧ qSlow (Fin 100) = 55 ∧ qWeak (Fin 100) = 28 := by
  decide

-- k = 2 (n = 101): `q_cert` falls behind `q` for good.
example : q (Fin 101) = 57 ∧ qCert (Fin 101) = 56 ∧ qSlow (Fin 101) = 55 := by decide

-- k = 4 (n = 103).
example : q (Fin 103) = 59 ∧ qCert (Fin 103) = 57 ∧ qSlow (Fin 103) = 55 := by decide

-- k = 10 (n = 109): `q` has climbed by `k`, `q_cert` by `⌈k/2⌉`, `q_slow`
-- not at all.
example : q (Fin 109) = 65 ∧ qCert (Fin 109) = 60 ∧ qSlow (Fin 109) = 55 := by decide

end HydrozoanTest
