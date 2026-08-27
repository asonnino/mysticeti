import Hydrozoan.Model.Faults

/-!
# Witness: the fault model is satisfiable

A concrete `Faults` instance, checked by `decide`, so the class's
hypotheses are demonstrably not contradictory. Built by default: a change
to the class that empties it fails the build.

Seven replicas with `f = 1, c = 1, k = 1` — at the tight replica count,
the smallest slack at which `qCert` and `qSlow` diverge (with non-tight
`n` they diverge at `k = 0` already). Here `q = qCert = 5`, so two of
the three quorums still coincide; the full three-way split first appears
at the `k = 2` row of `HydrozoanTest/Thresholds.lean`. Note also that
`f = p = 1` makes `n − p` and `n − f` indistinguishable in this file
alone — the `k = 1` row of the thresholds table (`p = 17 ≠ f = 10`) is
what tells them apart.
-/

namespace HydrozoanTest

open Hydrozoan

/-- Seven replicas: replica `0` Byzantine, replica `1` crashed, slack one
(`3f + 2c + k + 1 = 7 = n`, tight). -/
instance sevenReplicas : Faults (Fin 7) where
  f := 1
  c := 1
  k := 1
  byzantine := {0}
  crashed := {1}
  byzantine_disjoint_crashed := by decide
  card_replicas := by decide
  card_byzantine := by decide
  card_crashed := by decide

-- The full threshold table at n = 7, f = 1, c = 1, k = 1. Note
-- qCert = 5 > qSlow = 4: at the tight replica count, the divergence
-- between the certificate's inner threshold and the slow-commit outer
-- quorum appears from k = 1.
example :
    p (Fin 7) = 1 ∧ q (Fin 7) = 5 ∧ qFast (Fin 7) = 6 ∧
      qCert (Fin 7) = 5 ∧ qSlow (Fin 7) = 4 ∧ qWeak (Fin 7) = 3 := by
  decide

-- Negative example: `Correct` is not everyone — the fault sets bite.
example : (Correct : Finset (Fin 7)) = {2, 3, 4, 5, 6} := by decide

-- And `NonByzantine` strictly contains `Correct`: the crashed replica is
-- non-Byzantine (never equivocates) without being correct.
example : (NonByzantine : Finset (Fin 7)) = {1, 2, 3, 4, 5, 6} := by decide

end HydrozoanTest
