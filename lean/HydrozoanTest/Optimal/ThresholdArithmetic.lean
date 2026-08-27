import Hydrozoan.Optimal.ThresholdArithmetic.Statement
import HydrozoanTest.Optimal.Thresholds

/-!
# Witness: the Optimal-Hydrozoan threshold table, row by row

Every row of `Optimal.ThresholdArithmetic` is `decide`d at each
`OptimalFaults` instance of `HydrozoanTest/Optimal/Thresholds.lean`, so
the claims are demonstrably satisfiable before they are proved in
general — and a definition drifting under a row breaks a pinned instance
here before it silently weakens the theorem.

Two boundary facts are pinned separately: the rows that hold with
*equality* at the tight minimal instances (`n = 4`, where every slack is
zero, and `n = 3`, where only row 6 keeps a slack of one), and the
negative that the *unguarded* fast/fast row fails at
`fourCrashOnlySlack` while the guarded `FastUniqueness` holds there — the
`f ≥ 1` guard is load-bearing.
-/

namespace HydrozoanTest

open Hydrozoan Hydrozoan.Optimal Hydrozoan.Optimal.ThresholdArithmetic

/-- The seven rows at one instance, as a single decidable proposition. -/
abbrev Rows (Replica : Type) [Fintype Replica] [DecidableEq Replica]
    [OptimalFaults Replica] : Prop :=
  Hydrozoan.ThresholdArithmetic.CertUniqueness Replica ∧
    Hydrozoan.ThresholdArithmetic.AnchorSeesSlow Replica ∧
    Hydrozoan.ThresholdArithmetic.SlowCollectible Replica ∧
    CertFastExclusion Replica ∧ EvidencePlain Replica ∧
    EvidenceEquiv Replica ∧ FastUniqueness Replica

/-- The rows are plain `def`s, so `Decidable` is not synthesized through
them: expose the seven definitions, then decide the arithmetic. -/
macro "decide_rows" : tactic => `(tactic| (
  simp only [Rows, Hydrozoan.ThresholdArithmetic.CertUniqueness,
    Hydrozoan.ThresholdArithmetic.AnchorSeesSlow,
    Hydrozoan.ThresholdArithmetic.SlowCollectible,
    CertFastExclusion, EvidencePlain, EvidenceEquiv, FastUniqueness]
  decide))

-- `Rows` is what `Statement` claims at one type, definitionally — so a
-- conjunct dropped from `Statement` breaks this line, not just the prose.
example :
    Statement ↔ ∀ (R : Type) [Fintype R] [DecidableEq R] [OptimalFaults R], Rows R :=
  Iff.rfl

-- The global instances.
example : Rows (Fin 3) := by decide_rows   -- crash-only, f = 0
example : Rows (Fin 4) := by decide_rows   -- FinWhale's minimal instance
example : Rows (Fin 5) := by decide_rows   -- non-tight n
example : Rows (Fin 6) := by decide_rows   -- c + k odd
example : Rows (Fin 7) := by decide_rows
example : Rows (Fin 20) := by decide_rows

-- The local models.
example : @Rows (Fin 4) _ _ fourCrashOnlySlack := by decide_rows
example : @Rows (Fin 5) _ _ fiveCrashOnly := by decide_rows
example : @Rows (Fin 6) _ _ sixByzantineSlack := by decide_rows
example : @Rows (Fin 14) _ _ fourteenByzantineSlack := by decide_rows   -- k = 10

-- At n = 4, f = 1, c = k = 0 every slack is zero: rows 2, 4, 6 and the
-- fast/fast row hold with equality (`<` met by exactly one).
example :
    qCert (Fin 4) + qFastOpt (Fin 4) = 4 + 1 + 1 ∧
      qCert (Fin 4) = q (Fin 4) ∧
      4 + 1 + tEquiv (Fin 4) = qFastOpt (Fin 4) + q (Fin 4) + 1 ∧
      2 * qFastOpt (Fin 4) = 4 + 1 + 1 := by
  decide

-- At n = 3, f = 0, c = 1 (crash-only) rows 2 and 4 are tight as well;
-- row 6 has slack one — its slack is (n − n_tight) + [c + k odd], and
-- tPlain = tEquiv exactly when that slack is one; and the fast/fast row
-- holds even though its guard is off.
example :
    qCert (Fin 3) + qFastOpt (Fin 3) = 3 + 0 + 1 ∧
      qCert (Fin 3) = q (Fin 3) ∧
      3 + 0 + tEquiv (Fin 3) + 1 = qFastOpt (Fin 3) + q (Fin 3) + 1 ∧
      3 + 0 < 2 * qFastOpt (Fin 3) := by
  decide

-- The guard is load-bearing: at (f, c, k) = (0, 1, 1) the unguarded
-- fast/fast row is false while `FastUniqueness` (guarded) holds.
example :
    ¬ (4 + 0 < 2 * @qFastOpt (Fin 4) _ _ fourCrashOnlySlack) ∧
      @FastUniqueness (Fin 4) _ _ fourCrashOnlySlack := by
  decide_rows

end HydrozoanTest
