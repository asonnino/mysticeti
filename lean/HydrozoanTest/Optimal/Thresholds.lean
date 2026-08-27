import Hydrozoan.Optimal.Model.Faults
import HydrozoanTest.Model
import HydrozoanTest.Thresholds

/-!
# Witness: the Optimal-Hydrozoan fault model and thresholds

Concrete `OptimalFaults` instances, with their threshold tables pinned by
`decide`, so the class is demonstrably satisfiable and the threshold
formulas of `sections/optimal-protocol.tex`, instantiated at each
configuration, take the expected values:

* `Fin 4` with `f = 1, c = k = 0` — FinWhale's minimal instance, the
  first configuration on which Hydrozoan has no usable fast path
  (`p = 0`, unanimity) and Optimal-Hydrozoan commits on 3 of 4 votes;
* `Fin 3` with `f = 0, c = 1, k = 0` — crash-only: the `f = 0` branch of
  the paper's `lem:opt-thresholds`, and the only place `tEquiv = 1`;
* `Fin 7` with `f = c = k = 1`, lifted from `HydrozoanTest.Model`'s
  `sevenReplicas`;
* `Fin 6` with `f = 1, c = 1, k = 0` — `c + k` odd, where the last row of
  the lemma holds with slack (and, as at `Fin 3` and `Fin 5`,
  `tPlain = tEquiv`);
* `Fin 5` with `f = 1, c = k = 0` — the `Fin 4` bounds on a committee one
  above the tight size, so the thresholds are seen moving with `n`
  independently of the bound;
* `Fin 20` with `f = 3, c = 4, k = 2` — the mixed configuration of the
  reference implementation's threshold test (mysticeti, `protocol.rs`),
  where every quorum is distinct;
* three local models (not instances): `(f, c, k) = (1, 0, 2)` on six
  replicas — the paper's headline Byzantine-only shape `n = 3f + 2p − 1`
  with `pOpt = 2` —, `(0, 2, 0)` on five, a second crash-only point, and
  `(1, 0, 10)` on fourteen, a slack beyond Hydrangea's cap.

Each table pins `pOpt = p + 1` and `tPlain ≥ 1`; at `Fin 4` and `Fin 3`,
`tPlain = 1` is the boundary the class admits, so an off-by-one in the
subtrahend of `tPlain` (or `p` in place of `pOpt`) fails there. A local
`(f, c, k) = (0, 1, 1)` model shows the `f ≥ 1` guard on
`2·q_fast > n + f` is necessary, and the `(0, 2, 0)` model that `f = 0`
alone does not break the row. The negative examples at the end show the
`nontrivial` field bites: the class is empty at one and two replicas,
and at three — where `Faults` admits `f = c = 0` with `k = 2` — no
instance has `f = c = 0`.
-/

namespace HydrozoanTest

open Hydrozoan Hydrozoan.Optimal

/-- Four replicas, one Byzantine, no crash, no slack
(`3f + 2c + k + 1 = 4 = n`, tight): the `n = 3f + 2p − 1` instance of
FinWhale at `f = p = 1`. -/
instance fourReplicasOpt : OptimalFaults (Fin 4) where
  f := 1
  c := 0
  k := 0
  byzantine := {0}
  crashed := ∅
  byzantine_disjoint_crashed := by decide
  card_replicas := by decide
  card_byzantine := by decide
  card_crashed := by decide
  nontrivial := by decide

-- The table at n = 4, f = 1, c = k = 0: Hydrozoan's p = 0 (fast commit
-- needs all four votes); Optimal-Hydrozoan's pOpt = 1 (three votes).
-- Every quorum coincides at 3; tPlain = 1 is the boundary.
example :
    p (Fin 4) = 0 ∧ pOpt (Fin 4) = 1 ∧ q (Fin 4) = 3 ∧
      qFast (Fin 4) = 4 ∧ qFastOpt (Fin 4) = 3 ∧
      qCert (Fin 4) = 3 ∧ qSlow (Fin 4) = 3 ∧
      tPlain (Fin 4) = 1 ∧ tEquiv (Fin 4) = 2 := by
  decide

/-- Three replicas, no Byzantine, one crashed, no slack (tight): the
crash-only fault model, `f = 0`. -/
instance threeReplicasCrashOnly : OptimalFaults (Fin 3) where
  f := 0
  c := 1
  k := 0
  byzantine := ∅
  crashed := {0}
  byzantine_disjoint_crashed := by decide
  card_replicas := by decide
  card_byzantine := by decide
  card_crashed := by decide
  nontrivial := by decide

-- The table at n = 3, f = 0, c = 1, k = 0: a two-of-three fast commit;
-- tEquiv = 1, its minimum, and tPlain = 1.
example :
    p (Fin 3) = 0 ∧ pOpt (Fin 3) = 1 ∧ q (Fin 3) = 2 ∧
      qFast (Fin 3) = 3 ∧ qFastOpt (Fin 3) = 2 ∧
      qCert (Fin 3) = 2 ∧ qSlow (Fin 3) = 2 ∧
      tPlain (Fin 3) = 1 ∧ tEquiv (Fin 3) = 1 := by
  decide

/-- Seven replicas, `f = c = k = 1`: `HydrozoanTest.Model.sevenReplicas`
with the non-triviality field added. -/
instance sevenReplicasOpt : OptimalFaults (Fin 7) :=
  { sevenReplicas with nontrivial := by decide }

-- The table at n = 7, f = c = k = 1: pOpt = 2 (was 1), q_fast drops
-- from 6 to 5 and now coincides with q = q_cert = 5.
example :
    p (Fin 7) = 1 ∧ pOpt (Fin 7) = 2 ∧ q (Fin 7) = 5 ∧
      qFast (Fin 7) = 6 ∧ qFastOpt (Fin 7) = 5 ∧
      qCert (Fin 7) = 5 ∧ qSlow (Fin 7) = 4 ∧
      tPlain (Fin 7) = 2 ∧ tEquiv (Fin 7) = 3 := by
  decide

/-- Six replicas, `f = 1, c = 1, k = 0` (tight), built from
`HydrozoanTest.Thresholds.tight`: `c + k` odd. -/
instance sixReplicasOpt : OptimalFaults (Fin 6) :=
  { tight 1 1 0 with nontrivial := by decide }

-- The table at n = 6, f = 1, c = 1, k = 0: with c + k odd, tPlain and
-- tEquiv coincide, and q_fast + q − n − f + 1 = 3 exceeds tEquiv = 2.
example :
    p (Fin 6) = 0 ∧ pOpt (Fin 6) = 1 ∧ q (Fin 6) = 4 ∧
      qFast (Fin 6) = 6 ∧ qFastOpt (Fin 6) = 5 ∧
      qCert (Fin 6) = 4 ∧ qSlow (Fin 6) = 4 ∧
      tPlain (Fin 6) = 2 ∧ tEquiv (Fin 6) = 2 := by
  decide

-- The last row of the lemma, n + f + tEquiv ≤ q_fast + q + 1, holds
-- strictly here: 9 < 10.
example : 6 + 1 + tEquiv (Fin 6) < qFastOpt (Fin 6) + q (Fin 6) + 1 := by decide

/-- Five replicas with the `Fin 4` bounds (`f = 1, c = k = 0`): one
replica above the tight committee size. -/
instance fiveReplicasOpt : OptimalFaults (Fin 5) where
  f := 1
  c := 0
  k := 0
  byzantine := {0}
  crashed := ∅
  byzantine_disjoint_crashed := by decide
  card_replicas := by decide
  card_byzantine := by decide
  card_crashed := by decide
  nontrivial := by decide

-- The table at n = 5, f = 1, c = k = 0: pOpt is unchanged from Fin 4,
-- every n-dependent threshold has moved up by one, tPlain by one too.
example :
    p (Fin 5) = 0 ∧ pOpt (Fin 5) = 1 ∧ q (Fin 5) = 4 ∧
      qFast (Fin 5) = 5 ∧ qFastOpt (Fin 5) = 4 ∧
      qCert (Fin 5) = 4 ∧ qSlow (Fin 5) = 3 ∧
      tPlain (Fin 5) = 2 ∧ tEquiv (Fin 5) = 2 := by
  decide

/-- Twenty replicas, `f = 3, c = 4, k = 2` (tight), built from
`HydrozoanTest.Thresholds.tight`: the mixed configuration of the
reference implementation's threshold test. -/
instance twentyReplicasOpt : OptimalFaults (Fin 20) :=
  { tight 3 4 2 with nontrivial := by decide }

-- The table at n = 20, f = 3, c = 4, k = 2: pOpt = 4 (was 3), q_fast
-- 16 (was 17); the DAG, certificate, slow and evidence thresholds are
-- pairwise distinct.
example :
    p (Fin 20) = 3 ∧ pOpt (Fin 20) = 4 ∧ q (Fin 20) = 13 ∧
      qFast (Fin 20) = 17 ∧ qFastOpt (Fin 20) = 16 ∧
      qCert (Fin 20) = 12 ∧ qSlow (Fin 20) = 11 ∧
      tPlain (Fin 20) = 6 ∧ tEquiv (Fin 20) = 7 := by
  decide

/-- Four replicas, `f = 0, c = 1, k = 1` (tight): a local model, not an
instance (`Fin 4` already carries `fourReplicasOpt`), where
`2·q_fast = n + f` — the `f ≥ 1` guard on the fast/fast row of the
paper's `lem:opt-thresholds` is necessary. Also the first shape with
`q_fast < q_cert`. -/
@[instance_reducible]
def fourCrashOnlySlack : OptimalFaults (Fin 4) :=
  { tight 0 1 1 with nontrivial := by decide }

-- At n = 4, f = 0, c = 1, k = 1: pOpt = 2, q_fast = 2, so
-- 2·q_fast = 4 = n + f, not above it; and q_fast = 2 < q_cert = 3.
example :
    @pOpt (Fin 4) _ _ fourCrashOnlySlack = 2 ∧
      @qFastOpt (Fin 4) _ _ fourCrashOnlySlack = 2 ∧
      @qCert (Fin 4) _ _ fourCrashOnlySlack.toFaults = 3 ∧
      @tPlain (Fin 4) _ _ fourCrashOnlySlack = 1 ∧
      @tEquiv (Fin 4) _ _ fourCrashOnlySlack = 2 := by
  decide

-- The fast/fast row 2·q_fast > n + f fails here (4 = 4) ...
example : ¬ (4 + 0 < 2 * @qFastOpt (Fin 4) _ _ fourCrashOnlySlack) := by decide

/-- Five replicas, `f = 0, c = 2, k = 0` (tight): a second crash-only
local model, where the fast/fast row holds — `f = 0` alone does not
break it. -/
@[instance_reducible]
def fiveCrashOnly : OptimalFaults (Fin 5) :=
  { tight 0 2 0 with nontrivial := by decide }

-- ... but not at every f = 0 point: at (0, 2, 0), 2·q_fast = 6 > 5.
example :
    @pOpt (Fin 5) _ _ fiveCrashOnly = 2 ∧
      @qFastOpt (Fin 5) _ _ fiveCrashOnly = 3 ∧
      @qCert (Fin 5) _ _ fiveCrashOnly.toFaults = 3 ∧
      @tPlain (Fin 5) _ _ fiveCrashOnly = 1 ∧
      @tEquiv (Fin 5) _ _ fiveCrashOnly = 2 ∧
      5 + 0 < 2 * @qFastOpt (Fin 5) _ _ fiveCrashOnly := by
  decide

/-- Six replicas, `f = 1, c = 0, k = 2` (tight): the paper's headline
Byzantine-only shape, `n = 3f + 2·pOpt − 1` with `pOpt = 2` — the only
configuration here where the slack `k` alone raises the allowance. A
local model, since `Fin 6` carries `sixReplicasOpt`. -/
@[instance_reducible]
def sixByzantineSlack : OptimalFaults (Fin 6) :=
  { tight 1 0 2 with nontrivial := by decide }

-- At n = 6, f = 1, c = 0, k = 2: pOpt = 2 and n = 3f + 2·pOpt − 1.
example :
    @p (Fin 6) _ _ sixByzantineSlack.toFaults = 1 ∧
      @pOpt (Fin 6) _ _ sixByzantineSlack = 2 ∧
      @q (Fin 6) _ _ sixByzantineSlack.toFaults = 5 ∧
      @qFastOpt (Fin 6) _ _ sixByzantineSlack = 4 ∧
      @qCert (Fin 6) _ _ sixByzantineSlack.toFaults = 4 ∧
      @qSlow (Fin 6) _ _ sixByzantineSlack.toFaults = 3 ∧
      @tPlain (Fin 6) _ _ sixByzantineSlack = 2 ∧
      @tEquiv (Fin 6) _ _ sixByzantineSlack = 3 ∧
      6 = 3 * 1 + 2 * @pOpt (Fin 6) _ _ sixByzantineSlack - 1 := by
  decide

/-- Fourteen replicas, `f = 1, c = 0, k = 10` (tight): a slack far beyond
Hydrangea's own cap `k ≤ 2f + c − 4`, backing the "no cap on `k`" claim
of the arithmetic statement. `pOpt = 6`, and `n = 3f + 2·pOpt − 1`
again. A local model. -/
@[instance_reducible]
def fourteenByzantineSlack : OptimalFaults (Fin 14) :=
  { tight 1 0 10 with nontrivial := by decide }

-- At n = 14, f = 1, c = 0, k = 10: q_fast = 8 = q_cert, tPlain = 6,
-- tEquiv = 7.
example :
    @pOpt (Fin 14) _ _ fourteenByzantineSlack = 6 ∧
      @q (Fin 14) _ _ fourteenByzantineSlack.toFaults = 13 ∧
      @qFastOpt (Fin 14) _ _ fourteenByzantineSlack = 8 ∧
      @qCert (Fin 14) _ _ fourteenByzantineSlack.toFaults = 8 ∧
      @qSlow (Fin 14) _ _ fourteenByzantineSlack.toFaults = 3 ∧
      @tPlain (Fin 14) _ _ fourteenByzantineSlack = 6 ∧
      @tEquiv (Fin 14) _ _ fourteenByzantineSlack = 7 ∧
      14 = 3 * 1 + 2 * @pOpt (Fin 14) _ _ fourteenByzantineSlack - 1 := by
  decide

/-- The trivial fault model: one replica, no fault of any kind. A valid
`Faults` instance (`3·0 + 2·0 + 0 + 1 = 1 ≤ 1`) ... -/
@[instance_reducible]
def oneReplicaTrivial : Faults (Fin 1) where
  f := 0
  c := 0
  k := 0
  byzantine := ∅
  crashed := ∅
  byzantine_disjoint_crashed := by decide
  card_replicas := by decide
  card_byzantine := by decide
  card_crashed := by decide

-- ... but no `OptimalFaults` extends it: the `nontrivial` field bites.
example : ¬ ∃ O : OptimalFaults (Fin 1), O.toFaults = oneReplicaTrivial := by
  rintro ⟨O, h⟩
  have hn := O.nontrivial
  rw [h] at hn
  exact absurd hn (by decide)

-- In fact the class is empty at one and at two replicas: the committee
-- bound forces f = c = 0 there, and `nontrivial` then has no instance.
example : IsEmpty (OptimalFaults (Fin 1)) :=
  ⟨fun O => by
    have h1 := O.card_replicas
    have h2 := O.nontrivial
    rw [Fintype.card_fin] at h1
    omega⟩

example : IsEmpty (OptimalFaults (Fin 2)) :=
  ⟨fun O => by
    have h1 := O.card_replicas
    have h2 := O.nontrivial
    rw [Fintype.card_fin] at h1
    omega⟩

/-- Three replicas, `f = c = 0, k = 2` (tight): a valid `Faults`, with
slack but no fault ... -/
@[instance_reducible]
def threeReplicasTrivialSlack : Faults (Fin 3) := tight 0 0 2

-- ... that no `OptimalFaults` extends: the field reads `f + c`, not
-- `k`. Stated both against this instance and for every `f = c = 0`.
example : ¬ ∃ O : OptimalFaults (Fin 3), O.toFaults = threeReplicasTrivialSlack := by
  rintro ⟨O, h⟩
  have hn := O.nontrivial
  rw [h] at hn
  exact absurd hn (by decide)

example : ¬ ∃ O : OptimalFaults (Fin 3), O.f = 0 ∧ O.c = 0 := by
  rintro ⟨O, hf, hc⟩
  have h := O.nontrivial
  omega

end HydrozoanTest
