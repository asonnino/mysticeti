import Hydrozoan.Optimal.Model.Faults
import Hydrozoan.ThresholdArithmetic.Statement

/-!
# Optimal-Hydrozoan: threshold arithmetic — statement

The rows of the paper's `lem:opt-thresholds` (`sections/optimal-proof.tex`),
one definition per row, stated over an arbitrary `OptimalFaults` instance.
`Statement` claims the whole table for **every** configuration the class
admits — any `n ≥ 3f + 2c + k + 1` with `f + c ≥ 1`, no cap on `k`.

Three rows are Hydrozoan's own, applied unchanged to the underlying
`Faults` instance (they mention only `q`, `q_cert`, `q_slow`, which the
Optimal variant inherits): `CertUniqueness`, `AnchorSeesSlow`,
`SlowCollectible`. Of these, only `SlowCollectible` carries content
(`n ≥ 3f + 2c + 1`); `CertUniqueness` and `AnchorSeesSlow` are ℕ
tautologies, the latter a weakening of the paper's identity
`q + q_slow = n + f + 1` — both are here so the table is complete, not
because they constrain the Optimal model. Three rows are new and replace
the weak-rung rows `FastStarvation` / `AnchorSeesFast` of the Hydrozoan
table: `CertFastExclusion`, `EvidencePlain`, `EvidenceEquiv`. The last
row, `FastUniqueness`, is Hydrozoan's re-stated over `qFastOpt` with the
paper's `f ≥ 1` guard.

Rows involving subtraction are restated subtraction-free (`a − b ≥ c`
becomes `a ≥ b + c`), so natural-number truncation cannot distort them;
each docstring gives the paper's original form.
-/

namespace Hydrozoan

namespace Optimal

namespace ThresholdArithmetic

variable (Replica : Type*) [Fintype Replica] [DecidableEq Replica]
  [O : OptimalFaults Replica]

/-- **A fast commit starves every conflicting certificate**,
`q_cert + q_fast > n + f` (row 2): the `q_fast` voters of a fast-committed
block and the `q_cert` votes inside any certificate for a conflicting
block overlap in a non-Byzantine replica. Also what makes `q_cert` blames
exclude a fast commit — the Optimal direct skip's blame quorum. Replaces
Hydrozoan's `FastStarvation`, which involved `q_weak`. -/
def CertFastExclusion : Prop :=
  Fintype.card Replica + O.f < qCert Replica + qFastOpt Replica

/-- **Fast evidence without an exposed equivocation**, row 5: the paper's
identity `q_fast + q − n − f = t_plain`, stated as the ℕ equality
`q_fast + q = n + f + t_plain` together with `t_plain ≥ 1`.

The equality is the truncation guard announced on `tPlain`: were the
subtraction in `tPlain` truncated, the two sides could not agree. What
the seam consumes: a decision-round block's `q` parents meet the `q_fast`
voters in at least `q_fast + q − n` replicas, at most `f` of them
Byzantine, leaving `t_plain` non-Byzantine votes for the candidate. -/
def EvidencePlain : Prop :=
  qFastOpt Replica + q Replica = Fintype.card Replica + O.f + tPlain Replica ∧
    1 ≤ tPlain Replica

/-- **Fast evidence with an exposed equivocation**, row 6: the paper's
`q_fast + q − n − f + 1 ≥ t_equiv`, stated subtraction-free as
`n + f + t_equiv ≤ q_fast + q + 1`. The `+ 1` is the leader-exclusion
dividend: a block that witnesses the leader's equivocation does not
reference that leader's block, so at most `f − 1` of its parents are
undetected Byzantine replicas, and votes for the candidate from at least
`t_equiv = f + pOpt` parties remain. This is the row that pins
`n ≥ 3f + c + 2·pOpt − 1`. -/
def EvidenceEquiv : Prop :=
  Fintype.card Replica + O.f + tEquiv Replica ≤ qFastOpt Replica + q Replica + 1

/-- **No two conflicting fast commits**, `2·q_fast > n + f`, guarded by
`f ≥ 1` (the lemma's last claim): two fast quorums overlap in a
non-Byzantine replica. The guard is necessary — at `f = 0` the row can
fail (`HydrozoanTest/Optimal/Thresholds.lean`, `fourCrashOnlySlack`) —
and sufficient for the claim's use: with `f = 0` no replica equivocates,
so a slot holds a single candidate and fast/fast agreement is immediate.

`1 ≤ f` is the paper's exact guard. A silently *stronger* guard
(`2 ≤ f`), or `qFast` in place of `qFastOpt` (one larger, so the row only
gets easier), would keep every witness green: weakenings of a true row
are invisible to `decide`, and reading this line is the only defense. -/
def FastUniqueness : Prop :=
  1 ≤ O.f → Fintype.card Replica + O.f < 2 * qFastOpt Replica

/-- The full table of `lem:opt-thresholds`, for every configuration the
Optimal fault model admits: Hydrozoan's `CertUniqueness`,
`AnchorSeesSlow`, `SlowCollectible` (rows 1, 3, 4, inherited) and the
four Optimal rows. -/
def Statement : Prop :=
  ∀ (Replica : Type) [Fintype Replica] [DecidableEq Replica] [OptimalFaults Replica],
    Hydrozoan.ThresholdArithmetic.CertUniqueness Replica ∧
      Hydrozoan.ThresholdArithmetic.AnchorSeesSlow Replica ∧
      Hydrozoan.ThresholdArithmetic.SlowCollectible Replica ∧
      CertFastExclusion Replica ∧ EvidencePlain Replica ∧
      EvidenceEquiv Replica ∧ FastUniqueness Replica

end ThresholdArithmetic

end Optimal

end Hydrozoan
