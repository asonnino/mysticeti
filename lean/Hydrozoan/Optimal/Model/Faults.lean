import Hydrozoan.Model.Faults

/-!
# Optimal-Hydrozoan: fault model and thresholds

Trusted core of the Optimal-Hydrozoan arc: the "Thresholds" paragraph of
`sections/optimal-protocol.tex`. Definitions only — every lemma about
them lives on the generated side (`Optimal/Helpers/`, written with the
later phases).

Optimal-Hydrozoan keeps Hydrozoan's committee (`n ≥ 3f + 2c + k + 1`)
and every threshold of `Model/Faults.lean` except the fast-path
allowance and, through it, the fast quorum: `p` becomes
`pOpt = ⌊(c+k)/2⌋ + 1`, one more fault than
Hydrozoan tolerates in two rounds and exactly Hydrangea's lower bound
`⌊(c+k+2)/2⌋`. The weak-rung threshold `q_weak` is not used; in its place
come two **per-block** thresholds, `tPlain` and `tEquiv`, on the votes
that a single decision-round block references. The quorum `q_cert` is
reused, unchanged, as the quorum of fast-evidence blocks and of
no-evidence blocks (the decision rules of the O4 phase,
`Optimal/Model/DirectRules.lean` and `Optimal/Model/IndirectRules.lean`).

Everything here is layered on the frozen `Faults` class: `OptimalFaults`
*extends* it with one extra field, so `q`, `qCert`, `qSlow`, `Correct`
and `NonByzantine` are inherited as they are — never re-defined.
-/

namespace Hydrozoan

namespace Optimal

/-- The fault model of Optimal-Hydrozoan: Hydrozoan's `Faults` — the same
committee bound `n ≥ 3f + 2c + k + 1`, the same actual fault sets — plus
the paper's standing assumption `f + c ≥ 1`. Under `f = c = 0` the model
is trivial (no fault of any kind), and it is the one configuration in
which the arithmetic of `sections/optimal-proof.tex` (`lem:opt-thresholds`)
is not guaranteed — it fails, for instance, at `n = 1` — so it is
excluded here by construction rather than assumed away in every
statement. -/
class OptimalFaults (Replica : Type*) [Fintype Replica] [DecidableEq Replica]
    extends Faults Replica where
  /-- The fault model is non-trivial: at least one fault of some kind is
  tolerated (`f + c ≥ 1`). -/
  nontrivial : 1 ≤ f + c

section Thresholds

variable (Replica : Type*) [Fintype Replica] [DecidableEq Replica]
  [O : OptimalFaults Replica]

/-- `pOpt = ⌊(c+k)/2⌋ + 1`: the fast path's fault allowance, one more than
Hydrozoan's `p` — defined through it, so the "+1" is definitional. This
is Hydrangea's lower bound `⌊(c+k+2)/2⌋` on two-round commits. -/
def pOpt : ℕ := p Replica + 1

/-- `q_fast = n − pOpt`: the quorum of votes at the voting round to
fast-commit a leader (`FastCommittedLeader`, read with the new `p`). One
vote fewer than Hydrozoan's `q_fast` at the same committee size. Unlike
Hydrozoan, this is **not** the blame quorum of the direct skip, which
becomes `q_cert` (O4, `Optimal/Model/DirectRules.lean`). -/
def qFastOpt : ℕ := Fintype.card Replica - pOpt Replica

/-- `t_plain = n − 2f − c − pOpt`: the votes for a leader block that a
decision-round block must reference to be *fast evidence* for it, when
the block does not witness an equivocation of the leader
(`IsFastEvidence`, first case; O4).

A truncated ℕ subtraction, on purpose: the arithmetic phase (O2,
`Optimal/ThresholdArithmetic`) states the identity
`q_fast + q = n + f + t_plain` as an equality, which fails under
truncation — so the row the seam proof consumes also certifies
that no truncation occurred. -/
def tPlain : ℕ := Fintype.card Replica - (2 * O.f + O.c + pOpt Replica)

/-- `t_equiv = f + pOpt`: the same threshold when the decision-round block
witnesses an equivocation of the leader — it must then reference at
least `t_equiv` votes for the candidate and fewer than `t_equiv` for
every conflicting block (`IsFastEvidence`, second case; O4). -/
def tEquiv : ℕ := O.f + pOpt Replica

end Thresholds

end Optimal

end Hydrozoan
