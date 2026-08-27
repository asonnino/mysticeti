import Hydrozoan.Model.Faults

/-!
# Threshold arithmetic — statement

The six slack-cap inequalities: every inequality the two-protocol
consistency argument rests on, one definition per table row, stated over an arbitrary
`Faults` instance.

Hydrangea's Theorem 1 caps the slack (`k ≤ 2f + c − 4` for even `c`,
`k ≤ 2f + c − 3` for odd `c`); the `Faults` class only assumes
`n ≥ 3f + 2c + k + 1`. `Statement` therefore claims the whole table for
**every** fault configuration the class admits — in particular for every
`k ≥ 0` — machine-checking that nothing in the DAG argument needs the cap.

Rows involving subtraction are restated subtraction-free (`a − b ≥ c`
becomes `a ≥ b + c`), so natural-number truncation cannot distort them;
each docstring gives the note's original form.
-/

namespace Hydrozoan

namespace ThresholdArithmetic

variable (Replica : Type*) [Fintype Replica] [DecidableEq Replica] [F : Faults Replica]

/-- **Certificate uniqueness**, `2·q_cert > n + f`: two certificate vote
sets must overlap in a non-Byzantine replica, so no two conflicting blocks
are both certified in the same slot. -/
def CertUniqueness : Prop :=
  Fintype.card Replica + F.f < 2 * qCert Replica

/-- **No two conflicting fast commits**, `2·q_fast > n + f`: two fast
quorums must overlap in a non-Byzantine replica, so no two conflicting
leaders are both fast-committed. -/
def FastUniqueness : Prop :=
  Fintype.card Replica + F.f < 2 * qFast Replica

/-- **A fast commit starves conflicts below the weak rung**,
`q_fast + q_weak > n + f`: once a leader gathers `q_fast` votes, a
conflicting candidate's support is at most `(n − q_fast) + f = f + p`,
strictly below `q_weak` — the graded indirect rule can never resurrect
it. -/
def FastStarvation : Prop :=
  Fintype.card Replica + F.f < qFast Replica + qWeak Replica

/-- **The slow path is collectible**, `q_cert ≤ q`: a decision-round block
references `q` parents, so a certificate's `q_cert` votes fit among them —
the certificate threshold never outruns what a single block can carry. -/
def SlowCollectible : Prop :=
  qCert Replica ≤ q Replica

/-- **An anchor sees any slow commit**, `q + q_slow > n + f` (the note's
identity `Q + SLOW = n + f + 1`): an anchor's `q` parents meet the
`q_slow` certificates of any slow commit in a non-Byzantine replica. -/
def AnchorSeesSlow : Prop :=
  Fintype.card Replica + F.f < q Replica + qSlow Replica

/-- **An anchor sees the fast footprint**: a fast quorum (`q_fast`
voters) and an anchor's parent set (`q` authors) always intersect in at
least `q_weak` replicas — and that intersection is exactly the
anchor-linked votes the graded indirect rule counts. So for a
fast-committed leader every anchor reaches at least the weak rung, and
can never indirect-skip it.

Counting: two sets of sizes `q_fast` and `q` among `n` replicas share at
least `q_fast + q − n` members, so the requirement is
`q_fast + q − n ≥ q_weak` (the note's form) — stated subtraction-free
below as `n + q_weak ≤ q_fast + q`. -/
def AnchorSeesFast : Prop :=
  Fintype.card Replica + qWeak Replica ≤ qFast Replica + q Replica

/-- The full slack-cap table, for every fault configuration the model
admits — no analogue of Hydrangea's Theorem 1 slack cap is assumed. -/
def Statement : Prop :=
  ∀ (Replica : Type) [Fintype Replica] [DecidableEq Replica] [Faults Replica],
    CertUniqueness Replica ∧ FastUniqueness Replica ∧ FastStarvation Replica ∧
      SlowCollectible Replica ∧ AnchorSeesSlow Replica ∧ AnchorSeesFast Replica

end ThresholdArithmetic

end Hydrozoan
