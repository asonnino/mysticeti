import Hydrozoan.Optimal.Model.Decided
import Hydrozoan.IndirectLiveness.Statement

/-!
# Optimal-Hydrozoan: indirect liveness — the graded rule is total

Hydrozoan's `IndirectLiveness` read over `DecidedOpt`. Direct liveness
commits a synchronised wave's slot; the ledger advances only when *every*
slot below gets a verdict, including slots whose leader was faulty or
whose wave predates synchrony — the indirect rule's job. Two claims:

- `AnchoredTotality`: once a nearest eligible committed anchor exists,
  the three-rung ladder always returns a verdict — a certificate rung
  hit, else an evidence-quorum hit, else a skip. Without the tie-break of
  Hydrozoan's weak rung (decision D3), the evidence rung fires on *any*
  candidate clearing it; that this is at most one candidate is slot
  agreement's business, not totality's.
- `DecidedBelowRun`: a run of `c` consecutive committed slots, long
  enough that its last slot anchors everything below (`SpansEligible`,
  reused from Hydrozoan — a schedule-only notion), decides every slot
  below it.

Both claims are pure decision-relation combinatorics: no synchrony,
population, or fault-count hypothesis appears. The witness models
exercise the run length `c ∈ {3, 4}` under the pipelined schedule only;
a non-pipelined schedule (one slot per wave) satisfies `SpansEligible 1`,
where a single committed slot decides everything below — a residual no
witness pins.

This file imports Hydrozoan's `IndirectLiveness.Statement` for
`SpansEligible`: a reviewed file importing the reviewed file it mirrors,
as the other Optimal statements do (recorded in `lean/CLAUDE.md`).
-/

namespace Hydrozoan

namespace Optimal

namespace IndirectLiveness

open Hydrozoan.IndirectLiveness (SpansEligible)

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [O : OptimalFaults Replica] [S : Slots Replica]

/-- **The graded rule is total.** The premises are verbatim the shared
anchor prefix of the three indirect `DecidedOpt` constructors (minus
`k < j`, which follows from eligibility): an eligible committed anchor
whose eligible in-betweens all skipped. The conclusion: some rung fires
— slot `k` gets a verdict, commit or skip. -/
def AnchoredTotality (U : OptUniverse Replica BlockId) : Prop :=
  ∀ (V : View U.toBlockUniverse) (k j : ℕ) (A : BlockId),
    EligibleAsAnchor Replica k j →       -- j sits ≥ 3 rounds past k,
    DecidedOpt U V j (some A) →          -- slot j committed A,
    (∀ i, k < i → i < j →                -- and j is the NEAREST such slot:
      EligibleAsAnchor Replica k i →     -- every eligible slot in between
      DecidedOpt U V i none) →           -- skipped;
    ∃ v, DecidedOpt U V k v              -- then slot k has a verdict.

/-- **A committed run decides everything below it.** `c` consecutive
committed slots, under the `SpansEligible` runway, force a verdict on
every earlier slot: each such slot anchors on its nearest eligible
committed successor — the run's end if nothing nearer — and the ladder's
totality does the rest. -/
def DecidedBelowRun (U : OptUniverse Replica BlockId) : Prop :=
  ∀ (V : View U.toBlockUniverse) (b c : ℕ),
    0 < c →                              -- a nonempty run (implied by the next
    SpansEligible Replica c →            -- premise; kept for uniformity), long
                                        -- enough to anchor below it,
    (∀ j, b ≤ j → j ≤ b + c - 1 →        -- of committed slots b … b+c−1:
      ∃ B, DecidedOpt U V j (some B)) →
    ∀ i, i < b → ∃ v, DecidedOpt U V i v  -- then every slot below is decided.

/-- Indirect liveness of Optimal-Hydrozoan, over every fault
configuration, schedule, and universe the model admits. -/
def Statement : Prop :=
  ∀ (Replica BlockId : Type) [Fintype Replica] [DecidableEq Replica]
    [DecidableEq BlockId] [OptimalFaults Replica] [Slots Replica]
    (U : OptUniverse Replica BlockId),
    AnchoredTotality U ∧ DecidedBelowRun U

end IndirectLiveness

end Optimal

end Hydrozoan
