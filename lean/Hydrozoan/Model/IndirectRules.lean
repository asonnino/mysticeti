import Hydrozoan.Model.DirectRules
import Hydrozoan.Model.CausalHistory

/-!
# The graded indirect rule's ingredients

Trusted core: anchor eligibility and the two rung tests of the paper's
`DecideFromAnchor` (`sections/algorithms.tex`). Rung 1 asks for an
anchor-linked certificate; rung 2 asks for `q_weak` anchor-linked votes —
the fast path's weak footprint, read by the indirect rule. The strict
rung ordering (certificate before weak) is not encoded here; it lives in
the `Decided` relation (`Model/Decided.lean`).

`WeakLinked` is stated **existentially over a witness set of vote
blocks** rather than as a `Finset.filter`: filtering on `Reaches` would
require deciding reachability, and the computable surrogate (`history`)
is deliberately kept out of the trusted core. The two forms are
equivalent (`Helpers/IndirectRules.lean` proves it via the history
bridge); the audited statement mentions only audited notions.
-/

namespace Hydrozoan

section Eligibility

variable (Replica : Type*) [S : Slots Replica]

/-- Slot `j` may anchor slot `k`: `j`'s propose round lies strictly past
`k`'s decision round (the paper's `r_decision < s.round` in
`TryIndirectDecide`) — anchors sit at round ≥ propose + 3. -/
def EligibleAsAnchor (k j : ℕ) : Prop :=
  decisionRound Replica k < S.slotRound j

end Eligibility

section RungTests

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [F : Faults Replica]

/-- Rung 1's test: a certificate for `L` lies in the anchor's causal
history — the paper's `∃ b : Link(b, b_anchor) ∧ IsCertificate(b, b_leader)`,
with `r` the candidate's propose round. -/
def CertifiedIn (U : BlockUniverse Replica BlockId) (A L : BlockId)
    (r : ℕ) : Prop :=
  ∃ C ∈ certificates U L r, Reaches U A C

/-- Rung 2's test: `q_weak` distinct authors of anchor-reachable votes
for `L` at the voting round — the paper's
`|{b.author : Link(b, b_anchor) ∧ IsVote(b, b_leader)}| ≥ q_weak`.

Stated via an explicit witness set of vote blocks (see the module
docstring): some set of voting-round blocks, each voting for `L` and
reachable from the anchor `A`, carries `q_weak` distinct authors. -/
def WeakLinked (U : BlockUniverse Replica BlockId) (A L : BlockId)
    (r : ℕ) : Prop :=
  ∃ s : Finset BlockId,
    (∀ b ∈ s, b ∈ blocksAt U (r + 1) ∧ IsVote U b L ∧ Reaches U A b) ∧
    qWeak Replica ≤ (authorsOf U.block s).card

end RungTests

end Hydrozoan
