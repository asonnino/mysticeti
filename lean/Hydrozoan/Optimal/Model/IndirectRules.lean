import Hydrozoan.Optimal.Model.DirectRules
import Hydrozoan.Model.IndirectRules

/-!
# Optimal-Hydrozoan: the graded indirect rule's second rung

Trusted core: the evidence rung of the paper's `DecideFromAnchor`
(`sections/optimal-algorithms.tex`, Algorithm 3). Rung 1 — an anchor-linked
certificate — is Hydrozoan's `CertifiedIn`, reused; anchor eligibility is
Hydrozoan's `EligibleAsAnchor`, reused. Rung 2 replaces `WeakLinked`'s
`q_weak` anchor-linked *votes* by `qCert` anchor-linked decision-round
*blocks*, each fast evidence for the candidate.

Stated existentially over a witness set for the same reason as
`WeakLinked` (`Model/IndirectRules.lean`): filtering on `Reaches` would
require deciding reachability, and the computable surrogate is kept out
of the trusted core. The strict rung ordering lives in the decision
relation (`Optimal/Model/Decided.lean`).
-/

namespace Hydrozoan

namespace Optimal

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [O : OptimalFaults Replica] [S : Slots Replica]

/-- Rung 2's test: `qCert` distinct authors of decision-round blocks of
slot `k`, each fast evidence for `L` and reachable from the anchor `A` —
the paper's `|{b.author : b ∈ B_decision ∧ Link(b, b_anchor) ∧
IsFastEvidence(b, b_leader, w)}| ≥ q_cert`. -/
def EvidenceLinked (U : BlockUniverse Replica BlockId) (A L : BlockId) (k : ℕ) :
    Prop :=
  ∃ s : Finset BlockId,                            -- some set of blocks such that
    (∀ b ∈ s,                                      -- every block in it
      b ∈ blocksAt U (decisionRound Replica k) ∧   -- sits at slot k's decision round,
      IsFastEvidence U k b L ∧                     -- is fast evidence for L,
      Reaches U A b) ∧                             -- and lies in the anchor's history;
    qCert Replica ≤ (authorsOf U.block s).card     -- and they come from q_cert authors

end Optimal

end Hydrozoan
