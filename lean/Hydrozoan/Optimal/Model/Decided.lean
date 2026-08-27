import Hydrozoan.Optimal.Model.IndirectRules

/-!
# Optimal-Hydrozoan: the decision relation

Trusted core: the per-slot decision procedure of
`sections/optimal-algorithms.tex` — Hydrozoan's `TryDirectDecide` and
`TryIndirectDecide` with the Optimal `SkippedLeader` and `DecideFromAnchor`
— as an inductive relation over an `OptUniverse`. `DecidedOpt U V k (some L)`
says the replica holding view `V` may commit `L` at slot `k`;
`DecidedOpt U V k none` says it may skip the slot; *undecided* is the
absence of any derivation.

As for Hydrozoan's `Decided` (`Model/Decided.lean`): the relation is
**order-free between constructors** — any justifiable verdict is
derivable, and the safety theorems prove the routes never disagree —
while inside the indirect rule the strict grading
`certificate → evidence quorum → skip` **is** encoded, and the anchor is
the **nearest eligible committed** slot.

Two differences with `Decided`. The universe is an `OptUniverse`: only the
decision relation and the safety statements see the leader-exclusion
clause; the rule predicates are applied to `U.toBlockUniverse`. And the
evidence rung carries **no tie-break** (decision D3): two candidates
cannot both clear it — two evidence quorums share a non-Byzantine author
whose unique decision-round block would be evidence for both — so
`argmin digest` of the pseudocode is vacuous; uniqueness is a theorem of
the safety phase, not a premise here. No `LinearOrder BlockId` is needed.
-/

namespace Hydrozoan

namespace Optimal

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [O : OptimalFaults Replica] [S : Slots Replica]

/-- The verdicts a replica holding view `V` may reach on slot `k`. -/
inductive DecidedOpt (U : OptUniverse Replica BlockId) (V : View U.toBlockUniverse) :
    ℕ → Option BlockId → Prop
  /-- The fast path commits a candidate: `qFastOpt` votes in view. -/
  | directFast {k : ℕ} {L : BlockId} :
      IsLeaderBlock U.toBlockUniverse k L →
      FastCommitOptInView U.toBlockUniverse V L (S.slotRound k) →
      DecidedOpt U V k (some L)
  /-- The slow path commits a candidate: `qSlow` certificates in view
  (Hydrozoan's rule, unchanged). -/
  | directSlow {k : ℕ} {L : BlockId} :
      IsLeaderBlock U.toBlockUniverse k L →
      SlowCommitInView U.toBlockUniverse V L (S.slotRound k) →
      DecidedOpt U V k (some L)
  /-- The direct skip: `qCert` blames and a no-evidence quorum in view
  (covers the case of no candidate at all). -/
  | directSkip {k : ℕ} :
      SkippedLeaderOptInView U.toBlockUniverse V k → DecidedOpt U V k none
  /-- Rung 1: anchored on the nearest eligible committed slot, a
  certificate for `L` is in the anchor's reach. -/
  | indirectCert {k j : ℕ} {A L : BlockId} :
      -- the anchor slot lies ahead of k
      k < j →
      -- ... at round ≥ propose + 3
      EligibleAsAnchor Replica k j →
      -- slot j committed A, by any route
      DecidedOpt U V j (some A) →
      -- j is the NEAREST such slot: every eligible slot in between skipped
      -- (an undecided one in between leaves this underivable — the paper's
      -- "stop at the first undecided slot")
      (∀ i, k < i → i < j → EligibleAsAnchor Replica k i → DecidedOpt U V i none) →
      -- L is a candidate for slot k
      IsLeaderBlock U.toBlockUniverse k L →
      -- rung 1: anchor-linked certificate
      CertifiedIn U.toBlockUniverse A L (S.slotRound k) →
      DecidedOpt U V k (some L)
  /-- Rung 2: no candidate has an anchor-linked certificate, and `L` has
  an anchor-linked quorum of fast-evidence blocks. No tie-break. -/
  | indirectEvidence {k j : ℕ} {A L : BlockId} :
      -- the anchor slot lies ahead of k
      k < j →
      -- ... at round ≥ propose + 3
      EligibleAsAnchor Replica k j →
      -- slot j committed A, by any route
      DecidedOpt U V j (some A) →
      -- j is the nearest such slot (as in indirectCert)
      (∀ i, k < i → i < j → EligibleAsAnchor Replica k i → DecidedOpt U V i none) →
      -- rung 1 is empty for EVERY candidate — the strict grading
      (∀ L', IsLeaderBlock U.toBlockUniverse k L' →
        ¬ CertifiedIn U.toBlockUniverse A L' (S.slotRound k)) →
      -- L is a candidate for slot k
      IsLeaderBlock U.toBlockUniverse k L →
      -- rung 2: qCert anchor-linked fast-evidence blocks
      EvidenceLinked U.toBlockUniverse A L k →
      DecidedOpt U V k (some L)
  /-- Rung 3: anchored, and both rungs are empty for every candidate. -/
  | indirectSkip {k j : ℕ} {A : BlockId} :
      -- the anchor slot lies ahead of k
      k < j →
      -- ... at round ≥ propose + 3
      EligibleAsAnchor Replica k j →
      -- slot j committed A, by any route
      DecidedOpt U V j (some A) →
      -- j is the nearest such slot (as in indirectCert)
      (∀ i, k < i → i < j → EligibleAsAnchor Replica k i → DecidedOpt U V i none) →
      -- rung 1 empty for every candidate ...
      (∀ L, IsLeaderBlock U.toBlockUniverse k L →
        ¬ CertifiedIn U.toBlockUniverse A L (S.slotRound k)) →
      -- ... and rung 2 empty for every candidate: only then skip
      (∀ L, IsLeaderBlock U.toBlockUniverse k L →
        ¬ EvidenceLinked U.toBlockUniverse A L k) →
      DecidedOpt U V k none

end Optimal

end Hydrozoan
