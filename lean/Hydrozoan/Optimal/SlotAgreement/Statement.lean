import Hydrozoan.Optimal.Model.Decided

/-!
# Optimal-Hydrozoan: slot agreement — statement

The headline safety claim for a single slot: **any two verdicts agree** —
across views, across routes. Whether a replica commits via the fast path,
the slow path, the certificate rung, or the evidence rung, and whether
another skips directly or indirectly, two derivable verdicts for one slot
are equal. Undecided replicas assert nothing, so this is
no-conflicting-decision, not termination — Hydrozoan's `SlotAgreement`
read over `DecidedOpt`, minus the tie-break order.

This is the paper's `lem:opt-direct-decision` together with the
indirect/indirect case in one statement — and slightly more:
`lem:opt-indirect-decision` covers two decisions from the *same* anchor,
while this claim also covers two views whose nearest eligible committed
anchors differ (the paper leaves that selection argument in prose).
The proof (generated) consumes: `Optimal.DirectSafety` for the
direct-vs-direct pairings; `AnchorSeesSlow` (rung 1 fires at every
eligible anchor after a slow commit) and `CertUniqueness`; and, for the
fast path, the arc's own seam — a fast commit makes every decision-round
block fast evidence for the committed block (`EvidencePlain`,
`EvidenceEquiv`, the latter through the leader-exclusion clause of
`OptUniverse`), every eligible anchor therefore reaches an evidence quorum
(`SlowCollectible`, `q ≥ qCert`), a decision-round block is evidence for
at most one candidate, and two evidence quorums at one anchor share a
non-Byzantine author (`CertUniqueness`) — which is why the evidence rung
needs no tie-break. A direct skip's `qCert` blames and no-evidence blocks
exclude certificates and evidence quorums the same way.
-/

namespace Hydrozoan

namespace Optimal

namespace SlotAgreement

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [O : OptimalFaults Replica] [S : Slots Replica]

/-- Any two verdicts on one slot agree: across views, across routes
(fast, slow, direct skip, certificate rung, evidence rung, indirect
skip). -/
def DecidedUnique (U : OptUniverse Replica BlockId) : Prop :=
  ∀ (V₁ V₂ : View U.toBlockUniverse) (k : ℕ) (v₁ v₂ : Option BlockId),
    DecidedOpt U V₁ k v₁ → DecidedOpt U V₂ k v₂ → v₁ = v₂

/-- Slot agreement, over every fault configuration, schedule, and
universe the Optimal model admits. -/
def Statement : Prop :=
  ∀ (Replica BlockId : Type) [Fintype Replica] [DecidableEq Replica]
    [DecidableEq BlockId] [OptimalFaults Replica] [Slots Replica]
    (U : OptUniverse Replica BlockId),
    DecidedUnique U

end SlotAgreement

end Optimal

end Hydrozoan
