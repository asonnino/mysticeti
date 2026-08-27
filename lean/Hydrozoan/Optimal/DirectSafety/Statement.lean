import Hydrozoan.Optimal.Model.Decided
import Hydrozoan.DirectSafety.Statement

/-!
# Optimal-Hydrozoan: direct-rule safety — statement

The direct decision rules of Optimal-Hydrozoan never disagree about a
slot. The same five claims as Hydrozoan's `DirectSafety`, over an
`OptUniverse` and views of its underlying universe: fast/fast,
certificate/certificate, slow/slow, fast/slow, and commit/skip. Two of
them — certificate uniqueness and slow/slow agreement — involve only
rules the arc inherits unchanged, so they are Hydrozoan's own claims
applied to `U.toBlockUniverse`; the other three read the Optimal rules.

Each claim rests on a row of `Optimal/ThresholdArithmetic`:
`FastUniqueness` for fast/fast (guarded by `f ≥ 1`; at `f = 0` no replica
equivocates and a slot holds a single candidate, so agreement is by
non-equivocation); `CertUniqueness` for certificates, for slow/slow, and
for the slow half of commit/skip (the `qCert` blames of the Optimal skip
against the `qCert` votes inside a certificate); `CertFastExclusion` for
fast/slow and for the fast half of commit/skip (the blames against the
`qFastOpt` fast voters).

Commit/skip here is the paper's `lem:opt-commit-excludes-direct-skip`
restricted to *direct* commits; its no-evidence half is never needed
against them (the blames suffice) and only matters against the evidence
rung, which is slot agreement's business.

Statements only; the proofs live in `Proof.lean` (generated).
-/

namespace Hydrozoan

namespace Optimal

namespace DirectSafety

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [O : OptimalFaults Replica] [S : Slots Replica]

/-- **Fast/fast agreement**: two Optimal fast commits for one slot, in any
two views, name the same block. -/
def FastFastAgreement (U : OptUniverse Replica BlockId) : Prop :=
  ∀ (V₁ V₂ : View U.toBlockUniverse) (k : ℕ) (L₁ L₂ : BlockId),
    IsLeaderBlock U.toBlockUniverse k L₁ → IsLeaderBlock U.toBlockUniverse k L₂ →
    FastCommitOptInView U.toBlockUniverse V₁ L₁ (S.slotRound k) →
    FastCommitOptInView U.toBlockUniverse V₂ L₂ (S.slotRound k) → L₁ = L₂

/-- **Certificate uniqueness**: Hydrozoan's claim, on the underlying
universe — certificates are unchanged. -/
def CertUniqueness (U : OptUniverse Replica BlockId) : Prop :=
  Hydrozoan.DirectSafety.CertUniqueness U.toBlockUniverse

/-- **Slow/slow agreement**: Hydrozoan's claim, on the underlying
universe — the slow path is unchanged. -/
def SlowSlowAgreement (U : OptUniverse Replica BlockId) : Prop :=
  Hydrozoan.DirectSafety.SlowSlowAgreement U.toBlockUniverse

/-- **Fast/slow agreement**: an Optimal fast commit and a slow commit for
one slot, across views, name the same block — the `qFastOpt` voters and
the `qCert` votes of any conflicting certificate would share a
non-Byzantine replica. -/
def FastSlowAgreement (U : OptUniverse Replica BlockId) : Prop :=
  ∀ (V₁ V₂ : View U.toBlockUniverse) (k : ℕ) (L₁ L₂ : BlockId),
    IsLeaderBlock U.toBlockUniverse k L₁ → IsLeaderBlock U.toBlockUniverse k L₂ →
    FastCommitOptInView U.toBlockUniverse V₁ L₁ (S.slotRound k) →
    SlowCommitInView U.toBlockUniverse V₂ L₂ (S.slotRound k) → L₁ = L₂

/-- **Commit/skip exclusion**: a slot committed by either direct route in
any view is never directly skipped in any view — the `qCert` blamers and
the committed block's voters would share a non-Byzantine replica, whose
unique voting block cannot both vote and blame. -/
def CommitSkipExclusion (U : OptUniverse Replica BlockId) : Prop :=
  ∀ (V₁ V₂ : View U.toBlockUniverse) (k : ℕ) (L : BlockId),
    IsLeaderBlock U.toBlockUniverse k L →
    (FastCommitOptInView U.toBlockUniverse V₁ L (S.slotRound k) ∨
      SlowCommitInView U.toBlockUniverse V₁ L (S.slotRound k)) →
    ¬ SkippedLeaderOptInView U.toBlockUniverse V₂ k

/-- Slot safety for the Optimal direct rules, over every fault
configuration, schedule, and universe the model admits. -/
def Statement : Prop :=
  ∀ (Replica BlockId : Type) [Fintype Replica] [DecidableEq Replica]
    [DecidableEq BlockId] [OptimalFaults Replica] [Slots Replica]
    (U : OptUniverse Replica BlockId),
    FastFastAgreement U ∧ CertUniqueness U ∧ SlowSlowAgreement U ∧
      FastSlowAgreement U ∧ CommitSkipExclusion U

end DirectSafety

end Optimal

end Hydrozoan
