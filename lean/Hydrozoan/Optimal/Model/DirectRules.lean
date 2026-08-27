import Hydrozoan.Optimal.Model.Faults
import Hydrozoan.Optimal.Model.Universe

/-!
# Optimal-Hydrozoan: direct decision rules

Trusted core of the Optimal-Hydrozoan arc: the fast commit read with the
new allowance, the per-block *fast evidence* of `sections/optimal-protocol.tex`,
and the direct skip of `sections/optimal-algorithms.tex` (`IsFastEvidence`,
`IsNoFastEvidence`, `SkippedLeader`), as predicates over the block universe
plus their view-relative variants. Definitions only.

Everything else of the direct layer is Hydrozoan's, untouched and reused:
`IsVote`, `voteBlocks`, `IsCertificate`, `supporters`, `SlowCommit`, `blames`
and their in-view forms (`Model/DirectRules.lean`). What changes:

* the fast commit counts to `qFastOpt` instead of `qFast`;
* the weak rung's aggregated vote count is replaced by a property of a
  *single* decision-round block — being fast evidence for a candidate —
  with quorums of such blocks counted where Hydrozoan counted votes;
* the direct skip needs `qCert` blames **and** `qCert` decision-round
  blocks that are fast evidence for no candidate; it is decided at the
  decision round, and its blame quorum is `qCert`, not `qFast`.

Quorums of decision-round blocks are stated **existentially over a witness
set**, as `WeakLinked` is (`Model/IndirectRules.lean`): the block property
quantifies over ids, so a `Finset.filter` in the core would need
decidability the core does not carry. The two forms are equivalent
(`Optimal/Helpers/DirectRules.lean`).

**Fidelity** (decision D6): the paper's `IsNoFastEvidence` quantifies over
the candidates *in the local DAG*; here over the universe. The two agree:
being evidence for `L` references a vote for `L` (`tPlain, tEquiv ≥ 1`),
which references `L`, so any view holding the block holds `L`; and a rival
candidate outside the view has no vote among the block's parents, so the
rival clause is view-insensitive. Argued in prose, recorded here.
-/

namespace Hydrozoan

namespace Optimal

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [O : OptimalFaults Replica]

/-- `L` is fast-committed (the paper's `FastCommittedLeader`, read with the
Optimal allowance): `qFastOpt` votes at the voting round, `r` its propose
round. Two message delays; one vote fewer than Hydrozoan at the same
committee size. -/
def FastCommitOpt (U : BlockUniverse Replica BlockId) (L : BlockId) (r : ℕ) :
    Prop :=
  qFastOpt Replica ≤ (supporters U L (r + 1)).card

/-- Fast commit, as judged from a single view. -/
def FastCommitOptInView (U : BlockUniverse Replica BlockId) (V : View U)
    (L : BlockId) (r : ℕ) : Prop :=
  qFastOpt Replica ≤ (supportersInView U V L (r + 1)).card

/-- The replicas among `C`'s parents whose block votes for `L` — the set
whose cardinality is the paper's `Votes(b, b_leader)` (Algorithm 3). Also
the inner set of Hydrozoan's `IsCertificate`, which is definitionally
`qCert ≤ (votesFor U C L).card`. -/
def votesFor (U : BlockUniverse Replica BlockId) (C L : BlockId) :
    Finset Replica :=
  authorsOf U.block (voteBlocks U C L)

section Slots

variable [S : Slots Replica]

/-- `C` is *fast evidence* for `L` in slot `k` (the paper's
`IsFastEvidence(b, b_leader, w)`, Algorithm 3), by cases on whether `C`
witnesses an equivocation in `k`:

* it does not: `C` references votes for `L` from at least `tPlain`
  replicas;
* it does: at least `tEquiv` for `L`, and fewer than `tEquiv` for every
  other candidate of the slot — so a witnessing block is evidence for at
  most one candidate by construction.

Stated as two implications rather than an `if`: no decidability is needed
in the core. Not restricted to candidates, nor to decision-round blocks:
like the paper's procedure, it may hold of a non-candidate `L` or of a
`C` at any round; every consumer guards — `IsNoFastEvidence` and the
decision relation with `IsLeaderBlock`, the quorum sets with `blocksAt`. -/
def IsFastEvidence (U : BlockUniverse Replica BlockId) (k : ℕ) (C L : BlockId) :
    Prop :=
  (¬ WitnessesEquivocation U k C →                 -- no equivocation witnessed:
    tPlain Replica ≤ (votesFor U C L).card) ∧      --   t_plain votes for L suffice
  (WitnessesEquivocation U k C →                   -- equivocation witnessed:
    tEquiv Replica ≤ (votesFor U C L).card ∧       --   t_equiv votes for L, and
    ∀ L', IsLeaderBlock U k L' → L' ≠ L →          --   every rival candidate
      (votesFor U C L').card < tEquiv Replica)     --   stays below t_equiv

/-- `C` is fast evidence for no candidate of slot `k` (the paper's
`IsNoFastEvidence(b, w)`). Vacuously true when the slot has no candidate
at all — which is what lets a candidate-less slot be skipped. -/
def IsNoFastEvidence (U : BlockUniverse Replica BlockId) (k : ℕ) (C : BlockId) :
    Prop :=
  ∀ L, IsLeaderBlock U k L →                       -- for every candidate of the slot
    ¬ IsFastEvidence U k C L                       -- C is not evidence for it

/-- `qCert` distinct authors of decision-round blocks of slot `k` that are
fast evidence for no candidate — the second half of the paper's
`SkippedLeader`, `noEvidence`. Existential over a witness set of blocks
(see the module docstring). -/
def NoEvidenceQuorum (U : BlockUniverse Replica BlockId) (k : ℕ) : Prop :=
  ∃ s : Finset BlockId,                            -- some set of blocks such that
    (∀ b ∈ s,                                      -- every block in it
      b ∈ blocksAt U (decisionRound Replica k) ∧   -- sits at slot k's decision round
      IsNoFastEvidence U k b) ∧                    -- and is evidence for no candidate;
    qCert Replica ≤ (authorsOf U.block s).card     -- and they come from q_cert authors

/-- Slot `k` is skipped (the paper's `SkippedLeader(w)`, Optimal version):
`qCert` blames at the voting round **and** a no-evidence quorum at the
decision round. Hydrozoan's `blames` is reused (a blame is a voting-round
block referencing no candidate); only the threshold changes, from
`qFast` to `qCert`, and the rule is settled one round later. -/
def SkippedLeaderOpt (U : BlockUniverse Replica BlockId) (k : ℕ) : Prop :=
  qCert Replica ≤ (blames U k).card ∧              -- q_cert blames at the voting round
    NoEvidenceQuorum U k                           -- and q_cert no-evidence decision blocks

/-- The no-evidence quorum a view actually holds. -/
def NoEvidenceQuorumInView (U : BlockUniverse Replica BlockId) (V : View U)
    (k : ℕ) : Prop :=
  ∃ s : Finset BlockId,                            -- some set of blocks such that
    (∀ b ∈ s,                                      -- every block in it
      b ∈ blocksAt U (decisionRound Replica k) ∧   -- sits at slot k's decision round,
      b ∈ V.ids ∧                                  -- is held by the view,
      IsNoFastEvidence U k b) ∧                    -- and is evidence for no candidate;
    qCert Replica ≤ (authorsOf U.block s).card     -- and they come from q_cert authors

/-- Skip, as judged from a single view. -/
def SkippedLeaderOptInView (U : BlockUniverse Replica BlockId) (V : View U)
    (k : ℕ) : Prop :=
  qCert Replica ≤ (blamesInView U V k).card ∧      -- q_cert blames in view
    NoEvidenceQuorumInView U V k                   -- and a no-evidence quorum in view

end Slots

end Optimal

end Hydrozoan
