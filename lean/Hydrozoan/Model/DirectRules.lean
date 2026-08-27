import Hydrozoan.Model.Slots
import Hydrozoan.Model.View

/-!
# Direct decision rules

Trusted core: votes, certificates, and the three direct rules of
`sections/algorithms.tex` — `FastCommittedLeader`, `SlowCommittedLeader`,
`SkippedLeader` — as predicates over the block universe, plus their
view-relative variants (the rules a replica actually runs on its local
DAG, differing from the universe versions by exactly `∩ V.ids`).

Every rule is a cardinality comparison **counting authors**, never raw
blocks — the pseudocode's "count authors, as replicas may equivocate" —
against the audited thresholds of `Model/Faults.lean`. Universe-level
rules are primary (the safety arithmetic happens there); a view can only
under-report them, never exceed them.

The commit rules are round-parameterized: `r` is the slot's propose
round, and callers pass `S.slotRound k` (the paper's wave `w` maps to
`r = ProposeRound(w)`). Only the skip rule is slot-parameterized,
because blames target the leader slot, not a specific block.

The predicates used inside `Finset.filter` (`IsVote`, `IsCertificate`,
and `IsLeaderBlock` from `Model/Slots.lean`) are `@[reducible]` so their
decidability is inferable; the top-level rules get explicit `Decidable`
instances in `Helpers/`.
-/

namespace Hydrozoan

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [F : Faults Replica]

/-- The blocks of round `r` — the paper's `DAG[r]`, as used by
`GetVotingBlocks` and `GetDecisionBlocks`. -/
def blocksAt (U : BlockUniverse Replica BlockId) (r : ℕ) : Finset BlockId :=
  U.ids.filter fun i => (U.block i).round = r

/-- `b` votes for `L` (the paper's `IsVote`): `L` is among `b`'s parents.

Recall `ValidWrt.distinct_authors`: a well-formed block never references
two blocks by the same author, so `b` votes for at most one copy of any
leader — even an equivocating one.

**Fidelity gap**: the paper defines a vote by deterministic depth-first
traversal — `L` is the first block by its author encountered in `b`'s
causal history. The model uses the direct reference instead. At wave
length 3 the two coincide for the blocks the rules inspect — a leader
copy can only appear among a voter's direct references — and one vote
per author per slot follows from `distinct_authors` plus universe-level
non-equivocation; the DFS ≡ direct-reference equivalence is argued in
prose, not in Lean. Definitionally this is `RefStep`, kept under the
protocol's name. -/
@[reducible]
def IsVote (U : BlockUniverse Replica BlockId) (b L : BlockId) : Prop :=
  L ∈ (U.block b).parents

/-- The parents of `C` that vote for `L` — the inner set of the paper's
`IsCertificate`. -/
def voteBlocks (U : BlockUniverse Replica BlockId) (C L : BlockId) :
    Finset BlockId :=
  (U.block C).parents.filter fun b => IsVote U b L

/-- `C` certifies `L` (the paper's `IsCertificate`): `C`'s votes for `L`
come from `q_cert` distinct authors. -/
@[reducible]
def IsCertificate (U : BlockUniverse Replica BlockId) (C L : BlockId) : Prop :=
  qCert Replica ≤ (authorsOf U.block (voteBlocks U C L)).card

/-- The replicas whose round-`r` block votes for `L`. -/
def supporters (U : BlockUniverse Replica BlockId) (L : BlockId) (r : ℕ) :
    Finset Replica :=
  authorsOf U.block ((blocksAt U r).filter fun b => IsVote U b L)

/-- `L` is fast-committed (the paper's `FastCommittedLeader`): `q_fast`
votes at the voting round, `r` its propose round. Two message delays. -/
def FastCommit (U : BlockUniverse Replica BlockId) (L : BlockId) (r : ℕ) :
    Prop :=
  qFast Replica ≤ (supporters U L (r + 1)).card

/-- The decision-round blocks certifying `L`, `r` its propose round. -/
def certificates (U : BlockUniverse Replica BlockId) (L : BlockId) (r : ℕ) :
    Finset BlockId :=
  (blocksAt U (r + 2)).filter fun C => IsCertificate U C L

/-- The replicas whose decision-round block certifies `L` — the
slow-path counterpart of `supporters`. -/
def certifiers (U : BlockUniverse Replica BlockId) (L : BlockId) (r : ℕ) :
    Finset Replica :=
  authorsOf U.block (certificates U L r)

/-- `L` is slow-committed (the paper's `SlowCommittedLeader`): `q_slow`
certificates at the decision round. Three message delays. -/
def SlowCommit (U : BlockUniverse Replica BlockId) (L : BlockId) (r : ℕ) :
    Prop :=
  qSlow Replica ≤ (certifiers U L r).card

section Skip

variable [S : Slots Replica]

/-- The replicas whose voting-round block blames slot `k`: none of its
parents is a candidate for `k`. Blames target the leader slot, not a
specific block, so a vote for *any* equivocating copy is not a blame. -/
def blames (U : BlockUniverse Replica BlockId) (k : ℕ) : Finset Replica :=
  authorsOf U.block ((blocksAt U (votingRound Replica k)).filter fun b =>
    ∀ j ∈ (U.block b).parents, ¬ IsLeaderBlock U k j)

/-- Slot `k` is skipped (the paper's `SkippedLeader`): `q_fast` blames
at the voting round. Opportunistic — safe whenever it fires, but not
guaranteed to fire. -/
def SkippedLeader (U : BlockUniverse Replica BlockId) (k : ℕ) : Prop :=
  qFast Replica ≤ (blames U k).card

end Skip

section ViewRules

/-- The supporters of `L` a view actually holds. -/
def supportersInView (U : BlockUniverse Replica BlockId) (V : View U)
    (L : BlockId) (r : ℕ) : Finset Replica :=
  authorsOf U.block (((blocksAt U r).filter fun b => IsVote U b L) ∩ V.ids)

/-- Fast commit, as judged from a single view. -/
def FastCommitInView (U : BlockUniverse Replica BlockId) (V : View U)
    (L : BlockId) (r : ℕ) : Prop :=
  qFast Replica ≤ (supportersInView U V L (r + 1)).card

/-- The certificates for `L` a view actually holds. -/
def certificatesInView (U : BlockUniverse Replica BlockId) (V : View U)
    (L : BlockId) (r : ℕ) : Finset BlockId :=
  certificates U L r ∩ V.ids

/-- The certifiers of `L` a view actually holds. -/
def certifiersInView (U : BlockUniverse Replica BlockId) (V : View U)
    (L : BlockId) (r : ℕ) : Finset Replica :=
  authorsOf U.block (certificatesInView U V L r)

/-- Slow commit, as judged from a single view. -/
def SlowCommitInView (U : BlockUniverse Replica BlockId) (V : View U)
    (L : BlockId) (r : ℕ) : Prop :=
  qSlow Replica ≤ (certifiersInView U V L r).card

variable [S : Slots Replica]

/-- The blamers of slot `k` a view actually holds. -/
def blamesInView (U : BlockUniverse Replica BlockId) (V : View U) (k : ℕ) :
    Finset Replica :=
  authorsOf U.block (((blocksAt U (votingRound Replica k)).filter fun b =>
    ∀ j ∈ (U.block b).parents, ¬ IsLeaderBlock U k j) ∩ V.ids)

/-- Skip, as judged from a single view. -/
def SkippedLeaderInView (U : BlockUniverse Replica BlockId) (V : View U)
    (k : ℕ) : Prop :=
  qFast Replica ≤ (blamesInView U V k).card

end ViewRules

end Hydrozoan
