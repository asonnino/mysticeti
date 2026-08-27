import Hydrozoan.Model.Faults

/-!
# Blocks and block validity

Trusted core: the block structure and validity conditions of the paper's
"DAG-building layer" paragraph (`sections/algorithms.tex`): replicas
proceed in logical rounds, and every block references at least `q`
distinct valid blocks from the immediately preceding round.

Blocks reference other blocks by `BlockId`, never by value, so `Block` is
non-recursive. A `BlockId` means nothing on its own: it is resolved
through a total lookup function `blk : BlockId → Block`, and validity is
therefore a predicate on `(blk, b)` rather than on `b` alone. This is
deliberate — taking the lookup function rather than a whole universe is
what lets `BlockUniverse` state "every member is valid" as one of its own
fields without referring to the structure being defined. Acyclicity is a
consequence of the predecessor condition (every reference sits at a
strictly smaller round), not a structural constraint; no well-founded
recursion is ever needed.

**Fidelity gap** (stated once, here): parents point only to the
immediately preceding round — the model has no weak links.
-/

namespace Hydrozoan

/-- A block: its round, its author, and the ids of the blocks it
references from the preceding round. `BlockId` is the block's identity —
two blocks by the same author in the same round (equivocation) are simply
two distinct ids. -/
structure Block (Replica BlockId : Type*) where
  /-- The round this block was produced in. -/
  round : ℕ
  /-- The replica that authored the block. -/
  author : Replica
  /-- Ids of the blocks this one references, all from the preceding
  round. -/
  parents : Finset BlockId

section Authors

variable {Replica BlockId : Type*} [DecidableEq Replica]

/-- The replicas that authored a set of ids, resolved through `blk`.
Defined on an arbitrary `Finset BlockId`, not just on a block's parents:
later counting hypotheses quantify over id-sets that are nobody's
parents. -/
def authorsOf (blk : BlockId → Block Replica BlockId) (s : Finset BlockId) :
    Finset Replica :=
  s.image fun i => (blk i).author

/-- The replicas behind a block's parents. -/
def authors (blk : BlockId → Block Replica BlockId)
    (b : Block Replica BlockId) : Finset Replica :=
  authorsOf blk b.parents

end Authors

section Validity

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [F : Faults Replica]

/-- Block validity, relative to a lookup function — the paper's
"referencing `≥ q` distinct valid blocks from the previous round".

The predecessor condition is additive (`+ 1 =`, never `− 1`): this avoids
natural-number subtraction, and it makes the genesis case derivable
rather than assumed — at round `0` the equation `(blk i).round + 1 = 0`
is unsatisfiable, so `parents = ∅` follows. Only the quorum condition
needs a round guard.

The quorum counts **authors**, not `parents.card`: the protocol means `q`
distinct *replicas*' blocks, and the author-set form is what every
counting argument consumes (with `distinct_authors` the two coincide). -/
structure ValidWrt (blk : BlockId → Block Replica BlockId)
    (b : Block Replica BlockId) : Prop where
  /-- Every parent sits in the immediately preceding round. -/
  predecessor : ∀ i ∈ b.parents, (blk i).round + 1 = b.round
  /-- A block never references the same author twice. -/
  distinct_authors : ∀ i ∈ b.parents, ∀ j ∈ b.parents,
    (blk i).author = (blk j).author → i = j
  /-- Non-genesis blocks reference a DAG quorum of distinct authors. -/
  quorum : 0 < b.round → q Replica ≤ (authors blk b).card

end Validity

end Hydrozoan
