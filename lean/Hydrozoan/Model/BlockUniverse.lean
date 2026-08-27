import Hydrozoan.Model.Block

/-!
# The block universe

Trusted core: every block that exists across the whole execution —
authored by anyone, Byzantine, crashed, or correct. Later phases carve
per-replica views out of this universe; the safety theorems quantify
over it.

Here "exists" means **accepted by the DAG-building layer**, not merely
emitted: per `sections/algorithms.tex`, a replica stores a block only
after its entire causal history has been validated, and the decision
rules operate on stored blocks alone. Malformed Byzantine emissions —
dangling parent ids, wrong rounds, duplicate authors — are filtered
before entering any DAG, which is why `complete` and `valid` below hold
for Byzantine-authored blocks too. The Byzantine power that survives the
filter, and that the model does represent, is equivocation and the
adversarial choice of parents, votes, and withholding. The filtering
itself is assumed from Mysticeti, not formalized.

Non-equivocation is stated **here**, at the universe level, rather than
on any individual local DAG. Per-DAG would be too weak: two local DAGs
could each satisfy "at most one block per honest author per round" while
holding *different* such blocks — which is exactly that author
equivocating, with both DAGs looking well-formed.

The guard is `NonByzantine`, not `Correct`: crashed replicas follow the
protocol until they halt — they may author *fewer* blocks (or none), but
never two in one round. Only Byzantine replicas are unconstrained, so
equivocation by them is representable; the witness models exhibit it.
-/

namespace Hydrozoan

/-- Every block that exists, together with the well-formedness conditions
the protocol guarantees. -/
structure BlockUniverse (Replica BlockId : Type*) [Fintype Replica]
    [DecidableEq Replica] [F : Faults Replica] where
  /-- Which blocks exist. -/
  ids : Finset BlockId
  /-- What each id denotes. Total, with junk outside `ids`; every
  hypothesis below quantifies over `i ∈ ids`, so the junk is never
  observed. -/
  block : BlockId → Block Replica BlockId
  /-- Every referenced block is itself present. -/
  complete : ∀ i ∈ ids, ∀ j ∈ (block i).parents, j ∈ ids
  /-- Every block present is valid. -/
  valid : ∀ i ∈ ids, ValidWrt block (block i)
  /-- Non-Byzantine replicas do not equivocate: at most one block per
  author per round. Byzantine replicas are unconstrained. -/
  no_equivocation : ∀ i ∈ ids, ∀ j ∈ ids,
    (block i).author ∈ (NonByzantine : Finset Replica) →
    (block i).author = (block j).author →
    (block i).round = (block j).round → i = j

end Hydrozoan
