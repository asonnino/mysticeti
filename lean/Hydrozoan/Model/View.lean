import Hydrozoan.Model.BlockUniverse

/-!
# Views

Trusted core: a **view** is one replica's local DAG — the blocks it has
received, validated, and stored. Views share the universe's lookup
`U.block`, so replicas disagree only about *which* blocks they hold,
never about what an id denotes; validity and non-equivocation are
inherited from `U` unchanged.

Ref-closure is the local reading of the same DAG-building sentence the
universe's `complete` field encodes (`sections/algorithms.tex`): a
replica stores a block only after its entire causal history has been
validated, so everything a stored block references is stored too.

Different correct replicas may hold different views — that asymmetry is
what the cross-view agreement theorem is about. Byzantine withholding is
representable as a block present in the universe but missing from a
view.
-/

namespace Hydrozoan

/-- A view: one replica's local DAG — a subset of the universe that is
closed under references. -/
structure View {Replica BlockId : Type*} [Fintype Replica]
    [DecidableEq Replica] [F : Faults Replica]
    (U : BlockUniverse Replica BlockId) where
  /-- The ids this replica holds. -/
  ids : Finset BlockId
  /-- A view holds only blocks that exist. -/
  subset_ids : ids ⊆ U.ids
  /-- A view is closed downward: it holds everything its blocks
  reference. -/
  complete : ∀ i ∈ ids, ∀ j ∈ (U.block i).parents, j ∈ ids

end Hydrozoan
