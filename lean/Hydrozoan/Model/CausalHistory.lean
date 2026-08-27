import Hydrozoan.Model.BlockUniverse
import Mathlib.Logic.Relation

/-!
# Causal reachability

Trusted core: the paper's `Link` procedure (`sections/algorithms.tex`) —
`Link(b_old, b_new)` holds iff there is a sequence of blocks from
`b_old` to `b_new`, each referenced by the next. `Reaches` is the same
relation with the arguments in walking order:
`Link(b_old, b_new) ↔ Reaches U b_new b_old`, and `Reaches U c b` reads
"`b` lies in the causal history of `c`" — zero or more reference steps
downward. The correspondence is exact for blocks of the universe;
outside `U.ids` the total lookup's junk values are unconstrained, which
is why every lemma about `Reaches` carries a membership hypothesis.

This — direct references and their reflexive-transitive closure, not a
DFS — is the only reachability notion the model uses. No well-founded
recursion is involved: lemmas about `Reaches` induct on the round
number, using the fact that a block's parents sit at a strictly smaller
round (the additive `predecessor` condition).
-/

namespace Hydrozoan

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [F : Faults Replica]

/-- One step of causal history: `j` is directly referenced by `i`. -/
def RefStep (U : BlockUniverse Replica BlockId) (i j : BlockId) : Prop :=
  j ∈ (U.block i).parents

/-- `Reaches U c b` — `b` lies in the causal history of `c`: zero or
more reference steps. The paper's `Link(b, c)`. -/
def Reaches (U : BlockUniverse Replica BlockId) : BlockId → BlockId → Prop :=
  Relation.ReflTransGen (RefStep U)

end Hydrozoan
