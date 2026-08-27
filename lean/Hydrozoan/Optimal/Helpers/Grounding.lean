import Hydrozoan.Optimal.Grounding.Statement
import Hydrozoan.Helpers.Grounding
import Hydrozoan.Optimal.Helpers.Universe
import Hydrozoan.Optimal.DirectLiveness.Proof
import Hydrozoan.Optimal.EventualDecision.Proof

/-!
# Optimal-Hydrozoan: grounding — helpers

Generated. The horizon universe of `Hydrozoan/Helpers/Grounding.lean`
lifted to an `OptUniverse` (its leader-exclusion clause holds vacuously:
no author has two blocks in one round), and the capstone composition
over the Optimal liveness theorems.
-/

namespace Hydrozoan

namespace Optimal

namespace Grounding

open Hydrozoan.Grounding

section Universe

variable {Replica : Type*} [Fintype Replica] [DecidableEq Replica]
  [O : OptimalFaults Replica] [S : Slots Replica]

omit S in
/-- No author of the horizon universe has two blocks in one round —
Byzantine or not: block indices are determined by round and author. -/
theorem horizonUniverse_noEquivocation (T : Finset Replica) (hm : 0 < T.card)
    (hq : q Replica ≤ T.card) (N : ℕ) :
    ∀ i ∈ (horizonUniverse T hm hq N).ids, ∀ j ∈ (horizonUniverse T hm hq N).ids,
      ((horizonUniverse T hm hq N).block i).author
        = ((horizonUniverse T hm hq N).block j).author →
      ((horizonUniverse T hm hq N).block i).round
        = ((horizonUniverse T hm hq N).block j).round → i = j := by
  intro i _ j _ hauth hround
  rw [horizonUniverse_block, horizonBlock_author, horizonBlock_author] at hauth
  rw [horizonUniverse_block, horizonBlock_round, horizonBlock_round] at hround
  exact eq_of_div_mod_eq hround (cyclicAuthor_inj T hm hauth)

/-- The horizon universe as an `OptUniverse`: leader exclusion is inert
because nothing equivocates. -/
noncomputable def horizonOptUniverse (T : Finset Replica) (hm : 0 < T.card)
    (hq : q Replica ≤ T.card) (N : ℕ) : OptUniverse Replica ℕ :=
  { horizonUniverse T hm hq N with
    leader_excluded :=
      leaderExcluded_of_noEquivocation _ (horizonUniverse_noEquivocation T hm hq N) }

@[simp] theorem horizonOptUniverse_toBlockUniverse (T : Finset Replica)
    (hm : 0 < T.card) (hq : q Replica ≤ T.card) (N : ℕ) :
    (horizonOptUniverse T hm hq N).toBlockUniverse = horizonUniverse T hm hq N := rfl

end Universe

/-- The realizability conjunct: the lifted horizon universe is `T`-only
and discharges both hypotheses, under whatever schedule is in scope. -/
theorem hypothesesRealizable : HypothesesRealizable := by
  intro Replica _ _ _ _ T N hq
  have hm : 0 < T.card := lt_of_lt_of_le q_pos hq
  exact ⟨horizonOptUniverse T hm hq N,
    horizonUniverse_authors T hm hq N,
    fun r hr => horizonUniverse_populated T hm hq N r hr,
    horizonUniverse_synchronised T hm hq N⟩

/-- The capstone composition, as in Hydrozoan: fairness places a
correct-led wave past `k`, the lifted horizon universe realizes the
hypotheses over its span with `T = Correct`, Optimal direct liveness
commits the wave's first slot, and the Optimal descent settles
everything below it. -/
theorem groundedProgress : GroundedProgress := by
  intro n hn O k
  let S : Slots (Fin n) := waveRobin n hn
  obtain ⟨b, hkb, hlead⟩ := waveRobinFair n hn k
  have hq : q (Fin n) ≤ (Correct : Finset (Fin n)).card := q_le_card_correct
  have hm : 0 < (Correct : Finset (Fin n)).card := lt_of_lt_of_le q_pos hq
  set U : OptUniverse (Fin n) ℕ :=
    horizonOptUniverse (Correct : Finset (Fin n)) hm hq (b + 4)
  have hpop : ∀ r, r ≤ b + 4 →
      PopulatedOn U.toBlockUniverse (Correct : Finset (Fin n)) r :=
    fun r hr => horizonUniverse_populated _ hm hq _ r hr
  have hsync : SynchronisedOn U.toBlockUniverse (Correct : Finset (Fin n)) 0 :=
    horizonUniverse_synchronised _ hm hq _
  have hspan : IndirectLiveness.SpansEligible (Fin n) 3 := by
    intro b' i hi
    change i + 2 < b' + 3 - 1
    omega
  have hleadb : S.leader b ∈ (Correct : Finset (Fin n)) := by
    have := hlead 0 (by omega)
    rwa [Nat.add_zero] at this
  obtain ⟨L, -, -, hL⟩ :=
    (Optimal.DirectLiveness.holds (Fin n) ℕ U).1 (Correct : Finset (Fin n)) 0 b
      Finset.Subset.rfl hq hsync (Nat.zero_le _) (hpop b (by omega))
      (hpop (b + 1) (by omega)) (hpop (b + 2) (by omega)) hleadb
  have hbelow :=
    Optimal.EventualDecision.runDecidesBelow U (Correct : Finset (Fin n)) 0 b 3
      Finset.Subset.rfl hq hsync (by omega) hspan (Nat.zero_le _) hlead
      (fun r _ h2 => hpop r h2)
  exact ⟨b, hkb, U, horizonUniverse_authors _ hm hq _, ⟨L, hL⟩, hbelow⟩

end Grounding

end Optimal

end Hydrozoan
