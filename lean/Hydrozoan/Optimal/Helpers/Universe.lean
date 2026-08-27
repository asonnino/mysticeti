import Hydrozoan.Optimal.Model.Universe
import Hydrozoan.Helpers.Block

/-!
# Optimal-Hydrozoan: universe lemmas

Generated proof infrastructure over `Optimal/Model/Universe.lean`; not
part of the audit surface.

`leaderExcluded_of_bounded` is the bridge the witness models use: the
`leader_excluded` field quantifies over every slot `k`, which `decide`
cannot enumerate, but for a universe whose rounds stop at `N` only slots
with a decision round `≤ N` matter, and a concrete schedule bounds those
slots by some `B` (`B = N` for the pipelined schedule, `B = 2N` with two
slots per round) — so the bounded, decidable form suffices. The
`Decidable` instance lets the witness models check
`WitnessesEquivocation` by `decide` (the existential ranges over a
`Fintype` of ids).
-/

namespace Hydrozoan

namespace Optimal

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [F : Faults Replica] [S : Slots Replica]

omit [DecidableEq BlockId] in
/-- Witnessing an equivocation, read off the parents' parents: the two
candidates are votes' targets, so they sit two references below `b`.
This is the form the witness models decide — a few dozen checks instead
of a quadratic scan of all ids. -/
theorem witnessesEquivocation_iff_parents (U : BlockUniverse Replica BlockId)
    (k : ℕ) (b : BlockId) :
    WitnessesEquivocation U k b ↔
      ∃ j₁ ∈ (U.block b).parents, ∃ L₁ ∈ (U.block j₁).parents,
      ∃ j₂ ∈ (U.block b).parents, ∃ L₂ ∈ (U.block j₂).parents,
        IsLeaderBlock U k L₁ ∧ IsLeaderBlock U k L₂ ∧ L₁ ≠ L₂ := by
  constructor
  · rintro ⟨L₁, L₂, h₁, h₂, hne, ⟨j₁, hj₁, hv₁⟩, ⟨j₂, hj₂, hv₂⟩⟩
    exact ⟨j₁, hj₁, L₁, hv₁, j₂, hj₂, L₂, hv₂, h₁, h₂, hne⟩
  · rintro ⟨j₁, hj₁, L₁, hv₁, j₂, hj₂, L₂, hv₂, h₁, h₂, hne⟩
    exact ⟨L₁, L₂, h₁, h₂, hne, ⟨j₁, hj₁, hv₁⟩, ⟨j₂, hj₂, hv₂⟩⟩

/-- Witnessing an equivocation is decidable, through the parents' form. -/
instance decidableWitnessesEquivocation
    (U : BlockUniverse Replica BlockId) (k : ℕ) (b : BlockId) :
    Decidable (WitnessesEquivocation U k b) :=
  decidable_of_iff _ (witnessesEquivocation_iff_parents U k b).symm

omit [DecidableEq BlockId] in
/-- A universe with no two blocks of one author in one round witnesses no
equivocation anywhere: the two candidates would be such a pair. -/
theorem not_witnessesEquivocation_of_noEquivocation (U : BlockUniverse Replica BlockId)
    (h : ∀ i ∈ U.ids, ∀ j ∈ U.ids, (U.block i).author = (U.block j).author →
      (U.block i).round = (U.block j).round → i = j)
    (k : ℕ) (b : BlockId) : ¬ WitnessesEquivocation U k b := by
  rintro ⟨L₁, L₂, hL₁, hL₂, hne, -, -⟩
  exact hne (h L₁ hL₁.1 L₂ hL₂.1 (hL₁.2.2.trans hL₂.2.2.symm)
    (hL₁.2.1.trans hL₂.2.1.symm))

omit [DecidableEq BlockId] in
/-- In such a universe the leader-exclusion clause holds vacuously — the
cheap route for equivocation-free witness models. -/
theorem leaderExcluded_of_noEquivocation (U : BlockUniverse Replica BlockId)
    (h : ∀ i ∈ U.ids, ∀ j ∈ U.ids, (U.block i).author = (U.block j).author →
      (U.block i).round = (U.block j).round → i = j) :
    ∀ b ∈ U.ids, ∀ k,
      (U.block b).round = decisionRound Replica k →
      WitnessesEquivocation U k b →
      ∀ j ∈ (U.block b).parents, (U.block j).author ≠ S.leader k :=
  fun b _ k _ hw => absurd hw (not_witnessesEquivocation_of_noEquivocation U h k b)

omit [DecidableEq BlockId] in
/-- The leader-exclusion clause follows from its restriction to slots
`k ≤ B`, given that every slot whose decision round is at most `N` has
index at most `B`, and that no block sits above round `N`. -/
theorem leaderExcluded_of_bounded (U : BlockUniverse Replica BlockId) (N B : ℕ)
    (hslot : ∀ k, S.slotRound k + 2 ≤ N → k ≤ B)
    (hround : ∀ b ∈ U.ids, (U.block b).round ≤ N)
    (h : ∀ b ∈ U.ids, ∀ k ≤ B,
      (U.block b).round = decisionRound Replica k →
      WitnessesEquivocation U k b →
      ∀ j ∈ (U.block b).parents, (U.block j).author ≠ S.leader k) :
    ∀ b ∈ U.ids, ∀ k,
      (U.block b).round = decisionRound Replica k →
      WitnessesEquivocation U k b →
      ∀ j ∈ (U.block b).parents, (U.block j).author ≠ S.leader k := by
  intro b hb k hk
  refine h b hb k ?_ hk
  have h1 := hround b hb
  simp only [decisionRound] at hk
  exact hslot k (by omega)

end Optimal

end Hydrozoan
