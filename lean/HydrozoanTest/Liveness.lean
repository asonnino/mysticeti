import Hydrozoan.Model.Liveness
import HydrozoanTest.SlotAgreement

/-!
# Witness: the liveness hypotheses are satisfiable and biting

Four small universes over the seven-replica model (`Correct = {2,…,6}`,
exactly quorum-sized: q = 5):

* `U6` — the synchronised universe: three rounds, five correct blocks
  each, every block referencing the full correct round below. Validity
  holds *because* of full referencing (five distinct authors = q) —
  the compatibility claim of the `SynchronisedOn` docstring, proved.
  Byzantine 0 and crashed 1 are entirely silent: the hypotheses demand
  nothing of them.
* `U6b` — `U6` plus one Byzantine round-1 block that nobody references:
  `SynchronisedOn` still holds — the `T`-restriction doing visible
  work.
* `U6c` — the frozen-references failure in miniature: one correct
  round-2 block adopts the Byzantine block in place of a correct one
  (the adversary crowding out a slow correct block), and
  `SynchronisedOn` fails.
* `U6d` — a correct replica (3) misses round 1 (a Byzantine block fills
  the validity quorum): `Populated` fails at round 1, while the
  `T`-relative form without replica 3 still holds. The direct-liveness
  theorems require `q ≤ T.card`, so at this tight configuration dropping a
  correct replica exhausts the slack — both the design's tolerance and
  its limit, visible.
-/

namespace HydrozoanTest

open Hydrozoan

set_option maxRecDepth 8192

/-- Fifteen blocks: rounds 0–2 × authors 2–6, each non-genesis block
referencing all five blocks of the round below. -/
def lk6 : Fin 15 → Block (Fin 7) (Fin 15) := fun i =>
  if h : (i : ℕ) < 5 then
    { round := 0, author := ⟨(i : ℕ) % 5 + 2, by omega⟩, parents := ∅ }
  else if h : (i : ℕ) < 10 then
    { round := 1, author := ⟨(i : ℕ) % 5 + 2, by omega⟩,
      parents := {0, 1, 2, 3, 4} }
  else
    { round := 2, author := ⟨(i : ℕ) % 5 + 2, by omega⟩,
      parents := {5, 6, 7, 8, 9} }

/-- The synchronised universe. -/
def U6 : BlockUniverse (Fin 7) (Fin 15) where
  ids := Finset.univ
  block := lk6
  complete := by decide
  valid := by decide
  no_equivocation := by decide

-- All three rounds are fully populated by the correct replicas.
example : Populated U6 0 ∧ Populated U6 1 ∧ Populated U6 2 := by decide

-- (Not decidable as stated — the ∀ over rounds is unbounded — so the
-- proofs below bound the rounds first and decide each case.)
/-- The synchrony hypothesis holds from round 0 (named: consumed by the
end-to-end theorem applications in `HydrozoanTest/DirectLiveness.lean`). -/
theorem u6_synchronised : Synchronised U6 0 := by
  intro n hn b hb hbr hbc a ha har hac
  have hmax : ∀ c : Fin 15, (U6.block c).round ≤ 2 := by decide
  have hb2 := hmax b
  have hn2 : n = 0 ∨ n = 1 := by omega
  clear hb2 hmax hn
  rcases hn2 with rfl | rfl
  · revert b a; decide
  · revert b a; decide

/-- `U6` plus one Byzantine round-1 block (id 15) that nobody
references. -/
def lk6b : Fin 16 → Block (Fin 7) (Fin 16) := fun i =>
  if h : (i : ℕ) < 5 then
    { round := 0, author := ⟨(i : ℕ) % 5 + 2, by omega⟩, parents := ∅ }
  else if h : (i : ℕ) < 10 then
    { round := 1, author := ⟨(i : ℕ) % 5 + 2, by omega⟩,
      parents := {0, 1, 2, 3, 4} }
  else if h : (i : ℕ) < 15 then
    { round := 2, author := ⟨(i : ℕ) % 5 + 2, by omega⟩,
      parents := {5, 6, 7, 8, 9} }
  else
    { round := 1, author := 0, parents := {0, 1, 2, 3, 4} }

def U6b : BlockUniverse (Fin 7) (Fin 16) where
  ids := Finset.univ
  block := lk6b
  complete := by decide
  valid := by decide
  no_equivocation := by decide

-- Nobody references the Byzantine block — pinned, not just prose.
example : ∀ i : Fin 16, (15 : Fin 16) ∉ (U6b.block i).parents := by decide

-- An unreferenced Byzantine block does not break synchrony: both
-- quantifiers are `T`-restricted.
example : Synchronised U6b 0 := by
  intro n hn b hb hbr hbc a ha har hac
  have hmax : ∀ c : Fin 16, (U6b.block c).round ≤ 2 := by decide
  have hb2 := hmax b
  have hn2 : n = 0 ∨ n = 1 := by omega
  clear hb2 hmax hn
  rcases hn2 with rfl | rfl
  · revert b a; decide
  · revert b a; decide

/-- The crowding-out table: correct block 10 (author 2, round 2) adopts
the Byzantine round-1 block 15 in place of the correct block 5 — its
validity quorum is still met (authors 0, 3, 4, 5, 6), but the slow
correct block is frozen out of its references forever. -/
def lk6c : Fin 16 → Block (Fin 7) (Fin 16) := fun i =>
  if (i : ℕ) = 10 then
    { round := 2, author := 2, parents := {6, 7, 8, 9, 15} }
  else lk6b i

def U6c : BlockUniverse (Fin 7) (Fin 16) where
  ids := Finset.univ
  block := lk6c
  complete := by decide
  valid := by decide
  no_equivocation := by decide

-- The frozen-references failure, concretely: correct 10 does not
-- reference correct 5.
example : ¬ Synchronised U6c 0 := fun h =>
  absurd (h 1 (by omega) 10 (by decide) (by decide) (by decide)
    5 (by decide) (by decide) (by decide)) (by decide)

-- The start round `R` is load-bearing: the same universe IS
-- synchronised from round 2 — its only violation sits below the start.
-- (Deleting the `R ≤ n` guard from the definition falsifies this.)
example : Synchronised U6c 2 := by
  intro n hn b hb hbr hbc a ha har hac
  have hmax : ∀ c : Fin 16, (U6c.block c).round ≤ 2 := by decide
  have hb2 := hmax b
  exfalso
  omega

/-- `U6` plus a Byzantine round-0 block (id 16) and a Byzantine
round-1 block (id 15) whose parents **omit the correct round-0 block
id 0**, padding the validity quorum with the Byzantine parent instead
(authors 3, 4, 5, 6, 0 — five distinct). -/
def lk6e : Fin 17 → Block (Fin 7) (Fin 17) := fun i =>
  if h : (i : ℕ) < 5 then
    { round := 0, author := ⟨(i : ℕ) % 5 + 2, by omega⟩, parents := ∅ }
  else if h : (i : ℕ) < 10 then
    { round := 1, author := ⟨(i : ℕ) % 5 + 2, by omega⟩,
      parents := {0, 1, 2, 3, 4} }
  else if h : (i : ℕ) < 15 then
    { round := 2, author := ⟨(i : ℕ) % 5 + 2, by omega⟩,
      parents := {5, 6, 7, 8, 9} }
  else if (i : ℕ) = 15 then
    { round := 1, author := 0, parents := {1, 2, 3, 4, 16} }
  else
    { round := 0, author := 0, parents := ∅ }

def U6e : BlockUniverse (Fin 7) (Fin 17) where
  ids := Finset.univ
  block := lk6e
  complete := by decide
  valid := by decide
  no_equivocation := by decide

-- The Byzantine block genuinely shirks: it omits a correct round-0
-- block from its parents ...
example : (0 : Fin 17) ∉ (U6e.block 15).parents := by decide

-- ... yet synchrony holds: the referencing-side `T`-guard exempts it.
-- (Deleting that guard from the definition falsifies this — the guard
-- is load-bearing, not decorative.)
example : Synchronised U6e 0 := by
  intro n hn b hb hbr hbc a ha har hac
  have hmax : ∀ c : Fin 17, (U6e.block c).round ≤ 2 := by decide
  have hb2 := hmax b
  have hn2 : n = 0 ∨ n = 1 := by omega
  clear hb2 hmax hn
  rcases hn2 with rfl | rfl
  · revert b a; decide
  · revert b a; decide

/-- Correct replica 3 misses round 1 (its slot filled by a Byzantine
block, keeping every validity quorum intact). -/
def lk6d : Fin 15 → Block (Fin 7) (Fin 15) := fun i =>
  if (i : ℕ) = 6 then
    { round := 1, author := 0, parents := {0, 1, 2, 3, 4} }
  else lk6 i

def U6d : BlockUniverse (Fin 7) (Fin 15) where
  ids := Finset.univ
  block := lk6d
  complete := by decide
  valid := by decide
  no_equivocation := by decide

-- `Populated` fails at round 1 — replica 3 has no block there — while
-- the `T`-relative form without replica 3 still holds. (The
-- direct-liveness theorems demand `q ≤ T.card`; at this tight
-- configuration that slack is
-- exhausted, so `U6d` supports no commit through round 1 — tolerance
-- and its limit both visible.)
example : ¬ Populated U6d 1 := by decide
example : PopulatedOn U6d ((Correct : Finset (Fin 7)).erase 3) 1 := by decide

-- The eventual view of the Phase 6 witness is exactly the full view
-- used there.
example : (View.full U5).ids = Vfull5.ids := rfl

end HydrozoanTest
