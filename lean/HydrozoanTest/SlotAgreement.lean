import Hydrozoan.SlotAgreement.Statement
import Hydrozoan.Helpers.IndirectRules
import HydrozoanTest.DirectRules

/-!
# Witness: the hardening universe

Six rounds under the pipelined schedule of `HydrozoanTest.DirectRules`
(slot `k` at round `k`, leader `(k + 2) % 7`), built to retire every open
verifier finding at once:

* **`ids ⊊ univ`**: `BlockId = Fin 39`, blocks at ids 0–37, id 38 junk
  outside the universe — `complete` finally has something to reject.
* **A genuinely eligible skipped slot between candidate and anchor**:
  slot 3 (round 3, leader 5, candidate id 24) is skipped — no round-4
  block references id 24 — and slot 3 IS eligible to anchor slot 0
  (`2 < 3`). A slot-0 derivation anchored on slot 4 must therefore
  discharge the nearest-anchor in-between premise with a **real**
  `Decided … 3 none` sub-derivation, not vacuously.
* **The anchor**: slot 4 (round 4, leader 6, candidate id 31),
  fast-committed by all six round-5 votes.
* **A voteless slot for `indirectSkip`**: slot 1's candidate id 10
  receives no round-2 votes (round-2 parent sets omit it), so both
  rungs are empty for it at any anchor.

Round-1 keeps the equivocating pair (both copies vote for the slot-0
leader here); every round-2 block certifies the slot-0 leader, so slot 0
resolves via rung 1. Crashed replica 1 authors only its genesis block.
Thresholds at `n = 7`: `q = 5`, `q_fast = 6`, `q_cert = 5`, `q_slow = 4`,
`q_weak = 3`.
-/

namespace HydrozoanTest

open Hydrozoan

set_option maxRecDepth 16384

/-- Thirty-eight blocks over six rounds (id 38 is junk, outside the
universe). Ids 0–6: genesis (author = id). Ids 7/8: the equivocating
pair by Byzantine 0 (both vote for genesis 2). Ids 9–13: round 1 by
replicas 2–6. Ids 14–19 (round 2, authors 0, 2, 3, 4, 5, 6): parents
`{7, 9, 11, 12, 13}` — five distinct authors, all voting for id 2
(every round-2 block is a certificate for the slot-0 leader) and
**omitting id 10** (slot 1's candidate gets no votes). Ids 20–25
(round 3): parents `{14, 15, 16, 18, 19}`, containing certificate 15.
Ids 26–31 (round 4): parents `{20, 21, 22, 23, 25}` — **omitting
id 24**, slot 3's candidate: slot 3 is skipped. Ids 32–37 (round 5):
parents `{26, 27, 28, 29, 31}`, containing the anchor id 31 — six
votes fast-commit slot 4. -/
def lk5 : Fin 39 → Block (Fin 7) (Fin 39) := fun i =>
  if h : (i : ℕ) < 7 then
    { round := 0, author := ⟨i, by omega⟩, parents := ∅ }
  else if (i : ℕ) = 7 then
    { round := 1, author := 0, parents := {0, 1, 2, 3, 4} }
  else if (i : ℕ) = 8 then
    { round := 1, author := 0, parents := {0, 1, 2, 3, 5} }
  else if h : (i : ℕ) < 14 then
    { round := 1, author := ⟨(i : ℕ) - 7, by omega⟩, parents := {0, 1, 2, 3, 4} }
  else if (i : ℕ) = 14 then
    { round := 2, author := 0, parents := {7, 9, 11, 12, 13} }
  else if h : (i : ℕ) < 20 then
    { round := 2, author := ⟨(i : ℕ) - 13, by omega⟩, parents := {7, 9, 11, 12, 13} }
  else if (i : ℕ) = 20 then
    { round := 3, author := 0, parents := {14, 15, 16, 18, 19} }
  else if h : (i : ℕ) < 26 then
    { round := 3, author := ⟨(i : ℕ) - 19, by omega⟩, parents := {14, 15, 16, 18, 19} }
  else if (i : ℕ) = 26 then
    { round := 4, author := 0, parents := {20, 21, 22, 23, 25} }
  else if h : (i : ℕ) < 32 then
    { round := 4, author := ⟨(i : ℕ) - 25, by omega⟩, parents := {20, 21, 22, 23, 25} }
  else if (i : ℕ) = 32 then
    { round := 5, author := 0, parents := {26, 27, 28, 29, 31} }
  else if h : (i : ℕ) < 38 then
    { round := 5, author := ⟨(i : ℕ) - 31, by omega⟩, parents := {26, 27, 28, 29, 31} }
  else
    { round := 0, author := 0, parents := ∅ }

/-- The hardening universe: every id except the junk id 38. -/
def U5 : BlockUniverse (Fin 7) (Fin 39) where
  ids := Finset.univ.erase 38
  block := lk5
  complete := by decide
  valid := by decide
  no_equivocation := by decide

/-- The full view (of the universe — still without the junk id). -/
def Vfull5 : View U5 where
  ids := Finset.univ.erase 38
  subset_ids := by decide
  complete := by decide

/-- A second view, withholding the equivocation's second copy (id 8 is
referenced by nothing, so ref-closure is immediate). -/
def V5b : View U5 where
  ids := (Finset.univ.erase 38).erase 8
  subset_ids := by decide
  complete := by decide

-- The junk id is genuinely outside the universe: `complete` bites on
-- this table (a parent set containing 38 would fail the build).
example : (38 : Fin 39) ∉ U5.ids := by decide

-- The anchor: slot 4's candidate id 31, fast-committed at exact quorum
-- by the six round-5 votes.
example : IsLeaderBlock U5 4 31 := by decide
example : FastCommitInView U5 Vfull5 31 4 := by decide

-- Slot 3 is skipped: its candidate id 24 is referenced by no round-4
-- block, so all six round-4 authors blame it.
example : IsLeaderBlock U5 3 24 := by decide
example : SkippedLeaderInView U5 Vfull5 3 := by decide

-- Eligibility around slot 0: both slot 3 and slot 4 are far enough
-- ahead to anchor it — so a derivation anchored on slot 4 must
-- POSITIVELY dispose of slot 3 in between. Slots 1 and 2 are not
-- eligible.
example : EligibleAsAnchor (Fin 7) 0 4 := by decide
example : EligibleAsAnchor (Fin 7) 0 3 := by decide
example : ¬ EligibleAsAnchor (Fin 7) 0 2 := by decide

-- Slot 0's candidate is certified by every round-2 block (all five of
-- their parents vote for id 2).
example : IsLeaderBlock U5 0 2 := by decide
example : certificates U5 2 0 = {14, 15, 16, 17, 18, 19} := by decide

-- Slot 1's candidate id 10 is voteless: no round-2 block references
-- it, so it has no supporters, no certificates — nothing either rung
-- could ever find.
example : IsLeaderBlock U5 1 10 := by decide
example : supporters U5 10 2 = ∅ := by decide
example : certificates U5 10 1 = ∅ := by decide

-- The second view genuinely differs: the equivocation's second copy is
-- withheld.
example : (7 : Fin 39) ∈ V5b.ids ∧ (8 : Fin 39) ∉ V5b.ids := by decide

-- Slot 3 is decided none — the sub-derivation slot 0's premise needs.
example : Decided U5 Vfull5 3 none := Decided.directSkip (by decide)

-- The seam with a NON-vacuous in-between premise: slot 0 commits via
-- rung 1 anchored on slot 4; the eligible slot 3 in between is
-- positively disposed of by its own skip derivation.
example : Decided U5 Vfull5 0 (some 2) :=
  Decided.indirectCert (j := 4) (A := 31) (by omega) (by decide)
    (Decided.directFast (by decide) (by decide))
    (fun i h1 h2 h3 => by
      have hi : i = 1 ∨ i = 2 ∨ i = 3 := by omega
      rcases hi with rfl | rfl | rfl
      · exact absurd h3 (by decide)
      · exact absurd h3 (by decide)
      · exact Decided.directSkip (by decide))
    (by decide)
    ⟨15, by decide, Reaches.of_mem_parents (i := 31) (j := 20) (by decide)
      (Reaches.single (by decide))⟩

-- indirectSkip end-to-end: slot 1's candidate is voteless, so both
-- rungs are empty at the anchor.
example : Decided U5 Vfull5 1 none := by
  have hall : ∀ M : Fin 39, IsLeaderBlock U5 1 M → M = 10 := by decide
  refine Decided.indirectSkip (j := 4) (A := 31) (by omega) (by decide)
    (Decided.directFast (by decide) (by decide))
    (fun i h1 h2 h3 => by
      have hi : i = 2 ∨ i = 3 := by omega
      rcases hi with rfl | rfl
      · exact absurd h3 (by decide)
      · exact absurd h3 (by decide))
    (fun L hL hcert => by
      have := hall L hL
      subst this
      exact absurd ((certifiedIn_iff_history (by decide)).mp hcert) (by decide))
    (fun L hL hweak => by
      have := hall L hL
      subst this
      exact absurd ((weakLinked_iff_history (by decide)).mp hweak) (by decide))

-- The same slot from a different view, by a different route (V5b still
-- holds six voters, so the direct fast path fires there), same verdict.
example : Decided U5 V5b 0 (some 2) :=
  Decided.directFast (by decide) (by decide)

end HydrozoanTest
