import Hydrozoan.Optimal.EventualDecision.Proof
import HydrozoanTest.Optimal.IndirectLiveness

/-!
# Witness: Optimal eventual decision, applied

None of the safety witnesses is `T`-synchronised with a `T`-led run of
three slots under its schedule, so this file adds one steady-state
universe `US` on `fourReplicasOpt` (replica `0` Byzantine and silent
after genesis): the three correct replicas author every round from 1 to
6, each block referencing the three correct blocks of the round below.
Under `fourSlotsOpt` (leader `(k + 3) % 4`) the slots led by `1`, `2`, `3`
with a voting round in the table — `0, 2, 3, 4` — fast-commit at exactly
`qFastOpt`; the slots led by `0` (`1`, `5`) have no candidate; slot 6's
candidate has no voting round and the slot is undecided, so "every slot
below the run" is not an accident of a table that decides everything.

* **`RunDecidesBelow` end-to-end**: the run at slots `2, 3, 4` (`c = 3`,
  `T = {1, 2, 3}`, synchronised from `0` and from `2 = slotRound b`, the
  exact boundary of `R ≤ slotRound b`) decides slots `0` and `1` — and
  the verdicts the descent actually derives (the ladder at the nearest
  eligible committed anchor: rung 1 for slot 0, rung 3 for slot 1) are
  pinned and shown to agree with the direct ones.
* **Synchrony from `R > 0` only**: `US'`, the same table with replica 1's
  round-1 block referencing the Byzantine genesis instead of replica 1's
  own, is *not* synchronised from `0` but is from `1`; the run decides
  slots `0` and `1` from `R = 1`. A strengthening of the synchrony
  premise to round `0` fails here.
* **`RunsRecur` and `ledgerProgress` end-to-end**: the schedule is
  provably fair to `{1, 2, 3}` at `c = 3` (a run starts at every
  `4m + 2`), and a singleton `T` is starved at `c = 2` — fairness is not
  trivially true. `ledgerProgress`'s bound `b` is opaque (existential);
  the concrete guard is the `RunDecidesBelow` application.

Disclosed: at `fourReplicasOpt` every quorum the composition consumes
(`q`, `qFastOpt`, `qCert`, `qSlow`) is `3`, `Correct = {1, 2, 3} = T`, and
every run slot also fast-commits — so strengthenings that exchange one
quorum for another, `T` for `Correct`, or the slow path for the fast one
survive this file; a committee separating the quorums (deferred with the
other `n ≥ 5` material) is needed to kill them.
-/

namespace HydrozoanTest

open Hydrozoan Hydrozoan.Optimal

set_option maxRecDepth 16384

/-- Twenty-two blocks over seven rounds: ids 0–3 genesis; round
`r ≥ 1` holds ids `3r + 1, 3r + 2, 3r + 3` by replicas `1`, `2`, `3`, each
referencing the three correct blocks of round `r − 1`. -/
def lkS : Fin 22 → Block (Fin 4) (Fin 22) := fun i =>
  if h : (i : ℕ) < 4 then
    { round := 0, author := ⟨i, by omega⟩, parents := ∅ }
  else if h : (i : ℕ) < 7 then
    { round := 1, author := ⟨(i : ℕ) - 3, by omega⟩, parents := {1, 2, 3} }
  else if h : (i : ℕ) < 10 then
    { round := 2, author := ⟨(i : ℕ) - 6, by omega⟩, parents := {4, 5, 6} }
  else if h : (i : ℕ) < 13 then
    { round := 3, author := ⟨(i : ℕ) - 9, by omega⟩, parents := {7, 8, 9} }
  else if h : (i : ℕ) < 16 then
    { round := 4, author := ⟨(i : ℕ) - 12, by omega⟩, parents := {10, 11, 12} }
  else if h : (i : ℕ) < 19 then
    { round := 5, author := ⟨(i : ℕ) - 15, by omega⟩, parents := {13, 14, 15} }
  else
    { round := 6, author := ⟨(i : ℕ) - 18, by omega⟩, parents := {16, 17, 18} }

/-- The base universe. -/
def US : BlockUniverse (Fin 4) (Fin 22) where
  ids := Finset.univ
  block := lkS
  complete := by decide
  valid := by decide
  no_equivocation := by decide

/-- ... as an `OptUniverse` (no equivocation anywhere). -/
def OS : OptUniverse (Fin 4) (Fin 22) :=
  { US with leader_excluded := leaderExcluded_of_noEquivocation US (by decide) }

-- The run's candidates: slot 2 → 7 (by 1), slot 3 → 11 (by 2), slot 4 → 15
-- (by 3); slots 1 and 5 have none.
example :
    IsLeaderBlock US 2 7 ∧ IsLeaderBlock US 3 11 ∧ IsLeaderBlock US 4 15 ∧
      (∀ L, ¬ IsLeaderBlock US 1 L) ∧ (∀ L, ¬ IsLeaderBlock US 5 L) := by
  decide

-- Synchronised from round 0 (the round-bounding pattern).
theorem us_synchronised : SynchronisedOn US {1, 2, 3} 0 := by
  intro n hn b hb hbr hbc a ha har hac
  have hmax : ∀ c : Fin 22, (US.block c).round ≤ 6 := by decide
  have hb2 := hmax b
  have hn2 : n = 0 ∨ n = 1 ∨ n = 2 ∨ n = 3 ∨ n = 4 ∨ n = 5 := by omega
  clear hb2 hmax hn
  rcases hn2 with rfl | rfl | rfl | rfl | rfl | rfl <;>
    (revert b a; decide)

-- The correct replicas fill every round of the run's span (rounds 2–6).
theorem us_populated : ∀ r, Slots.slotRound (Replica := Fin 4) 2 ≤ r →
    r ≤ Slots.slotRound (Replica := Fin 4) (2 + 3 - 1) + 2 →
    PopulatedOn US {1, 2, 3} r := by
  intro r h1 h2
  change 2 ≤ r at h1
  change r ≤ 2 + 3 - 1 + 2 at h2
  have : r = 2 ∨ r = 3 ∨ r = 4 ∨ r = 5 ∨ r = 6 := by omega
  rcases this with rfl | rfl | rfl | rfl | rfl <;> decide

-- End-to-end: RunDecidesBelow at b = 2, c = 3, every hypothesis
-- discharged concretely. (Residual, as in Hydrozoan's witness: c = 3
-- also satisfies a `3 ≤ c` strengthening.)
example : ∀ i, i < 2 → ∃ v, DecidedOpt OS (View.full US) i v :=
  ((Optimal.EventualDecision.holds (Fin 4) (Fin 22)).1 OS) {1, 2, 3} 0 2 3
    (by decide) (by decide) us_synchronised (by omega) spansEligible_fourOpt
    (by decide)
    (by intro i hi
        have : i = 0 ∨ i = 1 ∨ i = 2 := by omega
        rcases this with rfl | rfl | rfl <;> decide)
    us_populated

-- The same run at R = 2 = slotRound b — the exact boundary of
-- `R ≤ slotRound b` (synchrony-from-2 restricts from the round-0 fact).
example : ∀ i, i < 2 → ∃ v, DecidedOpt OS (View.full US) i v :=
  ((Optimal.EventualDecision.holds (Fin 4) (Fin 22)).1 OS) {1, 2, 3} 2 2 3
    (by decide) (by decide)
    (fun n _ => us_synchronised n (Nat.zero_le n))
    (by omega) spansEligible_fourOpt (by decide)
    (by intro i hi
        have : i = 0 ∨ i = 1 ∨ i = 2 := by omega
        rcases this with rfl | rfl | rfl <;> decide)
    us_populated

/-- The full view, typed at the projection. -/
def VS : View OS.toBlockUniverse := View.full US

-- What the descent actually derives: the ladder at the nearest eligible
-- committed anchor. Slot 0 commits 3 through rung 1 anchored on slot 3
-- (certificate 7 is a parent of 11); slot 1, candidate-less, is skipped
-- through rung 3 anchored on slot 4 (15).
theorem os_slot0_ladder : DecidedOpt OS VS 0 (some 3) :=
  DecidedOpt.indirectCert (j := 3) (A := 11) (by omega) (by decide)
    (DecidedOpt.directFast (by decide) (by decide))
    (fun i h1 h2 h3 => by
      have hi : i = 1 ∨ i = 2 := by omega
      rcases hi with rfl | rfl
      · exact absurd h3 (by decide)
      · exact absurd h3 (by decide))
    (by decide)
    ⟨7, by decide, Reaches.single (by decide)⟩
theorem os_slot1_ladder : DecidedOpt OS VS 1 none :=
  DecidedOpt.indirectSkip (j := 4) (A := 15) (by omega) (by decide)
    (DecidedOpt.directFast (by decide) (by decide))
    (fun i h1 h2 h3 => by
      have hi : i = 2 ∨ i = 3 := by omega
      rcases hi with rfl | rfl
      · exact absurd h3 (by decide)
      · exact absurd h3 (by decide))
    (fun L hL _ => absurd ⟨L, hL⟩ (by decide : ¬ ∃ L, IsLeaderBlock US 1 L))
    (fun L hL _ => absurd ⟨L, hL⟩ (by decide : ¬ ∃ L, IsLeaderBlock US 1 L))

-- The direct routes reach the same verdicts, and slot agreement says
-- they must: slot 0 fast-commits 3, slot 1 is directly skipped.
example : ∀ v, DecidedOpt OS VS 0 v → v = some 3 := fun v h =>
  (Optimal.SlotAgreement.holds (Fin 4) (Fin 22) OS VS VS 0 _ v os_slot0_ladder h).symm
example : DecidedOpt OS VS 0 (some 3) := DecidedOpt.directFast (by decide) (by decide)
example : DecidedOpt OS VS 1 none := DecidedOpt.directSkip (by decide)

-- Slot 6 has a candidate but no voting round: no direct route, and no
-- anchor above — the table does not decide everything.
example : IsLeaderBlock US 6 19 ∧ supporters US 19 7 = ∅ ∧ ¬ SkippedLeaderOpt US 6 := by
  decide

-- Synchrony from R > 0 only: replica 1's round-1 block references the
-- Byzantine genesis 0 instead of its own genesis 1.

/-- `lkS` with block 4 referencing `{0, 2, 3}`. -/
def lkS' : Fin 22 → Block (Fin 4) (Fin 22) := fun i =>
  if (i : ℕ) = 4 then { round := 1, author := 1, parents := {0, 2, 3} } else lkS i

/-- The base universe. -/
def US' : BlockUniverse (Fin 4) (Fin 22) where
  ids := Finset.univ
  block := lkS'
  complete := by decide
  valid := by decide
  no_equivocation := by decide

/-- ... as an `OptUniverse`. -/
def OS' : OptUniverse (Fin 4) (Fin 22) :=
  { US' with leader_excluded := leaderExcluded_of_noEquivocation US' (by decide) }

-- Not synchronised from round 0 (block 4 omits the T-block 1 of round 0)
-- ...
example : ¬ SynchronisedOn US' {1, 2, 3} 0 := fun h =>
  absurd (h 0 (le_refl 0) 4 (by decide) (by decide) (by decide) 1 (by decide) (by decide)
    (by decide)) (by decide)

-- ... but synchronised from round 1.
theorem us'_synchronised : SynchronisedOn US' {1, 2, 3} 1 := by
  intro n hn b hb hbr hbc a ha har hac
  have hmax : ∀ c : Fin 22, (US'.block c).round ≤ 6 := by decide
  have hb2 := hmax b
  have hn2 : n = 1 ∨ n = 2 ∨ n = 3 ∨ n = 4 ∨ n = 5 := by omega
  clear hb2 hmax hn
  rcases hn2 with rfl | rfl | rfl | rfl | rfl <;>
    (revert b a; decide)

theorem us'_populated : ∀ r, Slots.slotRound (Replica := Fin 4) 2 ≤ r →
    r ≤ Slots.slotRound (Replica := Fin 4) (2 + 3 - 1) + 2 →
    PopulatedOn US' {1, 2, 3} r := by
  intro r h1 h2
  change 2 ≤ r at h1
  change r ≤ 2 + 3 - 1 + 2 at h2
  have : r = 2 ∨ r = 3 ∨ r = 4 ∨ r = 5 ∨ r = 6 := by omega
  rcases this with rfl | rfl | rfl | rfl | rfl <;> decide

-- RunDecidesBelow from R = 1, where round-0 synchrony is false.
example : ∀ i, i < 2 → ∃ v, DecidedOpt OS' (View.full US') i v :=
  ((Optimal.EventualDecision.holds (Fin 4) (Fin 22)).1 OS') {1, 2, 3} 1 2 3
    (by decide) (by decide) us'_synchronised (by omega) spansEligible_fourOpt
    (by decide)
    (by intro i hi
        have : i = 0 ∨ i = 1 ∨ i = 2 := by omega
        rcases this with rfl | rfl | rfl <;> decide)
    us'_populated

-- A fairness negative: consecutive slots never share a leader, so a
-- singleton T is starved at c = 2 — FairRunOn is not trivially true.
example : ¬ Hydrozoan.EventualDecision.FairRunOn (Fin 4) {0} 2 := fun h => by
  obtain ⟨k', -, hl⟩ := h 0
  have h0 : ((k' + 0 + 3) % 4 : ℕ) = 0 :=
    congrArg Fin.val (Finset.mem_singleton.mp (hl 0 (by omega)))
  have h1 : ((k' + 1 + 3) % 4 : ℕ) = 0 :=
    congrArg Fin.val (Finset.mem_singleton.mp (hl 1 (by omega)))
  omega

/-- The schedule is provably fair to the correct set `{1, 2, 3}` at
`c = 3`: a run starts at every `4m + 2` (leaders `1`, `2`, `3`). Proved
from the definition — finite enumeration cannot reach a ∀-over-ℕ claim. -/
theorem fairRun_fourOpt :
    Hydrozoan.EventualDecision.FairRunOn (Fin 4) ({1, 2, 3} : Finset (Fin 4)) 3 := by
  intro k
  refine ⟨4 * k + 2, by omega, fun i hi => ?_⟩
  have hmem : ∀ m : Fin 4, m ≠ 0 → m ∈ ({1, 2, 3} : Finset (Fin 4)) := by decide
  exact hmem _ (Fin.ne_of_val_ne (by
    change (4 * k + 2 + i + 3) % 4 ≠ (0 : Fin 4).val
    omega))

-- End-to-end: RunsRecur applied concretely — fairness places a
-- correct-led run past slot 5 at or after round 3.
example : ∃ b, 5 ≤ b ∧ 3 ≤ Slots.slotRound (Replica := Fin 4) b ∧
    ∀ i, i < 3 → Slots.leader (Replica := Fin 4) (b + i) ∈ ({1, 2, 3} : Finset (Fin 4)) :=
  (Optimal.EventualDecision.holds (Fin 4) (Fin 22)).2 {1, 2, 3} 3 5 3 fairRun_fourOpt

-- End-to-end: the composed headline, all hypotheses discharged.
example : ∃ b, 5 ≤ b ∧ 3 ≤ Slots.slotRound (Replica := Fin 4) b ∧
    ∀ (U : OptUniverse (Fin 4) (Fin 22)),
      SynchronisedOn U.toBlockUniverse {1, 2, 3} 3 →
      (∀ r, Slots.slotRound (Replica := Fin 4) b ≤ r →
        r ≤ Slots.slotRound (Replica := Fin 4) (b + 3 - 1) + 2 →
        PopulatedOn U.toBlockUniverse {1, 2, 3} r) →
      ∀ i, i < b → ∃ v, DecidedOpt U (View.full U.toBlockUniverse) i v :=
  Optimal.EventualDecision.ledgerProgress (Fin 4) (Fin 22) {1, 2, 3} 3 5 3
    (by decide) (by decide) (by omega) spansEligible_fourOpt fairRun_fourOpt

end HydrozoanTest
