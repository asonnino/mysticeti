import Hydrozoan.Optimal.DirectLiveness.Proof
import HydrozoanTest.Optimal.PrefixAgreement

/-!
# Witness: Optimal direct liveness, applied

The liveness claims applied to the universes of the safety witnesses —
no new tables:

* `OC`, crash-only on three replicas (`Correct = {1, 2}`, replica `0`
  crashed): the correct pair fills every round and each of its blocks
  references both correct blocks of the round below, so it is
  synchronised from round 0. `holds` commits slot 0 through the slow path
  and skips slot 1, the crashed leader's candidate-less slot, through
  `SkipLiveness`; `fastLatency` fast-commits slot 0 under `pOpt = 1`
  actual fault — where Hydrozoan's premise (`≤ p = 0`) is false: the
  one-more-fault claim on data.
* `OD`, the six-route universe: `SkipLiveness` on its candidate-less
  slots 4 and 5 with `T = {1, 2, 3}`; and `OD` is not `T`-synchronised
  (block 12 omits replica 1's block 8) — the synchrony premise bites.
* `OA`, FinWhale's attack on the skip, the reason `SkipLiveness` is
  restricted to candidate-less slots: the Byzantine leader's candidate is
  blamed by every correct voting-round block, the correct replicas fill
  the decision round — every premise of `SkipLiveness` holds except the
  candidate-less one — yet the slot is **not** skipped, because each
  correct decision-round block references the Byzantine replica's vote
  for the candidate and is thereby fast evidence for it (`tPlain = 1`).

Disclosed residuals, as in Hydrozoan's witness: at `OC` the fault bounds
themselves force `|byzantine ∪ crashed| ≤ 1 = pOpt`, so `fastLatency`'s
fault-count premise is inert there (only its Hydrozoan counterpart
`≤ p = 0` is refuted); and every application has `R = 0 = slotRound 0`,
so `R ≤ slotRound k` is never exercised strictly. A `SkipLiveness`
instance with more actual faults than `pOpt` and a non-synchronised
decision block needs a larger committee (deferred with the other
`n ≥ 5` material).
-/

namespace HydrozoanTest

open Hydrozoan Hydrozoan.Optimal

set_option maxRecDepth 16384

-- Correct = {1, 2}; every round up to 3 is filled by both.
example : (Correct : Finset (Fin 3)) = {1, 2} := by decide
example : Populated UC 0 ∧ Populated UC 1 ∧ Populated UC 2 ∧ Populated UC 3 := by decide

/-- `UC` is synchronised from round 0: rounds stop at 3, so only
`n ∈ {0, 1, 2}` carries an obligation, each decided on the table. -/
theorem uc_synchronised : Synchronised UC 0 := by
  intro n hn b hb hbr hbc a ha har hac
  have hmax : ∀ c : Fin 9, (UC.block c).round ≤ 3 := by decide
  have hb3 := hmax b
  have hn3 : n = 0 ∨ n = 1 ∨ n = 2 := by omega
  clear hb3 hmax hn
  rcases hn3 with rfl | rfl | rfl
  · revert b a; decide
  · revert b a; decide
  · revert b a; decide

-- Commit liveness on slot 0 (leader 2 ∈ Correct): the slow path fires
-- and the verdict is output at the full view.
example :
    ∃ L, IsLeaderBlock UC 0 L ∧ SlowCommit UC L 0 ∧ DecidedOpt OC (View.full UC) 0 (some L) :=
  (Optimal.DirectLiveness.holds (Fin 3) (Fin 9) OC).1 (Correct : Finset (Fin 3)) 0 0
    (by decide) (by decide) uc_synchronised (by decide) (by decide) (by decide) (by decide)
    (by decide)

-- Skip liveness on slot 1 (leader 0, crashed, no candidate): guaranteed
-- by the correct pair alone — no synchrony, no fault-count premise.
-- Hydrozoan's rule would need qFast = 3 blames and never fires here.
example : (∀ L, ¬ IsLeaderBlock UC 1 L) ∧ blames UC 1 = {1, 2} ∧ ¬ SkippedLeader UC 1 := by
  decide
example : SkippedLeaderOpt UC 1 ∧ DecidedOpt OC (View.full UC) 1 none :=
  (Optimal.DirectLiveness.holds (Fin 3) (Fin 9) OC).2 (Correct : Finset (Fin 3)) 1
    (by decide) (by decide) (by decide) (by decide) (by decide)

-- Fast latency: one crashed replica fits pOpt = 1 — and would not fit
-- Hydrozoan's p = 0. (At this configuration the bounds force the fault
-- count to at most 1, so the `≤ pOpt` premise is inert; see the header.)
example :
    (threeReplicasCrashOnly.byzantine ∪ threeReplicasCrashOnly.crashed).card ≤ pOpt (Fin 3) ∧
      ¬ (threeReplicasCrashOnly.byzantine ∪ threeReplicasCrashOnly.crashed).card ≤
        p (Fin 3) := by
  decide
example : ∃ L, IsLeaderBlock UC 0 L ∧ FastCommitOpt UC L 0 :=
  Optimal.DirectLiveness.fastLatency (Fin 3) (Fin 9) OC 0 0 (by decide) uc_synchronised
    (by decide) (by decide) (by decide) (by decide)

-- OD: skip liveness on the candidate-less slots 4 and 5, from the three
-- correct replicas.
example :
    SkippedLeaderOpt UD 4 ∧ DecidedOpt OD (View.full UD) 4 none :=
  (Optimal.DirectLiveness.holds (Fin 4) (Fin 30) OD).2 {1, 2, 3} 4
    (by decide) (by decide) (by decide) (by decide) (by decide)
example :
    SkippedLeaderOpt UD 5 ∧ DecidedOpt OD (View.full UD) 5 none :=
  (Optimal.DirectLiveness.holds (Fin 4) (Fin 30) OD).2 {1, 2, 3} 5
    (by decide) (by decide) (by decide) (by decide) (by decide)

-- The premises bite. OD is not {1, 2, 3}-synchronised: at round 2 → 3,
-- replica 1's block 12 omits replica 1's own round-2 block 8.
example : ¬ SynchronisedOn UD {1, 2, 3} 0 := fun h =>
  absurd (h 2 (by omega) 12 (by decide) (by decide) (by decide) 8 (by decide) (by decide)
    (by decide)) (by decide)

-- FinWhale's attack on the skip: four replicas, replica 0 Byzantine and
-- the leader of slot 1 (round 1). Its candidate 4 is blamed by every
-- correct voting-round block (9, 10, 11); it votes for 4 itself (8); and
-- every correct decision-round block (12, 13, 14) references that vote.

/-- Fifteen blocks over four rounds. Ids 0–3: genesis. Round 1: 4 by `0`
(slot 1's candidate) and 5, 6, 7 by `1`, `2`, `3`, all referencing
`{0, 1, 2}`. Round 2: 8 by `0` references `{4, 5, 6}` — the Byzantine
vote for 4; 9, 10, 11 by `1`, `2`, `3` reference `{5, 6, 7}` — blames.
Round 3: 12, 13, 14 by `1`, `2`, `3` reference `{8, 9, 10}`. -/
def lkA : Fin 15 → Block (Fin 4) (Fin 15) := fun i =>
  if h : (i : ℕ) < 4 then
    { round := 0, author := ⟨i, by omega⟩, parents := ∅ }
  else if h : (i : ℕ) < 8 then
    { round := 1, author := ⟨(i : ℕ) - 4, by omega⟩, parents := {0, 1, 2} }
  else if (i : ℕ) = 8 then
    { round := 2, author := 0, parents := {4, 5, 6} }
  else if h : (i : ℕ) < 12 then
    { round := 2, author := ⟨(i : ℕ) - 8, by omega⟩, parents := {5, 6, 7} }
  else
    { round := 3, author := ⟨(i : ℕ) - 11, by omega⟩, parents := {8, 9, 10} }

/-- The base universe. -/
def UA : BlockUniverse (Fin 4) (Fin 15) where
  ids := Finset.univ
  block := lkA
  complete := by decide
  valid := by decide
  no_equivocation := by decide

/-- ... as an `OptUniverse` (one block per author per round: no
equivocation is witnessed). -/
def OA : OptUniverse (Fin 4) (Fin 15) :=
  { UA with leader_excluded := leaderExcluded_of_noEquivocation UA (by decide) }

-- Every premise of SkipLiveness but the candidate-less one holds for
-- T = {1, 2, 3} at slot 1 ...
example :
    ({1, 2, 3} : Finset (Fin 4)) ⊆ Correct ∧ q (Fin 4) ≤ ({1, 2, 3} : Finset (Fin 4)).card ∧
      PopulatedOn UA {1, 2, 3} 2 ∧ PopulatedOn UA {1, 2, 3} 3 ∧ IsLeaderBlock UA 1 4 := by
  decide

-- ... the correct replicas all blame the candidate, at exactly qCert ...
example : blames UA 1 = {1, 2, 3} ∧ qCert (Fin 4) ≤ (blames UA 1).card := by decide

-- ... yet the slot is not skipped: the single Byzantine vote 8, referenced
-- by every correct decision-round block, makes each of them fast evidence
-- for the candidate, so no no-evidence quorum exists.
example :
    votesFor UA 12 4 = {0} ∧ IsFastEvidence UA 1 12 4 ∧ ¬ IsNoFastEvidence UA 1 12 ∧
      ¬ NoEvidenceQuorum UA 1 ∧ ¬ SkippedLeaderOpt UA 1 := by
  decide

end HydrozoanTest
