import Hydrozoan.EventualDecision.Proof
import HydrozoanTest.IndirectLiveness

/-!
# Witness: the liveness hardening batch

Three configurations, each killing a mutation class the indirect- and
eventual-decision phases' cold audits found unguarded:

* **A proper-subset `T`** (`U12`, eight replicas): a configuration whose
  ACTUAL faults undershoot the bounds — one Byzantine, nobody crashed —
  so `|Correct| = 7 > q = 6`, and the liveness theorem is applied at a
  six-member `T` that excludes the correct replica 7, which authors one
  genesis block nobody references and then goes silent. Every previous
  application used `T = Correct`; this one kills the silent
  strengthenings `T ⊆ Correct → T = Correct` and
  population/synchrony-on-`T` → on-all-of-`Correct`: both strengthened
  hypotheses are genuinely FALSE at `U12` (pinned) while their
  `T`-relative forms hold.
* **Route diversification** (`U13`, the frozen seven-replica
  configuration): one table where the three below-run slots resolve by
  three different routes — slot 0 by the certificate rung (five votes =
  `q_cert` < `q_fast`, exactly one certificate, one blame), slot 1
  directly, slot 2 by the indirect skip (two votes: below `q_weak`, yet
  starving the blame quorum). The indirect pair is exclusive: for slots
  0 and 2 the pinned route is provably the ONLY one available in any
  view; slot 1 is deliberately multi-route. The anchors diversify too:
  slot 0's anchor is the development's first SLOW-committed anchor
  (five supporters < `q_fast`, six certifiers ≥ `q_slow`).
* **A sparse schedule** (`U14`, six replicas, three-round spacing): the
  first non-pipelined schedule. With three rounds between slots,
  `SpansEligible` holds already at `c = 1`, and the descent fires below
  a SINGLE committed slot — killing the `0 < c → 3 ≤ c` strengthening
  disclosed by the descent and run witnesses, and pinning that the
  run-length
  requirement is a property of the schedule's density, not of the
  theorem.
-/

namespace HydrozoanTest

open Hydrozoan

set_option maxRecDepth 16384

-- ## A proper-subset T (U12, Fin 8: f = 1, c = 1, k = 2)

/-- Actual faults under the bounds: one Byzantine replica, NOBODY
crashed (the crash budget `c = 1` is unspent), so seven replicas are
correct — one more than the quorum `q = 6`. -/
instance eightReplicas : Faults (Fin 8) where
  f := 1
  c := 1
  k := 2
  byzantine := {0}
  crashed := ∅
  byzantine_disjoint_crashed := by decide
  card_replicas := by decide
  card_byzantine := by decide
  card_crashed := by decide

/-- Pipelined single-leader schedule on eight replicas; slot 0 is led
by replica 1 (correct, and a member of `T` below). -/
instance : Slots (Fin 8) :=
  Slots.uniformSingle 1 (by omega) fun k => ⟨(k + 1) % 8, by omega⟩

-- The threshold table at n = 8, f = 1, c = 1, k = 2.
example : p (Fin 8) = 1 ∧ q (Fin 8) = 6 ∧ qFast (Fin 8) = 7 ∧
    qCert (Fin 8) = 5 ∧ qSlow (Fin 8) = 4 ∧ qWeak (Fin 8) = 3 := by decide

-- T is a PROPER subset of the correct pool at exactly quorum size:
-- replica 7 is correct but excluded.
example : ({1, 2, 3, 4, 5, 6} : Finset (Fin 8)) ⊆
      (Correct : Finset (Fin 8)) ∧
    (7 : Fin 8) ∈ (Correct : Finset (Fin 8)) ∧
    (7 : Fin 8) ∉ ({1, 2, 3, 4, 5, 6} : Finset (Fin 8)) ∧
    ({1, 2, 3, 4, 5, 6} : Finset (Fin 8)).card = q (Fin 8) := by decide

/-- Nineteen blocks: rounds 0–2 × the six members of `T` (authors
1–6), plus id 18 — the correct replica 7's genesis block, which no
later block references and whose author then falls silent. The theorem
must fire without replica 7; the strengthened all-of-`Correct`
hypotheses must fail because of it. -/
def lk12 : Fin 19 → Block (Fin 8) (Fin 19) := fun i =>
  if h : (i : ℕ) < 6 then
    { round := 0, author := ⟨(i : ℕ) + 1, by omega⟩, parents := ∅ }
  else if h : (i : ℕ) < 12 then
    { round := 1, author := ⟨(i : ℕ) - 5, by omega⟩,
      parents := {0, 1, 2, 3, 4, 5} }
  else if h : (i : ℕ) < 18 then
    { round := 2, author := ⟨(i : ℕ) - 11, by omega⟩,
      parents := {6, 7, 8, 9, 10, 11} }
  else
    { round := 0, author := ⟨7, by omega⟩, parents := ∅ }

/-- The proper-subset-`T` universe. -/
def U12 : BlockUniverse (Fin 8) (Fin 19) where
  ids := Finset.univ
  block := lk12
  complete := by decide
  valid := by decide
  no_equivocation := by decide

-- The strengthened hypotheses are FALSE here, not merely unproven:
-- population on all of Correct fails from round 1 (replica 7 authored
-- only its genesis), and synchrony on all of Correct fails outright
-- (id 6, a round-1 T-block, omits the Correct-authored id 18) — while
-- the T-relative forms hold. This is what makes the application below
-- a genuine kill, not a coincidence.
example : PopulatedOn U12 (Correct : Finset (Fin 8)) 0 ∧
    ¬ PopulatedOn U12 (Correct : Finset (Fin 8)) 1 := by decide
example : ¬ SynchronisedOn U12 (Correct : Finset (Fin 8)) 0 := fun h =>
  absurd
    (h 0 (le_refl 0) 6 (by decide) (by decide) (by decide)
      18 (by decide) (by decide) (by decide))
    (by decide)
example : PopulatedOn U12 ({1, 2, 3, 4, 5, 6} : Finset (Fin 8)) 0 ∧
    PopulatedOn U12 ({1, 2, 3, 4, 5, 6} : Finset (Fin 8)) 1 ∧
    PopulatedOn U12 ({1, 2, 3, 4, 5, 6} : Finset (Fin 8)) 2 := by decide

theorem u12_synchronised :
    SynchronisedOn U12 ({1, 2, 3, 4, 5, 6} : Finset (Fin 8)) 0 := by
  intro n hn b hb hbr hbc a ha har hac
  have hmax : ∀ c : Fin 19, (U12.block c).round ≤ 2 := by decide
  have hb2 := hmax b
  have hn2 : n = 0 ∨ n = 1 := by omega
  clear hb2 hmax hn
  rcases hn2 with rfl | rfl <;> (revert b a; decide)

-- Slot 0 slow-commits at T: six supporters < q_fast = 7, but all six
-- decision-round blocks certify.
example : IsLeaderBlock U12 0 0 := by decide
example : supporters U12 0 1 = {1, 2, 3, 4, 5, 6} := by decide
example : ¬ FastCommit U12 0 0 ∧ SlowCommit U12 0 0 := by decide

-- End-to-end: CommitLiveness applied at the proper-subset T — the
-- first application anywhere whose T is not all of Correct.
example : ∃ L, IsLeaderBlock U12 0 L ∧ SlowCommit U12 L 0 ∧
    Decided U12 (View.full U12) 0 (some L) :=
  DirectLiveness.holds (Fin 8) (Fin 19) U12
    ({1, 2, 3, 4, 5, 6} : Finset (Fin 8)) 0 0
    (by decide) (by decide) u12_synchronised (by decide) (by decide)
    (by decide) (by decide) (by decide)
    (View.full U12) (View.coversUpto_full U12 _)

-- ## Route diversification (U13, the frozen Fin 7 configuration)

/-- Forty-three blocks over rounds 0–6, arranged so each below-run slot
resolves by a different exclusive route. Round 1: five votes for slot
0's candidate (genesis id 2) — ids 7–11 (authors 0, 2, 3, 4, 5) — and
one abstention (id 12, author 6). Round 2: id 13 references exactly the
five voters and is slot 0's UNIQUE certificate; ids 14–18 adopt the
abstainer and certify no slot-0 candidate; all reference slot 1's
candidate id 9.
Round 3: exactly two blocks (ids 19, 20) vote for slot 2's candidate id
16; all reference the certificate id 13, carrying it into slot 3's
candidate id 23. Round 4: five of six reference id 23 (slow, not
fast); id 30 (author 6) abstains and is slot 4's candidate. Rounds
5–6: full six-parent references — round 5 both votes for id 30 and
certifies id 23 (six certifiers), and proposes slot 5's candidate id
31; round 6 fast-commits it. -/
def lk13 : Fin 43 → Block (Fin 7) (Fin 43) := fun i =>
  if h : (i : ℕ) < 7 then
    { round := 0, author := ⟨i, by omega⟩, parents := ∅ }
  else if h : (i : ℕ) < 12 then
    { round := 1,
      author := ⟨if (i : ℕ) = 7 then 0 else (i : ℕ) - 6, by split <;> omega⟩,
      parents := {0, 2, 3, 4, 5} }
  else if h : (i : ℕ) < 13 then
    { round := 1, author := ⟨6, by omega⟩, parents := {0, 1, 3, 4, 5} }
  else if h : (i : ℕ) < 14 then
    { round := 2, author := ⟨0, by omega⟩, parents := {7, 8, 9, 10, 11} }
  else if h : (i : ℕ) < 19 then
    { round := 2, author := ⟨(i : ℕ) - 12, by omega⟩,
      parents := {8, 9, 10, 11, 12} }
  else if h : (i : ℕ) < 21 then
    { round := 3,
      author := ⟨if (i : ℕ) = 19 then 0 else (i : ℕ) - 18, by split <;> omega⟩,
      parents := {13, 14, 16, 17, 18} }
  else if h : (i : ℕ) < 25 then
    { round := 3, author := ⟨(i : ℕ) - 18, by omega⟩,
      parents := {13, 14, 15, 17, 18} }
  else if h : (i : ℕ) < 30 then
    { round := 4,
      author := ⟨if (i : ℕ) = 25 then 0 else (i : ℕ) - 24, by split <;> omega⟩,
      parents := {20, 21, 22, 23, 24} }
  else if h : (i : ℕ) < 31 then
    { round := 4, author := ⟨6, by omega⟩, parents := {19, 20, 21, 22, 24} }
  else if h : (i : ℕ) < 37 then
    { round := 5,
      author := ⟨if (i : ℕ) = 31 then 0 else (i : ℕ) - 30, by split <;> omega⟩,
      parents := {25, 26, 27, 28, 29, 30} }
  else
    { round := 6,
      author := ⟨if (i : ℕ) = 37 then 0 else (i : ℕ) - 36, by split <;> omega⟩,
      parents := {31, 32, 33, 34, 35, 36} }

/-- The route-diversification universe. -/
def U13 : BlockUniverse (Fin 7) (Fin 43) where
  ids := Finset.univ
  block := lk13
  complete := by decide
  valid := by decide
  no_equivocation := by decide

-- Slot 0 is rung-1-only: five votes (= q_cert, < q_fast), a UNIQUE
-- certificate (id 13), one certifier (< q_slow), one blame (< q_fast).
example : IsLeaderBlock U13 0 2 := by decide
example : supporters U13 2 1 = {0, 2, 3, 4, 5} := by decide
example : certificates U13 2 0 = {13} ∧ certifiers U13 2 0 = {0} := by decide
example : blames U13 0 = {6} := by decide
example : ¬ FastCommitInView U13 (View.full U13) 2 0 ∧
    ¬ SlowCommitInView U13 (View.full U13) 2 0 ∧
    ¬ SkippedLeaderInView U13 (View.full U13) 0 := by decide

-- Slot 2 is rung-3-only: two votes (< q_weak = 3, so the weak rung is
-- out and no certificate can form), yet four blames (< q_fast = 6).
example : IsLeaderBlock U13 2 16 := by decide
example : supporters U13 16 3 = {0, 2} := by decide
example : certificates U13 16 2 = ∅ ∧ blames U13 2 = {3, 4, 5, 6} := by
  decide
example : ¬ SkippedLeaderInView U13 (View.full U13) 2 := by decide

-- Slot 3: the development's first SLOW-committed anchor — five
-- supporters < q_fast, six certifiers ≥ q_slow.
example : IsLeaderBlock U13 3 23 := by decide
example : supporters U13 23 4 = {0, 2, 3, 4, 5} := by decide
example : ¬ FastCommit U13 23 3 ∧ SlowCommit U13 23 3 := by decide

-- Slots 1, 4, 5 fast-commit (candidates 9, 30, 31).
example : IsLeaderBlock U13 1 9 ∧ FastCommitInView U13 (View.full U13) 9 1 ∧
    FastCommitInView U13 (View.full U13) 30 4 ∧
    FastCommitInView U13 (View.full U13) 31 5 := by decide
example : Decided U13 (View.full U13) 1 (some 9) :=
  Decided.directFast (by decide) (by decide)

-- Slot 0's derivation: the certificate rung, anchored on the
-- slow-committed slot 3 — the first indirectCert whose anchor is not
-- fast-committed, and the first that is the slot's only route.
example : Decided U13 (View.full U13) 0 (some 2) :=
  Decided.indirectCert (j := 3) (A := 23) (by omega) (by decide)
    (Decided.directSlow (by decide) (by decide))
    (fun i h1 h2 h3 => by
      have hi : i = 1 ∨ i = 2 := by omega
      rcases hi with rfl | rfl
      · exact absurd h3 (by decide)
      · exact absurd h3 (by decide))
    (by decide)
    ((certifiedIn_iff_history (by decide)).mpr (by decide))

-- Slot 2's derivation: the indirect skip, with real negative rungs
-- (a candidate exists and has votes — just not enough for any rung).
example : Decided U13 (View.full U13) 2 none := by
  have hall : ∀ L : Fin 43, IsLeaderBlock U13 2 L → L = 16 := by decide
  refine Decided.indirectSkip (j := 5) (A := 31) (by omega) (by decide)
    (Decided.directFast (by decide) (by decide))
    (fun i h1 h2 h3 => by
      have hi : i = 3 ∨ i = 4 := by omega
      rcases hi with rfl | rfl
      · exact absurd h3 (by decide)
      · exact absurd h3 (by decide))
    (fun L hL hcert => by
      have := hall L hL
      subst this
      exact absurd ((certifiedIn_iff_history (by decide)).mp hcert)
        (by decide))
    (fun L hL hweak => by
      have := hall L hL
      subst this
      exact absurd ((weakLinked_iff_history (by decide)).mp hweak)
        (by decide))

-- End-to-end: the descent at b = 3, c = 3 — the three below-run slots
-- resolve by three different routes (cert rung, direct fast, indirect
-- skip), and the run itself mixes slow and fast commits.
example : ∀ i, i < 3 → ∃ v, Decided U13 (View.full U13) i v :=
  (IndirectLiveness.holds (Fin 7) (Fin 43) U13).2 (View.full U13) 3 3
    (by omega) spansEligible_seven
    (fun j h1 h2 => by
      have hj : j = 3 ∨ j = 4 ∨ j = 5 := by omega
      rcases hj with rfl | rfl | rfl
      · exact ⟨23, Decided.directSlow (by decide) (by decide)⟩
      · exact ⟨30, Decided.directFast (by decide) (by decide)⟩
      · exact ⟨31, Decided.directFast (by decide) (by decide)⟩)

-- ## A sparse schedule: descent below a SINGLE commit (U14, Fin 6)

/-- A crash-only configuration with an unspent budget: `c = 2` but only
replica 1 actually crashed. -/
instance sixReplicas : Faults (Fin 6) where
  f := 0
  c := 2
  k := 1
  byzantine := ∅
  crashed := {1}
  byzantine_disjoint_crashed := by decide
  card_replicas := by decide
  card_byzantine := by decide
  card_crashed := by decide

/-- The three-round-spacing schedule: slot `k` proposes at round `3k` —
the first non-pipelined schedule in the development. Slot 0 is led by
replica 2, slot 1 by replica 3. -/
instance : Slots (Fin 6) :=
  Slots.uniformSingle 3 (by omega) fun k => ⟨(k + 2) % 6, by omega⟩

-- The threshold table at n = 6, f = 0, c = 2, k = 1.
example : p (Fin 6) = 1 ∧ q (Fin 6) = 4 ∧ qFast (Fin 6) = 5 ∧
    qCert (Fin 6) = 4 ∧ qSlow (Fin 6) = 3 ∧ qWeak (Fin 6) = 2 := by decide

/-- With three rounds between slots, a run's last slot clears every
below-slot's decision window already at `c = 1`: the run-length
requirement is the schedule's density, not the theorem's. -/
theorem spansEligible_six : IndirectLiveness.SpansEligible (Fin 6) 1 := by
  intro b i h
  change 3 * (i / 1) + 2 < 3 * ((b + 1 - 1) / 1)
  omega

/-- Thirty blocks: rounds 0–5 × the five correct replicas
{0, 2, 3, 4, 5}, full references round to round. Slot 0 proposes at
round 0, slot 1 at round 3; rounds 1–2 are the inter-slot gap the
sparse schedule leaves, and round 5 is slot 1's decision round. -/
def lk14 : Fin 30 → Block (Fin 6) (Fin 30) := fun i =>
  { round := (i : ℕ) / 5,
    author := ⟨if (i : ℕ) % 5 = 0 then 0 else (i : ℕ) % 5 + 1,
      by split <;> omega⟩,
    parents :=
      if h : (i : ℕ) < 5 then ∅
      else {⟨(i : ℕ) / 5 * 5 - 5, by omega⟩, ⟨(i : ℕ) / 5 * 5 - 4, by omega⟩,
        ⟨(i : ℕ) / 5 * 5 - 3, by omega⟩, ⟨(i : ℕ) / 5 * 5 - 2, by omega⟩,
        ⟨(i : ℕ) / 5 * 5 - 1, by omega⟩} }

/-- The sparse-schedule universe. -/
def U14 : BlockUniverse (Fin 6) (Fin 30) where
  ids := Finset.univ
  block := lk14
  complete := by decide
  valid := by decide
  no_equivocation := by decide

-- Slot 1 (round 3, leader 3, candidate id 17) fast-commits at round 4.
example : IsLeaderBlock U14 1 17 ∧
    FastCommitInView U14 (View.full U14) 17 3 := by decide

-- End-to-end: the descent below a run of length ONE — killing the
-- `0 < c → 3 ≤ c` strengthening the pipelined witnesses cannot reach.
example : ∀ i, i < 1 → ∃ v, Decided U14 (View.full U14) i v :=
  (IndirectLiveness.holds (Fin 6) (Fin 30) U14).2 (View.full U14) 1 1
    (by omega) spansEligible_six
    (fun j h1 h2 => by
      have hj : j = 1 := by omega
      subst hj
      exact ⟨17, Decided.directFast (by decide) (by decide)⟩)

-- Synchronised from round 0 — for the strict-R application below.
theorem u14_synchronised : Synchronised U14 0 := by
  intro n hn b hb hbr hbc a ha har hac
  have hmax : ∀ c : Fin 30, (U14.block c).round ≤ 5 := by decide
  have hb2 := hmax b
  have hn2 : n = 0 ∨ n = 1 ∨ n = 2 ∨ n = 3 ∨ n = 4 := by omega
  clear hb2 hmax hn
  rcases hn2 with rfl | rfl | rfl | rfl | rfl <;> (revert b a; decide)

-- End-to-end: CommitLiveness with R STRICTLY below the propose round
-- (R = 0 < 3 = slotRound 1) — the first such application, killing the
-- `R ≤ slotRound k → R = slotRound k` mutation that every pipelined
-- witness (R = 0 = slotRound k) left alive, as disclosed since U6.
example : ∃ L, IsLeaderBlock U14 1 L ∧ SlowCommit U14 L 3 ∧
    Decided U14 (View.full U14) 1 (some L) :=
  DirectLiveness.holds (Fin 6) (Fin 30) U14 (Correct : Finset (Fin 6)) 0 1
    (by decide) (by decide) u14_synchronised (by decide) (by decide)
    (by decide) (by decide) (by decide)
    (View.full U14) (View.coversUpto_full U14 _)

end HydrozoanTest
