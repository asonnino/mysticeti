import Hydrozoan.Optimal.PrefixAgreement.Proof
import HydrozoanTest.Optimal.SlotAgreement

/-!
# Witness: Optimal output sequences

The seven settled slots of `OD` packaged as decision functions, and the
output claims made concrete: the committed-leader sequence below horizon
7 is `[3, 8, 13, 22]` — the evidence-rung commit 8 and the
certificate-rung commit 13 sit in it between the two fast commits — a
shorter-horizon replica's `[3]` is its prefix (in the one-vote-short view
the anchor is undecided, so nothing indirect settles there and the
replica stops at slot 2), a toy linearizer's ledgers keep the prefix, and
`holds` fixes the sequence of *any* replica that has decided below 7 in
*any* view — all three of its conjuncts applied on data, and a verdict
function that drops the evidence-rung commit shown to decide below 7 in
no view.

A second, crash-only universe `OC` on `threeReplicasCrashOnly` (`f = 0`,
one crashed replica of three; `q = qFastOpt = qCert = qSlow = 2`) puts the
Optimal headline theorems through a configuration with no Byzantine
replica at all: a two-of-three fast commit (which Hydrozoan's `qFast = 3`
would not grant), a slow commit, a skip of the crashed leader's slot, and
the sequence `[2, 5]` fixed for every replica.
-/

namespace HydrozoanTest

open Hydrozoan Hydrozoan.Optimal Hydrozoan.PrefixAgreement Hydrozoan.Optimal.PrefixAgreement

set_option maxRecDepth 16384

/-- The seven settled verdicts of `OD` as a decision function: slots 0, 2,
3, 6 commit (ids 3, 8, 13, 22), slots 1, 4, 5 are skipped. -/
def gD : ℕ → Option (Fin 30)
  | 0 => some 3
  | 2 => some 8
  | 3 => some 13
  | 6 => some 22
  | _ => none

/-- A shorter-horizon replica in the one-vote-short view: slots 0 and 1
settled directly; slot 2 waits for an anchor the view cannot commit. -/
def gDs : ℕ → Option (Fin 30)
  | 0 => some 3
  | _ => none

-- Every slot below 7 is decided in the full view with gD's verdicts —
-- five of the six constructors across the seven slots (the sixth,
-- indirectSkip, settles slot 1 in the holds application below).
example : DecidesBelow OD VD gD 7 := by
  intro k hk
  have hcase : k = 0 ∨ k = 1 ∨ k = 2 ∨ k = 3 ∨ k = 4 ∨ k = 5 ∨ k = 6 := by omega
  rcases hcase with rfl | rfl | rfl | rfl | rfl | rfl | rfl
  · exact DecidedOpt.directSlow (by decide) (by decide)
  · exact skipD1
  · exact od_slot2_evidence
  · exact DecidedOpt.indirectCert (j := 6) (A := 22) (by omega) (by decide)
      (DecidedOpt.directFast (by decide) (by decide))
      (fun i h1 h2 h3 => by
        have hi : i = 4 ∨ i = 5 := by omega
        rcases hi with rfl | rfl
        · exact absurd h3 (by decide)
        · exact absurd h3 (by decide))
      (by decide)
      ⟨18, by decide, Reaches.single (by decide)⟩
  · exact skipD4
  · exact skipD5
  · exact DecidedOpt.directFast (by decide) (by decide)

-- The actual output sequence: skips dropped, slot order kept.
example : commitSeq gD 7 = [3, 8, 13, 22] := rfl

-- The shorter replica, in the one-vote-short view: slot 0 fast, slot 1
-- skipped, both directly; slot 2 has no direct route there (its blames
-- pass, its no-evidence quorum fails, no certificate, one vote) and no
-- anchor (the view cannot fast-commit 22), so the replica stops there.
theorem vds_gDs : DecidesBelow OD VDs' gDs 2 := by
  intro k hk
  have hcase : k = 0 ∨ k = 1 := by omega
  rcases hcase with rfl | rfl
  · exact DecidedOpt.directFast (by decide) (by decide)
  · exact DecidedOpt.directSkip (by decide)
example :
    ¬ FastCommitOptInView OD.toBlockUniverse VDs' 8 (Slots.slotRound (Fin 4) 2) ∧
      ¬ SlowCommitInView OD.toBlockUniverse VDs' 8 (Slots.slotRound (Fin 4) 2) ∧
      ¬ SkippedLeaderOptInView OD.toBlockUniverse VDs' 2 := by
  decide

example : commitSeq gDs 2 = [3] := rfl

-- Prefix consistency, concretely ...
example : commitSeq gDs 2 <+: commitSeq gD 7 := ⟨[8, 13, 22], rfl⟩

-- ... and through a toy linearizer: flattening preserves the prefix.
example : ledger (fun b => [b, b]) gDs 2 <+: ledger (fun b => [b, b]) gD 7 :=
  ⟨[8, 8, 13, 13, 22, 22], rfl⟩

/-- The full-view verdicts below 7, as a named derivation for the theorem
applications: fast at 0 and 3, indirectSkip at 1, the evidence rung at
2, direct skips at 4 and 5, fast at 6. -/
theorem vd_gD : DecidesBelow OD VD gD 7 := by
  intro k hk
  have hcase : k = 0 ∨ k = 1 ∨ k = 2 ∨ k = 3 ∨ k = 4 ∨ k = 5 ∨ k = 6 := by omega
  rcases hcase with rfl | rfl | rfl | rfl | rfl | rfl | rfl
  · exact DecidedOpt.directFast (by decide) (by decide)
  · have hall : ∀ M : Fin 30, IsLeaderBlock UD 1 M → M = 29 := by decide
    refine DecidedOpt.indirectSkip (j := 6) (A := 22) (by omega) (by decide)
      (DecidedOpt.directFast (by decide) (by decide))
      (fun i h1 h2 h3 => by
        have hi : i = 2 ∨ i = 3 ∨ i = 4 ∨ i = 5 := by omega
        rcases hi with rfl | rfl | rfl | rfl
        · exact absurd h3 (by decide)
        · exact absurd h3 (by decide)
        · exact skipD4
        · exact skipD5)
      (fun L hL hcert => by
        have := hall L hL
        subst this
        exact absurd ((certifiedIn_iff_history (by decide)).mp hcert) (by decide))
      (fun L hL hev => by
        have := hall L hL
        subst this
        exact absurd ((evidenceLinked_iff_history (by decide)).mp hev) (by decide))
  · exact od_slot2_evidence
  · exact DecidedOpt.directFast (by decide) (by decide)
  · exact skipD4
  · exact skipD5
  · exact DecidedOpt.directFast (by decide) (by decide)

-- The theorem's prefix and ledger conjuncts on data: the short replica's
-- output is a prefix of the full one, and so are their ledgers.
example : commitSeq gDs 2 <+: commitSeq gD 7 :=
  (Optimal.PrefixAgreement.holds (Fin 4) (Fin 30) OD).2.1 VDs' VD gDs gD 2 7 (by omega)
    vds_gDs vd_gD
example : ledger (fun b => [b, b]) gDs 2 <+: ledger (fun b => [b, b]) gD 7 :=
  (Optimal.PrefixAgreement.holds (Fin 4) (Fin 30) OD).2.2 (fun b => [b, b]) VDs' VD gDs gD 2 7
    (by omega) vds_gDs vd_gD

/-- A verdict function that drops the evidence-rung commit of slot 2. -/
def gDbad : ℕ → Option (Fin 30)
  | 0 => some 3
  | 3 => some 13
  | 6 => some 22
  | _ => none

-- It decides below 7 in no view: its sequence [3, 13, 22] would have to
-- equal [3, 8, 13, 22].
example : ∀ V : View OD.toBlockUniverse, ¬ DecidesBelow OD V gDbad 7 := fun V h =>
  absurd ((Optimal.PrefixAgreement.holds (Fin 4) (Fin 30) OD).1 VD V gD gDbad 7 vd_gD h)
    (by decide)

-- The theorem on data: any replica that has decided every slot below 7,
-- in any view, outputs exactly [3, 8, 13, 22].
example :
    ∀ (V : View OD.toBlockUniverse) (g : ℕ → Option (Fin 30)),
      DecidesBelow OD V g 7 → commitSeq g 7 = [3, 8, 13, 22] :=
  fun V g h => ((Optimal.PrefixAgreement.holds (Fin 4) (Fin 30) OD).1 VD V gD g 7 vd_gD h).symm

-- The crash-only universe: three replicas, replica 0 crashed, f = 0.

/-- The pipelined schedule on three replicas: slot `k` at round `k`, led
by `(k + 2) % 3` — slot 1 by the crashed replica `0`. -/
instance threeSlotsOpt : Slots (Fin 3) where
  slotRound k := k
  leader k := ⟨(k + 2) % 3, Nat.mod_lt _ (by decide)⟩
  mono := fun _ _ h => h
  unbounded := fun n => ⟨n, le_rfl⟩
  keyed := fun _ _ h => congrArg Prod.fst h

/-- Nine blocks over four rounds, replica `0` silent after genesis. Ids
0–2: genesis; 2 is slot 0's candidate (leader `2`). Round 1: 3, 4 by `1`,
`2` reference `{1, 2}` — two votes for 2, exactly `qFastOpt`. Round 2: 5,
6 by `1`, `2` reference `{3, 4}` — certificates for 2; 5 is slot 2's
candidate (leader `1`); both blame slot 1 (leader `0`, no candidate).
Round 3: 7, 8 by `1`, `2` reference `{5, 6}` — two votes for 5, and
no-evidence for slot 1. -/
def lkC : Fin 9 → Block (Fin 3) (Fin 9) := fun i =>
  if h : (i : ℕ) < 3 then
    { round := 0, author := ⟨i, by omega⟩, parents := ∅ }
  else if h : (i : ℕ) < 5 then
    { round := 1, author := ⟨(i : ℕ) - 2, by omega⟩, parents := {1, 2} }
  else if h : (i : ℕ) < 7 then
    { round := 2, author := ⟨(i : ℕ) - 4, by omega⟩, parents := {3, 4} }
  else
    { round := 3, author := ⟨(i : ℕ) - 6, by omega⟩, parents := {5, 6} }

/-- The base universe. -/
def UC : BlockUniverse (Fin 3) (Fin 9) where
  ids := Finset.univ
  block := lkC
  complete := by decide
  valid := by decide
  no_equivocation := by decide

/-- ... as an `OptUniverse` (no equivocation: nobody is Byzantine). -/
def OC : OptUniverse (Fin 3) (Fin 9) :=
  { UC with leader_excluded := leaderExcluded_of_noEquivocation UC (by decide) }

/-- The full view, typed at the projection. -/
def VC : View OC.toBlockUniverse := View.full UC

-- Thresholds and the headline on data: f = 0; two votes fast-commit
-- here and would not in Hydrozoan (qFast = 3).
example :
    Faults.f (Replica := Fin 3) = 0 ∧ qFastOpt (Fin 3) = 2 ∧ qFast (Fin 3) = 3 ∧
      FastCommitOpt UC 2 0 ∧ ¬ FastCommit UC 2 0 := by
  decide

/-- The three settled verdicts: slot 0 commits 2, slot 1 (crashed
leader) is skipped, slot 2 commits 5. -/
def gC : ℕ → Option (Fin 9)
  | 0 => some 2
  | 2 => some 5
  | _ => none

theorem vc_gC : DecidesBelow OC VC gC 3 := by
  intro k hk
  have hcase : k = 0 ∨ k = 1 ∨ k = 2 := by omega
  rcases hcase with rfl | rfl | rfl
  · exact DecidedOpt.directFast (by decide) (by decide)
  · exact DecidedOpt.directSkip (by decide)
  · exact DecidedOpt.directFast (by decide) (by decide)

-- Slot 0 also slow-commits (two certifiers), and the crashed leader's
-- slot is skipped by two blames and two vacuous no-evidence blocks.
example : DecidedOpt OC VC 0 (some 2) := DecidedOpt.directSlow (by decide) (by decide)
example : blames UC 1 = {1, 2} ∧ (∀ L, ¬ IsLeaderBlock UC 1 L) := by decide

-- The Optimal headline theorems at f = 0: no view skips slot 0, and
-- every replica that has decided below 3 outputs [2, 5].
example : ∀ V : View OC.toBlockUniverse, ¬ DecidedOpt OC V 0 none := fun V h =>
  Option.some_ne_none 2 (Optimal.SlotAgreement.holds (Fin 3) (Fin 9) OC VC V 0 _ none
    (DecidedOpt.directFast (by decide) (by decide)) h)
example :
    ∀ (V : View OC.toBlockUniverse) (g : ℕ → Option (Fin 9)),
      DecidesBelow OC V g 3 → commitSeq g 3 = [2, 5] :=
  fun V g h => ((Optimal.PrefixAgreement.holds (Fin 3) (Fin 9) OC).1 VC V gC g 3 vc_gC h).symm

end HydrozoanTest
