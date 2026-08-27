import Hydrozoan.Model.Faults

/-!
# Fault-model lemmas

Generated proof infrastructure over `Model/Faults.lean`: membership
unfoldings for the two pools and the availability reading of `q`. Nothing
here is part of the audit surface.
-/

namespace Hydrozoan

variable {Replica : Type*} [Fintype Replica] [DecidableEq Replica] [F : Faults Replica]

/-- Membership in `Correct`, unfolded. -/
@[simp]
theorem mem_correct {v : Replica} :
    v ∈ (Correct : Finset Replica) ↔ v ∉ F.byzantine ∧ v ∉ F.crashed := by
  simp [Correct]

/-- Membership in `NonByzantine`, unfolded. -/
@[simp]
theorem mem_nonByzantine {v : Replica} :
    v ∈ (NonByzantine : Finset Replica) ↔ v ∉ F.byzantine := by
  simp [NonByzantine]

/-- Every correct replica is non-Byzantine. -/
theorem correct_subset_nonByzantine :
    (Correct : Finset Replica) ⊆ NonByzantine := fun v hv => by
  rw [mem_correct] at hv
  exact mem_nonByzantine.mpr hv.1

/-- The correct replicas alone meet the DAG quorum: at least `n − f − c` of
them. This is what the threshold `q` is *for* — the correct pool suffices
on its own to keep the DAG advancing. -/
theorem q_le_card_correct : q Replica ≤ (Correct : Finset Replica).card := by
  have hcompl : (Correct : Finset Replica).card
      = Fintype.card Replica - (F.byzantine ∪ F.crashed).card :=
    Finset.card_compl _
  have hunion : (F.byzantine ∪ F.crashed).card ≤ F.byzantine.card + F.crashed.card :=
    Finset.card_union_le _ _
  have hle : (F.byzantine ∪ F.crashed).card ≤ Fintype.card Replica :=
    Finset.card_le_univ _
  have hf := F.card_byzantine
  have hc := F.card_crashed
  simp only [q]
  omega

end Hydrozoan
