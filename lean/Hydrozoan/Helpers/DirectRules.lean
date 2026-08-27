import Hydrozoan.Model.DirectRules

/-!
# Direct-rule instances and bridges

Generated: decidability for the top-level rule predicates (so witness
models settle them by `decide`) and the "views only under-report" bridge
lemmas. Nothing here is part of the audit surface.
-/

namespace Hydrozoan

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [F : Faults Replica]
  {U : BlockUniverse Replica BlockId}

instance decidableFastCommit (L : BlockId) (r : ℕ) :
    Decidable (FastCommit U L r) :=
  inferInstanceAs (Decidable (qFast Replica ≤ (supporters U L (r + 1)).card))

instance decidableSlowCommit (L : BlockId) (r : ℕ) :
    Decidable (SlowCommit U L r) :=
  inferInstanceAs (Decidable (qSlow Replica ≤ (certifiers U L r).card))

instance decidableFastCommitInView (V : View U) (L : BlockId) (r : ℕ) :
    Decidable (FastCommitInView U V L r) :=
  inferInstanceAs (Decidable (qFast Replica ≤ (supportersInView U V L (r + 1)).card))

instance decidableSlowCommitInView (V : View U) (L : BlockId) (r : ℕ) :
    Decidable (SlowCommitInView U V L r) :=
  inferInstanceAs (Decidable (qSlow Replica ≤ (certifiersInView U V L r).card))

section Skip

variable [S : Slots Replica]

instance decidableSkippedLeader (k : ℕ) : Decidable (SkippedLeader U k) :=
  inferInstanceAs (Decidable (qFast Replica ≤ (blames U k).card))

instance decidableSkippedLeaderInView (V : View U) (k : ℕ) :
    Decidable (SkippedLeaderInView U V k) :=
  inferInstanceAs (Decidable (qFast Replica ≤ (blamesInView U V k).card))

end Skip

/-- A view can only under-report fast commits. -/
theorem fastCommit_of_fastCommitInView {V : View U} {L : BlockId} {r : ℕ}
    (h : FastCommitInView U V L r) : FastCommit U L r :=
  le_trans h
    (Finset.card_le_card (Finset.image_subset_image Finset.inter_subset_left))

/-- A view can only under-report slow commits. -/
theorem slowCommit_of_slowCommitInView {V : View U} {L : BlockId} {r : ℕ}
    (h : SlowCommitInView U V L r) : SlowCommit U L r :=
  le_trans h
    (Finset.card_le_card (Finset.image_subset_image Finset.inter_subset_left))

/-- A view can only under-report skips. -/
theorem skippedLeader_of_skippedLeaderInView [S : Slots Replica] {V : View U}
    {k : ℕ} (h : SkippedLeaderInView U V k) : SkippedLeader U k :=
  le_trans h
    (Finset.card_le_card (Finset.image_subset_image Finset.inter_subset_left))

end Hydrozoan
