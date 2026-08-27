import Hydrozoan.Model.Slots

/-!
# Schedule constructors

Generated: concrete `Slots` instances with the algebraic side conditions
discharged once and for all. Nothing here is part of the audit surface
(the witness *instantiations* built from these are).
-/

namespace Hydrozoan

namespace Slots

variable {Replica : Type*}

/-- The uniform schedule: `m` leaders in every `p`-th round, slot `k`
led by `elect k`. -/
@[reducible]
def uniform (p m : ℕ) (hp : 0 < p) (hm : 0 < m) (elect : ℕ → Replica)
    (hblock : ∀ k₁ k₂, k₁ / m = k₂ / m → elect k₁ = elect k₂ → k₁ = k₂) :
    Slots Replica where
  slotRound k := p * (k / m)
  leader k := elect k
  mono := fun _ _ hab => Nat.mul_le_mul_left p (Nat.div_le_div_right hab)
  unbounded := fun n => ⟨m * n, by
    rw [Nat.mul_div_cancel_left n hm]
    exact Nat.le_mul_of_pos_left n hp⟩
  keyed := by
    intro k₁ k₂ h
    simp only [Prod.mk.injEq] at h
    exact hblock k₁ k₂ (Nat.eq_of_mul_eq_mul_left hp h.1) h.2

/-- With one leader per round-group, any election is collision-free. -/
theorem one_hblock (elect : ℕ → Replica) :
    ∀ k₁ k₂ : ℕ, k₁ / 1 = k₂ / 1 → elect k₁ = elect k₂ → k₁ = k₂ := by
  intro k₁ k₂ h _
  simpa using h

/-- One leader every `p` rounds; `p = 1` is the pipelined single-leader
schedule. -/
@[reducible]
def uniformSingle (p : ℕ) (hp : 0 < p) (elect : ℕ → Replica) :
    Slots Replica :=
  uniform p 1 hp Nat.one_pos elect (one_hblock elect)

end Slots

end Hydrozoan
