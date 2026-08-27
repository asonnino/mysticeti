import Hydrozoan.PrefixAgreement.Statement
import HydrozoanTest.SlotAgreement

/-!
# Witness: output sequences

The five settled slots of `U5` packaged as decision functions, and the
output claims made concrete: the committed-leader sequence below
horizon 5 is `[2, 31]`, a shorter-horizon replica's `[2]` is its
prefix, and a toy linearizer's ledgers keep the prefix.
-/

namespace HydrozoanTest

open Hydrozoan Hydrozoan.PrefixAgreement

set_option maxRecDepth 16384

/-- The five settled verdicts of `U5` as a decision function: slots 0
and 4 commit (ids 2 and 31), slots 1–3 are skipped. -/
def g5 : ℕ → Option (Fin 39)
  | 0 => some 2
  | 4 => some 31
  | _ => none

/-- A shorter-horizon replica: only slot 0 settled, from the second
view. -/
def g5b : ℕ → Option (Fin 39)
  | 0 => some 2
  | _ => none

-- Every slot below 5 is genuinely decided in the full view with g5's
-- verdicts: fast commits at slots 0 and 4, direct skips at 1–3.
example : DecidesBelow U5 Vfull5 g5 5 := by
  intro k hk
  have hcase : k = 0 ∨ k = 1 ∨ k = 2 ∨ k = 3 ∨ k = 4 := by omega
  rcases hcase with rfl | rfl | rfl | rfl | rfl
  · exact Decided.directFast (by decide) (by decide)
  · exact Decided.directSkip (by decide)
  · exact Decided.directSkip (by decide)
  · exact Decided.directSkip (by decide)
  · exact Decided.directFast (by decide) (by decide)

-- The actual output sequence: skips dropped, slot order kept.
example : commitSeq g5 5 = [2, 31] := rfl

-- The shorter replica, from the other view.
example : DecidesBelow U5 V5b g5b 1 := by
  intro k hk
  have hcase : k = 0 := by omega
  subst hcase
  exact Decided.directFast (by decide) (by decide)

example : commitSeq g5b 1 = [2] := rfl

-- Prefix consistency, concretely.
example : commitSeq g5b 1 <+: commitSeq g5 5 := ⟨[31], rfl⟩

-- And through a toy linearizer: flattening preserves the prefix.
example : ledger (fun b => [b, b]) g5b 1 <+: ledger (fun b => [b, b]) g5 5 :=
  ⟨[31, 31], rfl⟩

end HydrozoanTest
