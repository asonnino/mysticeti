import Hydrozoan.Optimal.Model.Decided

/-!
# Optimal-Hydrozoan: decision-relation lemmas

Generated proof infrastructure over `Optimal/Model/Decided.lean`; not
part of the audit surface.
-/

namespace Hydrozoan

namespace Optimal

variable {Replica BlockId : Type*} [Fintype Replica] [DecidableEq Replica]
  [DecidableEq BlockId] [O : OptimalFaults Replica] [S : Slots Replica]
  {U : OptUniverse Replica BlockId} {V : View U.toBlockUniverse}

/-- Every commit verdict names a candidate of its slot: each committing
constructor carries `IsLeaderBlock`. -/
theorem isLeaderBlock_of_decidedOpt {k : ℕ} {L : BlockId}
    (h : DecidedOpt U V k (some L)) : IsLeaderBlock U.toBlockUniverse k L := by
  cases h with
  | directFast hL _ => exact hL
  | directSlow hL _ => exact hL
  | indirectCert _ _ _ _ hL _ => exact hL
  | indirectEvidence _ _ _ _ _ hL _ => exact hL

end Optimal

end Hydrozoan
