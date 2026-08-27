import Hydrozoan.Optimal.IndirectLiveness.Statement
import Hydrozoan.Optimal.Helpers.IndirectLiveness

/-!
# Optimal-Hydrozoan: indirect liveness — proof

Generated proof layer; not part of the audit surface. Both conjuncts are
the totality and descent lemmas of `Optimal/Helpers/IndirectLiveness.lean`.
-/

namespace Hydrozoan

namespace Optimal

namespace IndirectLiveness

theorem holds : Statement := by
  intro Replica BlockId _ _ _ _ _ U
  constructor
  · intro V k j A helig hj hmid
    exact decidedOpt_of_anchor helig hj hmid
  · intro V b c hc hspan hrun i hi
    exact decidedOpt_below_of_committed_run (by omega)
      (fun i' hi' => hspan b i' hi') hrun i hi

end IndirectLiveness

end Optimal

end Hydrozoan
