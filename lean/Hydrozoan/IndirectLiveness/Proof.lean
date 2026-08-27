import Hydrozoan.IndirectLiveness.Statement
import Hydrozoan.Helpers.IndirectLiveness

/-!
# Proof: indirect liveness

Generated. Totality is `decided_of_anchor` (three-way classical split
over the rungs, with `Finset.min'` supplying the weak rung's least
candidate); descent instantiates `decided_below_of_committed_run` at the
run's last slot `n := b + c - 1`.
-/

namespace Hydrozoan
namespace IndirectLiveness

theorem holds : Statement := by
  intro Replica BlockId _ _ _ _ _ _ U
  constructor
  · intro V k j A helig hj hmid
    exact decided_of_anchor helig hj hmid
  · intro V b c hc hspan hrun i hi
    exact decided_below_of_committed_run (by omega)
      (fun i' hi' => hspan b i' hi') hrun i hi

end IndirectLiveness
end Hydrozoan
