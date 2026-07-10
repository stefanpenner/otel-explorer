# log-groups — gates (agent SSOT)

Load this for code changes. Full TLC for timestamps/gaps: `LogGroups.tla`.

| Gate | Production | Decision | Generated pure |
|------|------------|----------|----------------|
| May open group | `canOpenGroup` (parametric) | Open | — |
| May close group | `canCloseGroup` → **CanClose** | Close | `CanClose`, `Inv_DepthNonNeg` |
| Package | `pkg/logparse` | `decision/Decision.tla` | `loggroupsspec` |

**SSOT:** production `canCloseGroup` calls generated `CanClose`.  
Open stays parametric (prod unbounded depth; core MaxDepth=3).  
**Rule:** never underflow depth (stray endgroup is no-op).

**Check:** `scripts/decision-check.sh`
