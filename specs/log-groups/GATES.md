# log-groups — gates (agent SSOT)

Load this for code changes. Full TLC for timestamps/gaps: `LogGroups.tla`.

| Gate | Production | Decision | Generated pure |
|------|------------|----------|----------------|
| May open group | `canOpenGroup` → **CanOpen** (maxDepth=3) | Open | — |
| May close group | `canCloseGroup` → **CanClose** | Close | `CanClose`, `Inv_DepthNonNeg` |
| Package | `pkg/logparse` | `decision/Decision.tla` | `loggroupsspec` |

**SSOT:** close always via `CanClose`; open via `CanOpen` when maxDepth=3.  
Production `splitGroups` uses unbounded (maxDepth=0).  
**Rule:** never underflow depth (stray endgroup is no-op).

**Rust peer:** `gates::can_open_group` / `can_close_group` → `log_groups` module.  
**Check:** `scripts/decision-check.sh`
