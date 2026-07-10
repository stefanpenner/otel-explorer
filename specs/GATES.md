# Gates index (agent entrypoint)

**Day-to-day:** load `specs/<core>/GATES.md` only.  
**Race redesign:** full TLC in `specs/<core>/*.tla`.  
**Regen:** `bazel run //tools/decision:update`  
**Lean check:** `scripts/decision-check.sh`  
(`--with-tlc` or `<core>` for TLC; full suite: `scripts/check-specs.sh`)  
**Cites:** production / decision / pure **symbols** only — never `file.go:line`
(lines rot; GATES + duals are the map).

| Core | GATES | Production package | Prod→gen SSOT |
|------|-------|--------------------|---------------|
| tui-reload | [tui-reload/GATES.md](tui-reload/GATES.md) | `pkg/tui/results` | `CanFetchAccept` |
| rate-limit | [rate-limit/GATES.md](rate-limit/GATES.md) | `pkg/githubapi` | `WaitNeeded` |
| timing-clamp | [timing-clamp/GATES.md](timing-clamp/GATES.md) | `pkg/analyzer` | `DoClamp` |
| sync-bounds | [sync-bounds/GATES.md](sync-bounds/GATES.md) | `pkg/store` | `AcceptAllowed` |
| gha-lifecycle | [gha-lifecycle/GATES.md](gha-lifecycle/GATES.md) | `pkg/analyzer` | `CanClassify*` |
| log-groups | [log-groups/GATES.md](log-groups/GATES.md) | `pkg/logparse` | `CanOpen`/`CanClose` |
| span-tree | [span-tree/GATES.md](span-tree/GATES.md) | `pkg/analyzer` | `DedupChoose` |

Inventory: `scripts/decision-stack-status.sh`
