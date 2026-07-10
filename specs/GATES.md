# Gates index (agent entrypoint)

**Day-to-day:** load `specs/<core>/GATES.md` only.  
**Race redesign:** full TLC in `specs/<core>/*.tla`.  
**Regen:** `bazel run //tools/decision:update`  
**Lean check:** `scripts/decision-check.sh`  
(wires + decision tags + Go↔Rust name parity + `cargo test -p decision_cores`)  
**Full TLC:** `scripts/check-specs.sh` (`--with-tlc` / core names on decision-check)  
**Cites:** production / decision / pure **symbols** only — never `file.go:line`
(lines rot; GATES + duals are the map).  
**Rust peer:** `crates/decision_cores` (same Decision.tla; `gates::*` wrappers).

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

## New cores (policy)

**Only** when a real concurrent / stale / race **decision** appears in production.

Do:

- `specs/<name>/decision/Decision.tla` + MC / bait
- `decision_core(...)` in `tools/decision/BUILD.bazel` (Go dest + `src_rs`)
- duals (Go `*spec` + Rust `gates` / dual tables)
- wire prod pure/actions → generated symbols (not re-inlined formulas)

Do **not**:

- add cores “for completeness” or docs alone
- expand specgen to multi-object / record codegen
- put Rust into the `ote` binary until a real Rust consumer exists

Both languages share the same `Decision.tla` SSOT.
