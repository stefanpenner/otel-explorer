# Gates index (agent entrypoint)

**Day-to-day:** load `specs/<core>/GATES.md` only.  
**Race redesign:** full TLC in `specs/<core>/*.tla`.  
**Regen:** `bazel run //tools/decision:update`

| Core | GATES | Production package |
|------|-------|--------------------|
| tui-reload | [tui-reload/GATES.md](tui-reload/GATES.md) | `pkg/tui/results` |
| rate-limit | [rate-limit/GATES.md](rate-limit/GATES.md) | `pkg/githubapi` |
| timing-clamp | [timing-clamp/GATES.md](timing-clamp/GATES.md) | `pkg/analyzer` |
| sync-bounds | [sync-bounds/GATES.md](sync-bounds/GATES.md) | `pkg/store` |
| gha-lifecycle | [gha-lifecycle/GATES.md](gha-lifecycle/GATES.md) | `pkg/analyzer` |
| log-groups | [log-groups/GATES.md](log-groups/GATES.md) | `pkg/logparse` |
| span-tree | [span-tree/GATES.md](span-tree/GATES.md) | `pkg/analyzer` |

Inventory: `scripts/decision-stack-status.sh`
