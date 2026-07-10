# timing-clamp — gates (agent SSOT)

Load this for code changes. Full TLC for pipeline faults: `TimingClamp.tla`.

| Gate | Production | Decision | Generated pure |
|------|------------|----------|----------------|
| Clamp child to parent | `clampSpanToParent` → **DoClamp** | DoClamp | `ClampedOrdered`, `ClampedContained` |
| Package | `pkg/analyzer` | `decision/Decision.tla` | `timingclampspec` |

**SSOT:** production calls generated `DoClamp` (not a hand formula).

**Rust peer:** `gates::clamp_span_to_parent` → `timing_clamp` module.  
**Check:** `scripts/decision-check.sh`
