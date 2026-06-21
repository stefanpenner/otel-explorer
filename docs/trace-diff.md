# Trace Diff

`ote diff` compares two traces the way `git diff` compares two trees — but
semantically, over spans. Point it at two back-to-back CI runs (two commits, a
base vs a PR head, yesterday vs today) and it tells you what actually changed:
which jobs and steps were **added**, **removed**, got **slower/faster**, or
**flipped pass/fail** — and attributes the wall-clock delta to the spans
responsible.

```
ote diff <before> <after> [flags]
```

Each side is either a **GitHub URL** (a run, commit, or PR — fetched live) or a
local **trace file** (any format `ote` reads: OTLP JSON, Chrome, Jaeger,
Zipkin). On a terminal it opens an interactive TUI; otherwise it prints a
report.

### Diffing from a trace backend (Tempo / Jaeger)

Add `--tempo=<url>` or `--jaeger=<url>` and the two inputs become **trace IDs**
fetched live from that backend — no manual export step:

```
ote diff --jaeger=http://localhost:16686 <trace-id-before> <trace-id-after>
ote diff --tempo=http://localhost:3200  <trace-id-before> <trace-id-after>
```

No GitHub token is needed in this mode. The two inputs are always interpreted as
trace IDs (never files or URLs) when a backend is selected.

To diff **across different backends** (e.g. a trace in Jaeger vs one in Tempo),
drop the flag and pass each side as a full `.../api/traces/<id>` URL — the
response format is auto-detected per side, so the two can differ:

```
ote diff \
  http://localhost:16686/api/traces/<jaeger-id> \
  http://localhost:3200/api/traces/<tempo-id>
```

This also lets you mix sources freely — e.g. a local Jaeger file on one side and
a live Tempo trace on the other.

## What it computes

- **Wall clock** — end-to-end time before → after, with the signed delta.
- **Structural changes** — jobs/steps present on only one side (added/removed),
  matched by their position in the workflow→job→step hierarchy.
- **Renames** — when an unmatched pair is similar enough (shared child structure
  and/or near-identical name), it collapses into one `R old → new` entry instead
  of a separate add + remove — the way `git diff -M` detects a moved file. A
  matrix dimension bump (`test (ruby 3.2)` → `test (ruby 3.4)`) becomes a single
  rename whose children are still diffed, so an inner regression still surfaces
  and a pure relabel stays out of the top movers.
- **Status flips** — every span that changed `success`/`failure`/`skipped`.
- **Top movers** — the wall-clock change *attributed* to the smallest set of
  responsible spans. A job that merely passes a slow step's regression upward is
  skipped in favour of the step itself, so the list reads like a blame, not an
  echo.

A change counts as material when it clears an absolute floor **and** a
proportional floor (≥2s and ≥20% by default), or is simply large in absolute
terms (≥2m) — so runner jitter is filtered but a real multi-minute regression on
a long job is never hidden.

## Output formats

| Invocation | Result |
|---|---|
| `ote diff a b` (a terminal) | interactive TUI |
| `ote diff a b --output=stdout` | styled text report |
| `ote diff a b --output=markdown` | GitHub-flavored markdown (drop into a PR comment) |
| `ote diff a b --output=json` | machine-readable diff (`jq '.top_movers'`) |

In the TUI: `↑/↓` move, `n`/`N` jump to the next/previous change, `u` toggles
unchanged context (a diff hides it by default), `g`/`G` jump to top/bottom,
`?` help, `q` quit.

## Example

The repo ships a reproducible example — two synthetic back-to-back commits of a
matrix Ruby build. Regenerate the fixtures with
`go run ./scripts/gen-diff-example` and diff them:

```
$ ote diff examples/diff/before.json examples/diff/after.json --output=stdout

  Trace Diff
  - before before.json
  + after  after.json

  wall clock  8m → 11m   +3m (+38%)
  changes     2 added · 2 removed · 3 changed · 3 status flips

  Status flips
    ✗ CI                                      success → failure
    ✗ test (ruby 3.3)                         success → failure
    ✗ Run specs                               success → failure

  Top movers (wall-clock change attributed to the responsible span)
    + test (ruby 3.4) (CI)                    +7m 50s new
    - test (ruby 3.2) (CI)                    -7m 50s gone
    ~ Run specs (test (ruby 3.3))             +3m (+45%)  6m 40s → 9m 40s
    - lint (CI)                               -2m gone
    + typecheck (CI)                          +1m 30s new

  Tree (- removed  + added  ~ changed)
  ~ CI                                          8m → 11m  +3m (+38%)  success → failure
  ~   test (ruby 3.3)                           7m 50s → 10m 50s  +3m (+38%)  success → failure
  ~     Run specs                               6m 40s → 9m 40s  +3m (+45%)  success → failure
  +   test (ruby 3.4)                           → 7m 50s new
  +   typecheck                                 → 1m 30s new
  -   test (ruby 3.2)                           7m 50s → gone
  -   lint                                      2m → gone
```

(The unchanged `build` job and its steps are pruned from the tree, like git
hides untouched regions; press `u` in the TUI to reveal them.)

## How matching works

Spans are matched by a stable **path key** — category + name, disambiguated by
occurrence among same-named siblings — rather than by span ID (which differs
between runs). This is the analogue of git matching files by path. Two steps
named identically in one job match positionally; a third copy on one side is an
add/remove. Matched spans recurse into their children; a matched node is
reported `Changed` if its own duration/status moved or anything beneath it did.

After exact matching, a **rename pass** (git's `-M`) runs over whatever is left:
each leftover removal is scored against each leftover addition of the same
category by similarity — child-set overlap (Jaccard, the "content") weighted 70%
plus a name bigram Dice score weighted 30%, or name alone for leaf nodes. Pairs
scoring ≥ 0.5 are collapsed greedily, highest score first, each node used once.
Below the threshold they stay a separate add + remove, so spurious renames are
never invented.
