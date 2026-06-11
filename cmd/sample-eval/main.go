// Command sample-eval evaluates job-detail sampling strategies against a
// ground-truth dataset captured with `ote trends --no-sample --dump-runs=`.
//
// For each strategy it draws many independent samples (varying the RNG seed),
// recomputes the job-level statistics the typical-run view reports (duration
// p50/p95, success rate, hourly queue pattern), and measures the error
// against the full-population truth. Output is a table of error quantiles
// per strategy, so "is the sample sufficient?" becomes a measured claim
// instead of a textbook one.
//
// Usage: sample-eval [-trials 200] [-margin 0.10] dump1.json [dump2.json ...]
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/stefanpenner/otel-explorer/pkg/githubapi"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
)

const minTruthObs = 20 // jobs with fewer counted observations are not scored

type jobKey struct{ workflow, job string }

type jobTruth struct {
	p50, p95    float64
	successRate float64
	obs         int
	presence    float64 // fraction of the workflow's runs containing the job
}

type strategy struct {
	name   string
	sample func(runs []analyzer.RunData, n int, seed int64) []int // returns indices of sampled runs
	sizeFn func(runs []analyzer.RunData, margin float64) int
}

func main() {
	trials := flag.Int("trials", 200, "samples drawn per strategy")
	margin := flag.Float64("margin", 0.10, "margin of error for the sample-size formula")
	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: sample-eval [-trials N] [-margin M] dump.json ...")
		os.Exit(1)
	}

	for _, path := range flag.Args() {
		dump, err := loadDump(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "loading %s: %v\n", path, err)
			os.Exit(1)
		}
		evalDump(dump, *trials, *margin)
	}
}

func loadDump(path string) (*analyzer.RunDump, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var dump analyzer.RunDump
	if err := json.Unmarshal(data, &dump); err != nil {
		return nil, err
	}
	return &dump, nil
}

func counted(j analyzer.JobData) bool {
	return j.Conclusion != "skipped" && j.Conclusion != "cancelled" && j.Conclusion != "neutral"
}

// jobStats computes per-job duration quantiles and success rates over the
// runs at the given indices.
func jobStats(runs []analyzer.RunData, indices []int) map[jobKey]jobTruth {
	durations := make(map[jobKey][]float64)
	successes := make(map[jobKey]int)
	totals := make(map[jobKey]int)
	runsSeen := make(map[jobKey]int)
	wfRuns := make(map[string]int)

	for _, idx := range indices {
		run := runs[idx]
		wfRuns[run.WorkflowName]++
		seen := make(map[jobKey]bool)
		for _, j := range run.Jobs {
			if !counted(j) {
				continue
			}
			k := jobKey{run.WorkflowName, j.Name}
			totals[k]++
			if j.Conclusion == "success" {
				successes[k]++
			}
			if j.Duration > 0 {
				durations[k] = append(durations[k], float64(j.Duration)/1000.0)
			}
			if !seen[k] {
				seen[k] = true
				runsSeen[k]++
			}
		}
	}

	out := make(map[jobKey]jobTruth, len(totals))
	for k, total := range totals {
		d := durations[k]
		sort.Float64s(d)
		out[k] = jobTruth{
			p50:         percentile(d, 50),
			p95:         percentile(d, 95),
			successRate: float64(successes[k]) / float64(total),
			obs:         total,
			presence:    float64(runsSeen[k]) / float64(wfRuns[k.workflow]),
		}
	}
	return out
}

// percentile matches analyzer.calculatePercentile (nearest-rank by ceil).
func percentile(sorted []float64, p int) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(len(sorted))*float64(p)/100.0)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// hourlyQueueP50 returns the median queue time per hour-of-day over the runs
// at the given indices (hours with <3 observations are zero).
func hourlyQueueP50(runs []analyzer.RunData, indices []int) [24]float64 {
	var buckets [24][]float64
	for _, idx := range indices {
		for _, j := range runs[idx].Jobs {
			if !counted(j) || j.QueueTime <= 0 || j.StartedAt.IsZero() {
				continue
			}
			h := j.StartedAt.UTC().Hour()
			buckets[h] = append(buckets[h], float64(j.QueueTime)/1000.0)
		}
	}
	var out [24]float64
	for h, b := range buckets {
		if len(b) >= 3 {
			sort.Float64s(b)
			out[h] = percentile(b, 50)
		}
	}
	return out
}

// pearson computes correlation over hours where both series have data.
func pearson(a, b [24]float64) float64 {
	var xs, ys []float64
	for i := 0; i < 24; i++ {
		if a[i] > 0 && b[i] > 0 {
			xs = append(xs, a[i])
			ys = append(ys, b[i])
		}
	}
	n := float64(len(xs))
	if n < 3 {
		return math.NaN()
	}
	var sx, sy, sxx, syy, sxy float64
	for i := range xs {
		sx += xs[i]
		sy += ys[i]
		sxx += xs[i] * xs[i]
		syy += ys[i] * ys[i]
		sxy += xs[i] * ys[i]
	}
	den := math.Sqrt((n*sxx - sx*sx) * (n*syy - sy*sy))
	if den == 0 {
		return math.NaN()
	}
	return (n*sxy - sx*sy) / den
}

// cochran computes the finite-population sample size (p=0.5, z=1.96).
func cochran(total int, margin float64) int {
	if total <= 0 {
		return 0
	}
	n0 := (1.96 * 1.96 * 0.25) / (margin * margin)
	n := n0 / (1 + (n0-1)/float64(total))
	size := int(math.Ceil(n))
	if size > total {
		size = total
	}
	return size
}

// --- sampling strategies ---

// timeStratified mirrors the production sampler: up to `buckets` equal-width
// time intervals, proportional allocation, uniform draw within each bucket.
func timeStratified(runs []analyzer.RunData, n int, seed int64, buckets int) []int {
	total := len(runs)
	if n >= total {
		return seqIndices(total)
	}
	rng := rand.New(rand.NewSource(seed))
	minT, maxT := runs[0].CreatedAt, runs[0].CreatedAt
	for _, r := range runs {
		if r.CreatedAt.Before(minT) {
			minT = r.CreatedAt
		}
		if r.CreatedAt.After(maxT) {
			maxT = r.CreatedAt
		}
	}
	span := maxT.Sub(minT)
	if buckets > n {
		buckets = n
	}
	if buckets < 1 {
		buckets = 1
	}
	groups := make([][]int, buckets)
	for i, r := range runs {
		b := 0
		if span > 0 {
			b = int(float64(r.CreatedAt.Sub(minT)) / float64(span) * float64(buckets))
			if b >= buckets {
				b = buckets - 1
			}
		}
		groups[b] = append(groups[b], i)
	}
	return drawProportional(groups, n, total, rng)
}

// workflowTimeStratified stratifies by (workflow, time bucket): every
// workflow is guaranteed representation proportional to its run count, with
// a minimum of one run per non-empty workflow.
func workflowTimeStratified(runs []analyzer.RunData, n int, seed int64, timeBuckets int) []int {
	total := len(runs)
	if n >= total {
		return seqIndices(total)
	}
	rng := rand.New(rand.NewSource(seed))
	minT, maxT := runs[0].CreatedAt, runs[0].CreatedAt
	for _, r := range runs {
		if r.CreatedAt.Before(minT) {
			minT = r.CreatedAt
		}
		if r.CreatedAt.After(maxT) {
			maxT = r.CreatedAt
		}
	}
	span := maxT.Sub(minT)
	type cell struct{ wf, tb int }
	wfIdx := map[string]int{}
	var cells = map[cell][]int{}
	for i, r := range runs {
		w, ok := wfIdx[r.WorkflowName]
		if !ok {
			w = len(wfIdx)
			wfIdx[r.WorkflowName] = w
		}
		tb := 0
		if span > 0 {
			tb = int(float64(r.CreatedAt.Sub(minT)) / float64(span) * float64(timeBuckets))
			if tb >= timeBuckets {
				tb = timeBuckets - 1
			}
		}
		cells[cell{w, tb}] = append(cells[cell{w, tb}], i)
	}
	groups := make([][]int, 0, len(cells))
	for _, g := range cells {
		groups = append(groups, g)
	}
	// Deterministic group order for reproducibility.
	sort.Slice(groups, func(i, j int) bool { return groups[i][0] < groups[j][0] })
	return drawProportional(groups, n, total, rng)
}

// drawProportional allocates n samples across groups proportionally (largest
// remainder), guaranteeing at least one sample per non-empty group when the
// budget allows, then draws uniformly within each group.
func drawProportional(groups [][]int, n, total int, rng *rand.Rand) []int {
	type alloc struct {
		gi    int
		want  float64
		given int
	}
	allocs := make([]alloc, 0, len(groups))
	given := 0
	for gi, g := range groups {
		if len(g) == 0 {
			continue
		}
		want := float64(len(g)) / float64(total) * float64(n)
		base := int(want)
		if base < 1 && len(allocs) < n {
			base = 1 // floor: every non-empty stratum gets one if budget allows
		}
		if base > len(g) {
			base = len(g)
		}
		allocs = append(allocs, alloc{gi, want, base})
		given += base
	}
	// Trim or top up to exactly n by largest remainder / largest excess.
	for given > n {
		bi, best := -1, -1.0
		for i, a := range allocs {
			excess := float64(a.given) - a.want
			if a.given > 1 && excess > best {
				best = excess
				bi = i
			}
		}
		if bi < 0 {
			break
		}
		allocs[bi].given--
		given--
	}
	for given < n {
		bi, best := -1, math.Inf(-1)
		for i, a := range allocs {
			if a.given >= len(groups[a.gi]) {
				continue
			}
			deficit := a.want - float64(a.given)
			if deficit > best {
				best = deficit
				bi = i
			}
		}
		if bi < 0 {
			break
		}
		allocs[bi].given++
		given++
	}
	var out []int
	for _, a := range allocs {
		g := groups[a.gi]
		perm := rng.Perm(len(g))
		for i := 0; i < a.given && i < len(g); i++ {
			out = append(out, g[perm[i]])
		}
	}
	sort.Ints(out)
	return out
}

// adaptivePerWorkflow samples each workflow independently: workflows holding
// >=1% of total compute time get min(N_w, majorTarget) runs, the rest
// min(N_w, minorTarget), drawn from 10 time strata within the workflow.
func adaptivePerWorkflow(runs []analyzer.RunData, seed int64, majorTarget, minorTarget int) []int {
	rng := rand.New(rand.NewSource(seed))
	byWF := map[string][]int{}
	compute := map[string]float64{}
	var totalCompute float64
	for i, r := range runs {
		byWF[r.WorkflowName] = append(byWF[r.WorkflowName], i)
		d := float64(r.Duration)
		compute[r.WorkflowName] += d
		totalCompute += d
	}
	var names []string
	for name := range byWF {
		names = append(names, name)
	}
	sort.Strings(names)
	var out []int
	for _, name := range names {
		idxs := byWF[name]
		target := minorTarget
		if totalCompute > 0 && compute[name]/totalCompute >= 0.01 {
			target = majorTarget
		}
		if target >= len(idxs) {
			out = append(out, idxs...)
			continue
		}
		// Time-stratify within the workflow for temporal coverage.
		sub := make([]analyzer.RunData, len(idxs))
		for i, idx := range idxs {
			sub[i] = runs[idx]
		}
		for _, si := range timeStratified(sub, target, rng.Int63(), 10) {
			out = append(out, idxs[si])
		}
	}
	sort.Ints(out)
	return out
}

// toWorkflowRuns synthesizes the WorkflowRun shape the production sampler
// consumes from dumped RunData.
func toWorkflowRuns(runs []analyzer.RunData) []githubapi.WorkflowRun {
	out := make([]githubapi.WorkflowRun, len(runs))
	for i, r := range runs {
		out[i] = githubapi.WorkflowRun{
			ID:        r.ID,
			Name:      r.WorkflowName,
			Status:    r.Status,
			CreatedAt: r.CreatedAt.Format(time.RFC3339),
			UpdatedAt: r.UpdatedAt.Format(time.RFC3339),
		}
	}
	return out
}

func seqIndices(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

// --- evaluation ---

type scored struct{ p50Errs, p95Errs, succErrs []float64 }

func evalDump(dump *analyzer.RunDump, trials int, margin float64) {
	// Truth over completed runs with job data.
	var runs []analyzer.RunData
	for _, r := range dump.Runs {
		if r.Status == "completed" && len(r.Jobs) > 0 {
			runs = append(runs, r)
		}
	}
	if len(runs) == 0 {
		fmt.Printf("%s/%s: no completed runs with job data — was this dumped with --no-sample?\n", dump.Owner, dump.Repo)
		return
	}
	all := seqIndices(len(runs))
	truth := jobStats(runs, all)
	truthHourly := hourlyQueueP50(runs, all)

	// Jobs worth scoring.
	var keys []jobKey
	for k, t := range truth {
		if t.obs >= minTruthObs {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].workflow != keys[j].workflow {
			return keys[i].workflow < keys[j].workflow
		}
		return keys[i].job < keys[j].job
	})

	baseN := cochran(len(runs), margin)
	if baseN < 80 {
		baseN = min(80, len(runs))
	}

	strategies := []strategy{
		{name: fmt.Sprintf("S0 current: global time-strata(10), n=%d", baseN),
			sample: func(r []analyzer.RunData, n int, seed int64) []int { return timeStratified(r, n, seed, 10) },
			sizeFn: func(r []analyzer.RunData, m float64) int { return baseN }},
		{name: fmt.Sprintf("S1 per-workflow x time(10), n=%d", baseN),
			sample: func(r []analyzer.RunData, n int, seed int64) []int { return workflowTimeStratified(r, n, seed, 10) },
			sizeFn: func(r []analyzer.RunData, m float64) int { return baseN }},
		{name: fmt.Sprintf("S2 per-workflow x hourly(56), n=%d", baseN),
			sample: func(r []analyzer.RunData, n int, seed int64) []int { return workflowTimeStratified(r, n, seed, 56) },
			sizeFn: func(r []analyzer.RunData, m float64) int { return baseN }},
		{name: fmt.Sprintf("S3 per-workflow x time(10), n=%d (2x)", baseN*2),
			sample: func(r []analyzer.RunData, n int, seed int64) []int { return workflowTimeStratified(r, n, seed, 10) },
			sizeFn: func(r []analyzer.RunData, m float64) int { return baseN * 2 }},
		{name: fmt.Sprintf("S4 per-workflow x time(10), n=%d (4x)", baseN*4),
			sample: func(r []analyzer.RunData, n int, seed int64) []int { return workflowTimeStratified(r, n, seed, 10) },
			sizeFn: func(r []analyzer.RunData, m float64) int { return baseN * 4 }},
	}
	// Grid of per-workflow (major, minor) observation targets — find the knee.
	for _, tg := range [][2]int{{15, 8}, {20, 10}, {25, 10}, {30, 10}, {40, 15}, {50, 20}, {50, 50}} {
		major, minor := tg[0], tg[1]
		strategies = append(strategies, strategy{
			name: fmt.Sprintf("grid: %d obs major / %d minor", major, minor),
			sample: func(r []analyzer.RunData, n int, seed int64) []int {
				return adaptivePerWorkflow(r, seed, major, minor)
			},
			sizeFn: func(r []analyzer.RunData, m float64) int { return len(adaptivePerWorkflow(r, 1, major, minor)) },
		})
	}
	// The actual production sampler (deterministic, so a single draw): parity check.
	strategies = append(strategies, strategy{
		name: "PROD analyzer.SelectSampleIndices (margin default)",
		sample: func(r []analyzer.RunData, n int, seed int64) []int {
			major, minor := analyzer.JobSampleTargets(0.10)
			return analyzer.SelectSampleIndices(toWorkflowRuns(r), major, minor)
		},
		sizeFn: func(r []analyzer.RunData, m float64) int {
			major, minor := analyzer.JobSampleTargets(m)
			return len(analyzer.SelectSampleIndices(toWorkflowRuns(r), major, minor))
		},
	})

	fmt.Printf("\n=== %s/%s — %d completed runs with jobs, %d scoreable jobs (>=%d obs), %d trials ===\n",
		dump.Owner, dump.Repo, len(runs), len(keys), minTruthObs, trials)
	fmt.Printf("%-44s %7s %7s %7s %7s %7s %7s %7s\n",
		"strategy", "n", "p50@50", "p50@90", "p95@50", "p95@90", "miss%", "hr-cor")

	for _, st := range strategies {
		n := st.sizeFn(runs, margin)
		if n > len(runs) {
			n = len(runs)
		}
		perJob := make(map[jobKey]*scored, len(keys))
		for _, k := range keys {
			perJob[k] = &scored{}
		}
		missing := 0
		var hourCors []float64

		for trial := 0; trial < trials; trial++ {
			idx := st.sample(runs, n, int64(trial)*7919+1)
			est := jobStats(runs, idx)
			for _, k := range keys {
				t := truth[k]
				e, ok := est[k]
				if !ok || e.obs == 0 {
					missing++
					continue
				}
				s := perJob[k]
				if t.p50 > 0 {
					s.p50Errs = append(s.p50Errs, math.Abs(e.p50-t.p50)/t.p50)
				}
				if t.p95 > 0 {
					s.p95Errs = append(s.p95Errs, math.Abs(e.p95-t.p95)/t.p95)
				}
				s.succErrs = append(s.succErrs, math.Abs(e.successRate-t.successRate))
			}
			if c := pearson(truthHourly, hourlyQueueP50(runs, idx)); !math.IsNaN(c) {
				hourCors = append(hourCors, c)
			}
		}

		// Per-job p90-of-trials error, then quantiles across jobs.
		var p50at90s, p95at90s []float64
		for _, k := range keys {
			s := perJob[k]
			sort.Float64s(s.p50Errs)
			sort.Float64s(s.p95Errs)
			if len(s.p50Errs) > 0 {
				p50at90s = append(p50at90s, percentile(s.p50Errs, 90))
			}
			if len(s.p95Errs) > 0 {
				p95at90s = append(p95at90s, percentile(s.p95Errs, 90))
			}
		}
		sort.Float64s(p50at90s)
		sort.Float64s(p95at90s)
		sort.Float64s(hourCors)
		missRate := float64(missing) / float64(trials*len(keys)) * 100

		fmt.Printf("%-44s %7d %6.1f%% %6.1f%% %6.1f%% %6.1f%% %6.1f%% %7.2f\n",
			st.name, n,
			percentile(p50at90s, 50)*100, percentile(p50at90s, 90)*100,
			percentile(p95at90s, 50)*100, percentile(p95at90s, 90)*100,
			missRate, percentile(hourCors, 50))
	}
	fmt.Println("\n  p50@50/p50@90: median/p90 across jobs of each job's 90th-percentile-of-trials |relative error| in duration p50.")
	fmt.Println("  p95@50/p95@90: same for duration p95. miss%: job absent from sample. hr-cor: median Pearson r of hourly queue-time p50 vs truth.")
}
