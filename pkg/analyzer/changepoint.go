package analyzer

import (
	"math"
	"sort"
)

// chiSq1at95 is the 95th percentile of the chi-square distribution with one
// degree of freedom — the likelihood-ratio threshold for the single-changepoint
// location interval.
const chiSq1at95 = 3.8414588

// significanceAlpha gates whether a located changepoint is real enough to
// report. Below this two-sided p-value we trust the shift; above it we don't
// pretend to have found one.
const significanceAlpha = 0.05

// locateChangepoint finds the most likely single changepoint in a chronological
// series and quantifies it: the maximum-likelihood split, a Mann-Whitney
// significance p-value for that split, and the 95% likelihood-ratio interval on
// the split location [lo, hi].
//
// It operates on log values so it localizes *multiplicative* shifts (a job that
// goes 30% slower) and isn't dominated by a single outlier run; the log scale
// also makes the Gaussian likelihood model appropriate for right-skewed CI
// durations. Splits are 1-based: split i means "between values[i-1] and
// values[i]". dir constrains the shift direction so the located changepoint
// agrees with the regression/improvement it explains: +1 requires an increase
// (got slower), -1 a decrease (got faster), 0 either. ok is false when there
// aren't enough points or no split matches the requested direction.
func locateChangepoint(values []float64, minSideSize, dir int) (best int, pValue float64, lo, hi int, ok bool) {
	n := len(values)
	if minSideSize < 1 {
		minSideSize = 1
	}
	if n < 2*minSideSize {
		return 0, 1, 0, 0, false
	}

	logv := make([]float64, n)
	for i, v := range values {
		if v <= 0 {
			v = 1e-9
		}
		logv[i] = math.Log(v)
	}

	// Prefix sums make each split's sum-of-squared-residuals O(1):
	// SSR = Σx² - (Σx)²/m per side, so the whole scan is O(n).
	prefixSum := make([]float64, n+1)
	prefixSq := make([]float64, n+1)
	rawSum := make([]float64, n+1)
	for i, x := range logv {
		prefixSum[i+1] = prefixSum[i] + x
		prefixSq[i+1] = prefixSq[i] + x*x
		rawSum[i+1] = rawSum[i] + values[i]
	}
	ssrAt := func(i int) float64 {
		leftSum := prefixSum[i]
		left := prefixSq[i] - leftSum*leftSum/float64(i)
		rightSum := prefixSum[n] - prefixSum[i]
		right := (prefixSq[n] - prefixSq[i]) - rightSum*rightSum/float64(n-i)
		return left + right
	}
	// matchesDir reports whether split i shifts in the requested direction.
	// It compares the *arithmetic* means of the raw values (not the log/SSR
	// means used for localization) so the direction agrees with the reported
	// before/after averages and the regression/improvement classification —
	// they can disagree when one side carries outliers.
	matchesDir := func(i int) bool {
		if dir == 0 {
			return true
		}
		leftMean := rawSum[i] / float64(i)
		rightMean := (rawSum[n] - rawSum[i]) / float64(n-i)
		if dir > 0 {
			return rightMean > leftMean
		}
		return rightMean < leftMean
	}

	bestSSR := math.MaxFloat64
	best = -1
	for i := minSideSize; i <= n-minSideSize; i++ {
		if !matchesDir(i) {
			continue
		}
		if s := ssrAt(i); s < bestSSR {
			bestSSR, best = s, i
		}
	}
	if best < 0 {
		return 0, 1, 0, 0, false
	}

	// 95% likelihood-ratio interval on the location: a split i is in the
	// interval when -2·Δlog-likelihood ≤ χ²₁(0.95). For the Gaussian model
	// that is n·log(SSR(i)/SSR_min) ≤ χ², i.e. SSR(i) ≤ SSR_min·exp(χ²/n).
	// The window stays within same-direction splits.
	threshold := bestSSR * math.Exp(chiSq1at95/float64(n))
	lo, hi = best, best
	for i := best - 1; i >= minSideSize && matchesDir(i) && ssrAt(i) <= threshold; i-- {
		lo = i
	}
	for i := best + 1; i <= n-minSideSize && matchesDir(i) && ssrAt(i) <= threshold; i++ {
		hi = i
	}

	pValue = mannWhitneyP(values[:best], values[best:])
	return best, pValue, lo, hi, true
}

// mannWhitneyP returns the two-sided p-value of the Mann-Whitney U test (a.k.a.
// Wilcoxon rank-sum) for whether two samples come from the same distribution,
// using the normal approximation with tie correction. It is rank-based, so it
// is robust to the heavy right tail of CI job durations and invariant to the
// log transform used for localization. Returns 1 (no evidence) for empty or
// constant inputs.
func mannWhitneyP(a, b []float64) float64 {
	n1, n2 := len(a), len(b)
	if n1 == 0 || n2 == 0 {
		return 1
	}

	type valGroup struct {
		v float64
		g int // 0 = a, 1 = b
	}
	all := make([]valGroup, 0, n1+n2)
	for _, x := range a {
		all = append(all, valGroup{x, 0})
	}
	for _, x := range b {
		all = append(all, valGroup{x, 1})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].v < all[j].v })

	// Average ranks within tie groups; accumulate the tie-correction term Σ(t³−t).
	ranks := make([]float64, len(all))
	var tieTerm float64
	for i := 0; i < len(all); {
		j := i
		for j < len(all) && all[j].v == all[i].v {
			j++
		}
		avgRank := (float64(i+1) + float64(j)) / 2 // positions i+1..j (1-based)
		for k := i; k < j; k++ {
			ranks[k] = avgRank
		}
		t := float64(j - i)
		tieTerm += t*t*t - t
		i = j
	}

	var rankSumA float64
	for k, it := range all {
		if it.g == 0 {
			rankSumA += ranks[k]
		}
	}

	nn1, nn2 := float64(n1), float64(n2)
	N := nn1 + nn2
	u := rankSumA - nn1*(nn1+1)/2
	meanU := nn1 * nn2 / 2
	variance := nn1 * nn2 / 12 * ((N + 1) - tieTerm/(N*(N-1)))
	if variance <= 0 {
		return 1 // all values tied → no separation
	}
	// Continuity-corrected z, two-sided.
	z := (math.Abs(u-meanU) - 0.5) / math.Sqrt(variance)
	if z < 0 {
		z = 0
	}
	p := 2 * (1 - normalCDF(z))
	if p > 1 {
		p = 1
	}
	return p
}

func normalCDF(x float64) float64 {
	return 0.5 * (1 + math.Erf(x/math.Sqrt2))
}

// populateChangepointURLs fills the GitHub compare URLs for a changepoint's
// point boundary and localization window. No-op when cp is nil.
func populateChangepointURLs(cp *Changepoint, owner, repo string) {
	if cp == nil {
		return
	}
	compare := func(a, b string) string {
		return "https://github.com/" + owner + "/" + repo + "/compare/" + a + "..." + b
	}
	cp.DiffURL = compare(cp.BeforeSHA, cp.AfterSHA)
	if cp.RangeStartSHA != "" && cp.RangeEndSHA != "" {
		cp.RangeDiffURL = compare(cp.RangeStartSHA, cp.RangeEndSHA)
	}
}

func confidenceLabel(p float64) string {
	switch {
	case p < 0.001:
		return "very high"
	case p < 0.01:
		return "high"
	default:
		return "medium"
	}
}
