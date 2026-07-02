package output

import (
	"sort"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
)

func sortURLResults(results []analyzer.URLResult) {
	sort.Slice(results, func(i, j int) bool {
		return results[i].EarliestTime < results[j].EarliestTime
	})
}

func requiredEmoji(isRequired bool) string {
	if isRequired {
		return " 🔒"
	}
	return ""
}
