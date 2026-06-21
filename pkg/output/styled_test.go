package output

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
	"github.com/stretchr/testify/assert"
)

func TestOutputStyledResultsMalformedSuccessRate(t *testing.T) {
	a := assert.New(t)
	var buf bytes.Buffer
	combined := analyzer.CombinedMetrics{
		TotalRuns:      5,
		TotalJobs:      10,
		SuccessRate:    "abc",
		JobSuccessRate: "xyz",
	}
	err := OutputStyledResults(&buf, nil, combined, nil, 0, 0, nil, nil)
	a.NoError(err)
	out := buf.String()
	a.True(strings.Contains(out, "–"), "malformed success rate should show '–', got:\n%s", out)
	a.False(strings.Contains(out, "abc%"), "malformed rate should not be rendered as 'abc%%'")
	a.False(strings.Contains(out, "xyz%"), "malformed job rate should not be rendered as 'xyz%%'")
}
