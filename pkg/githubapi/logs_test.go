package githubapi

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func splitTestSteps() []Step {
	return []Step{
		{Number: 1, StartedAt: "2024-01-15T10:00:00Z", CompletedAt: "2024-01-15T10:00:59Z"},
		{Number: 2, StartedAt: "2024-01-15T10:01:00Z", CompletedAt: "2024-01-15T10:01:59Z"},
		{Number: 3, StartedAt: "2024-01-15T10:02:00Z", CompletedAt: "2024-01-15T10:02:59Z"},
	}
}

func TestSplitJobLogByStepBasic(t *testing.T) {
	log := strings.Join([]string{
		"2024-01-15T10:00:01.0000000Z step one line",
		"2024-01-15T10:01:01.0000000Z step two line",
		"2024-01-15T10:02:01.0000000Z step three line",
	}, "\n")

	out := SplitJobLogByStep([]byte(log), splitTestSteps())
	assert.Contains(t, string(out[1]), "step one line")
	assert.Contains(t, string(out[2]), "step two line")
	assert.Contains(t, string(out[3]), "step three line")
}

func TestSplitJobLogByStepHandlesLinesOver64KB(t *testing.T) {
	longLine := "2024-01-15T10:00:02.0000000Z " + strings.Repeat("x", 70*1024)
	log := strings.Join([]string{
		"2024-01-15T10:00:01.0000000Z before long line",
		longLine,
		"2024-01-15T10:01:01.0000000Z after long line",
	}, "\n")

	out := SplitJobLogByStep([]byte(log), splitTestSteps())
	assert.NotEmpty(t, out, "a >64KB line must not abort the entire split")
	assert.Contains(t, string(out[1]), "before long line")
	assert.Contains(t, string(out[1]), strings.Repeat("x", 70*1024))
	assert.Contains(t, string(out[2]), "after long line")
}

func TestSplitJobLogByStepShortLineAttachesToMostRecentStep(t *testing.T) {
	log := strings.Join([]string{
		"2024-01-15T10:00:01.0000000Z in step one",
		"ok", // short, untimestamped line emitted during step 1
		"2024-01-15T10:00:02.0000000Z still in step one",
		"2024-01-15T10:02:01.0000000Z in step three",
	}, "\n")

	out := SplitJobLogByStep([]byte(log), splitTestSteps())
	assert.Contains(t, string(out[1]), "ok", "short line should attach to the most recently matched step")
	assert.NotContains(t, string(out[3]), "ok", "short line must not be misrouted to the last step")
}

func TestSplitJobLogByStepUnparseableLineAttachesToMostRecentStep(t *testing.T) {
	untimestamped := "this line is long enough but has no leading timestamp prefix"
	log := strings.Join([]string{
		"2024-01-15T10:01:01.0000000Z in step two",
		untimestamped,
		"2024-01-15T10:02:01.0000000Z in step three",
	}, "\n")

	out := SplitJobLogByStep([]byte(log), splitTestSteps())
	assert.Contains(t, string(out[2]), untimestamped, "untimestamped line should attach to the most recently matched step, not be dropped")
}
