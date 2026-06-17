package perfetto

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetUIOrigin(t *testing.T) {
	t.Cleanup(func() { uiOrigin = DefaultUIOrigin })

	assert.Equal(t, DefaultUIOrigin, UIOrigin(), "defaults to the public Perfetto UI")

	SetUIOrigin("https://perfetto.internal.example.com")
	assert.Equal(t, "https://perfetto.internal.example.com", UIOrigin())

	SetUIOrigin("") // empty is ignored, keeps the current origin
	assert.Equal(t, "https://perfetto.internal.example.com", UIOrigin())
}

func TestOpenTraceArgs_PassesOrigin(t *testing.T) {
	args := openTraceArgs("/tmp/open_trace_in_ui", "/tmp/run.pftrace", "https://perfetto.internal.example.com")
	assert.Equal(t, []string{
		"/tmp/open_trace_in_ui",
		"--origin", "https://perfetto.internal.example.com",
		"/tmp/run.pftrace",
	}, args)
}
