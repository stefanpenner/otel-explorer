package export

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderSlack_RunBlockKit(t *testing.T) {
	rep := sampleRunReport()
	rep.Run.Runs[0].Jobs[1].Name = "test <danger> & co" // exercise escaping

	var buf bytes.Buffer
	require.NoError(t, RenderSlack(&buf, rep))

	var msg map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &msg), "valid Slack JSON payload")

	// Fallback text for notifications.
	assert.Contains(t, msg["text"], "CI Run Analysis")

	blocks, ok := msg["blocks"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, blocks)

	// First block is a header.
	first := blocks[0].(map[string]any)
	assert.Equal(t, "header", first["type"])

	types := map[string]int{}
	var allText strings.Builder
	for _, b := range blocks {
		bm := b.(map[string]any)
		types[bm["type"].(string)]++
		if tx, ok := bm["text"].(map[string]any); ok {
			allText.WriteString(tx["text"].(string))
			allText.WriteByte('\n')
		}
		if fs, ok := bm["fields"].([]any); ok {
			for _, f := range fs {
				allText.WriteString(f.(map[string]any)["text"].(string))
				allText.WriteByte('\n')
			}
		}
	}
	assert.Positive(t, types["section"], "has section blocks (KPI fields + findings)")
	assert.Equal(t, 1, types["divider"])

	s := allText.String()
	assert.Contains(t, s, "*Job success*") // KPI field, bold mrkdwn
	assert.Contains(t, s, "*Key findings*")
	assert.Contains(t, s, ":red_circle:", "failing run → bad-severity emoji")
	// Slack mrkdwn escaping of user content.
	assert.Contains(t, s, "&lt;danger&gt;")
	assert.Contains(t, s, "&amp;")
	assert.NotContains(t, s, "<danger>")
}

func TestRenderSlack_TrendsHasFindingsAndFields(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, RenderSlack(&buf, sampleTrendReport()))

	var msg slackMessage
	require.NoError(t, json.Unmarshal(buf.Bytes(), &msg))
	assert.Contains(t, msg.Text, "CI Trends")
	require.NotEmpty(t, msg.Blocks)
	assert.Equal(t, "header", msg.Blocks[0].Type)

	var joined string
	for _, b := range msg.Blocks {
		if b.Text != nil {
			joined += b.Text.Text + "\n"
		}
		for _, f := range b.Fields {
			joined += f.Text + "\n"
		}
	}
	assert.Contains(t, joined, "*Avg success*")
	assert.Contains(t, joined, "Key findings")
}

func TestRenderSlack_HeaderTruncatedTo150(t *testing.T) {
	rep := sampleRunReport()
	rep.Meta.Repo = strings.Repeat("x", 300)
	var buf bytes.Buffer
	require.NoError(t, RenderSlack(&buf, rep))
	var msg slackMessage
	require.NoError(t, json.Unmarshal(buf.Bytes(), &msg))
	header := msg.Blocks[0]
	require.Equal(t, "header", header.Type)
	// Slack hard-limits header plain_text to 150 chars.
	assert.LessOrEqual(t, len([]rune(header.Text.Text)), 150)
}

func TestTruncate_MultiByte(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		n     int
		want  string
	}{
		{name: "short passthrough", input: "abc", n: 80, want: "abc"},
		{name: "ascii truncate with ellipsis", input: "hello world", n: 8, want: "hello w…"},
		{name: "multibyte no split", input: "ビルドテストビルドテスト", n: 5, want: "ビルドテ…"},
		{name: "single rune", input: "ビルド", n: 1, want: "ビ"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := truncate(tc.input, tc.n)
			assert.Equal(t, tc.want, got)
			assert.True(t, utf8.ValidString(got), "result must be valid UTF-8")
		})
	}
}
