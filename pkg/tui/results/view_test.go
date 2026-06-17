package results

import (
	"testing"
	"unicode/utf8"

	"github.com/charmbracelet/lipgloss"
	"github.com/stretchr/testify/assert"
)

func TestHighlightMatch(t *testing.T) {
	t.Parallel()

	// Plain styles so rendered output equals the input text
	plain := lipgloss.NewStyle()

	t.Run("simple case-insensitive match", func(t *testing.T) {
		result := highlightMatch("Hello World", "world", plain, plain)
		assert.Equal(t, "Hello World", result)
	})

	t.Run("no match returns name unchanged", func(t *testing.T) {
		result := highlightMatch("Hello", "zzz", plain, plain)
		assert.Equal(t, "Hello", result)
	})

	t.Run("does not panic when lowercasing grows byte length", func(t *testing.T) {
		// 'Ⱥ' (U+023A) is 2 bytes but its lowercase 'ⱥ' (U+2C65) is 3 bytes,
		// so a byte index computed against strings.ToLower(name) would
		// overrun the original name and panic.
		name := "ȺȺȺȺx"
		result := highlightMatch(name, "x", plain, plain)
		assert.Equal(t, name, result)
		assert.True(t, utf8.ValidString(result))
	})

	t.Run("does not misalign when lowercasing shrinks byte length", func(t *testing.T) {
		// 'K' (U+212A, Kelvin sign) is 3 bytes but lowercases to 'k' (1 byte).
		name := "aKbc"
		result := highlightMatch(name, "kb", plain, plain)
		assert.Equal(t, name, result)
		assert.True(t, utf8.ValidString(result))
	})

	t.Run("multibyte query matches without splitting runes", func(t *testing.T) {
		name := "straße"
		result := highlightMatch(name, "AßE", plain, plain)
		assert.Equal(t, name, result)
		assert.True(t, utf8.ValidString(result))
	})
}

func TestRuneIndex(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 0, runeIndex([]rune("abc"), []rune("")))
	assert.Equal(t, 0, runeIndex([]rune("abc"), []rune("a")))
	assert.Equal(t, 2, runeIndex([]rune("abc"), []rune("c")))
	assert.Equal(t, -1, runeIndex([]rune("abc"), []rune("d")))
	assert.Equal(t, -1, runeIndex([]rune("ab"), []rune("abc")))
	assert.Equal(t, 1, runeIndex([]rune("日本語"), []rune("本語")))
}

func TestRenderInspectorLineTruncatesRuneSafely(t *testing.T) {
	t.Parallel()

	longCJK := ""
	for i := 0; i < 60; i++ {
		longCJK += "日本語"
	}

	t.Run("value with label", func(t *testing.T) {
		entry := FlatInspectorEntry{Node: &InspectorNode{Label: "msg", Value: longCJK}}
		line := renderInspectorLine(entry, false, 50, "")
		assert.True(t, utf8.ValidString(line))
	})

	t.Run("value only", func(t *testing.T) {
		entry := FlatInspectorEntry{Node: &InspectorNode{Value: longCJK}}
		line := renderInspectorLine(entry, false, 50, "")
		assert.True(t, utf8.ValidString(line))
	})
}

func TestRenderInspectorBreadcrumbTruncatesRuneSafely(t *testing.T) {
	t.Parallel()

	m := createTestModel()
	longName := "ジョブの名前がとても長いテストケースです"
	m.modalItem = &TreeItem{DisplayName: longName}
	m.inspectorBreadcrumb = []inspectorBreadcrumbEntry{
		{item: &TreeItem{DisplayName: longName}},
	}
	crumb := m.renderInspectorBreadcrumb()
	assert.True(t, utf8.ValidString(crumb))
}
