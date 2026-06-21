// Package diff renders an interactive, navigable view of a semantic trace
// diff: a scrollable +/-/~ tree with a detail pane, the TUI counterpart of
// output.RenderTraceDiff.
package diff

import (
	"fmt"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/stefanpenner/otel-explorer/pkg/analyzer"
)

// Tokyo Night-ish palette, matching the rest of the TUI.
var (
	colTitle   = lipgloss.Color("#bb9af7")
	colGreen   = lipgloss.Color("#9ece6a")
	colRed     = lipgloss.Color("#f7768e")
	colYellow  = lipgloss.Color("#e0af68")
	colGray    = lipgloss.Color("#565f89")
	colWhite   = lipgloss.Color("#c0caf5")
	colSelBg   = lipgloss.Color("#283457")
	colBlue    = lipgloss.Color("#7aa2f7")
	titleStyle = lipgloss.NewStyle().Bold(true).Foreground(colTitle)
	dimStyle   = lipgloss.NewStyle().Foreground(colGray)
	okStyle    = lipgloss.NewStyle().Foreground(colGreen)
	badStyle   = lipgloss.NewStyle().Foreground(colRed)
	warnStyle  = lipgloss.NewStyle().Foreground(colYellow)
	headStyle  = lipgloss.NewStyle().Bold(true).Foreground(colBlue)
	whiteStyle = lipgloss.NewStyle().Foreground(colWhite)
	selStyle   = lipgloss.NewStyle().Background(colSelBg)
)

// Model is the bubbletea model for the diff view.
type Model struct {
	diff        *analyzer.TraceDiff
	beforeLabel string
	afterLabel  string

	hideUnchanged bool
	rows          []analyzer.FlatDiffNode // current (possibly filtered) view
	cursor        int
	top           int // first visible row index
	width         int
	height        int
	showHelp      bool
}

// NewModel builds a diff Model. It starts focused on the changes (unchanged
// subtrees hidden), since that is what a diff is for.
func NewModel(d *analyzer.TraceDiff, beforeLabel, afterLabel string) Model {
	m := Model{
		diff:          d,
		beforeLabel:   beforeLabel,
		afterLabel:    afterLabel,
		hideUnchanged: true,
		width:         80,
		height:        24,
	}
	m.rows = d.Flatten(true)
	return m
}

func (m Model) Init() tea.Cmd { return nil }

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.clampScroll()
		return m, nil
	case tea.KeyMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

func (m Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch {
	case key.Matches(msg, keyQuit):
		return m, tea.Quit
	case key.Matches(msg, keyHelp):
		m.showHelp = !m.showHelp
		return m, nil
	case key.Matches(msg, keyDown):
		m.move(1)
	case key.Matches(msg, keyUp):
		m.move(-1)
	case key.Matches(msg, keyPageDown):
		m.move(m.bodyHeight())
	case key.Matches(msg, keyPageUp):
		m.move(-m.bodyHeight())
	case key.Matches(msg, keyTop):
		m.cursor = 0
		m.clampScroll()
	case key.Matches(msg, keyBottom):
		m.cursor = len(m.rows) - 1
		m.clampScroll()
	case key.Matches(msg, keyNextChange):
		m.jumpChange(1)
	case key.Matches(msg, keyPrevChange):
		m.jumpChange(-1)
	case key.Matches(msg, keyToggle):
		m.toggleUnchanged()
	}
	return m, nil
}

func (m *Model) move(delta int) {
	if len(m.rows) == 0 {
		return
	}
	m.cursor += delta
	if m.cursor < 0 {
		m.cursor = 0
	}
	if m.cursor >= len(m.rows) {
		m.cursor = len(m.rows) - 1
	}
	m.clampScroll()
}

// jumpChange moves the cursor to the next/previous materially-changed row
// (skipping unchanged context rows when they are shown).
func (m *Model) jumpChange(dir int) {
	for i := m.cursor + dir; i >= 0 && i < len(m.rows); i += dir {
		if m.rows[i].Node.Kind != analyzer.Unchanged {
			m.cursor = i
			m.clampScroll()
			return
		}
	}
}

func (m *Model) toggleUnchanged() {
	// Anchor candidates: the cursor node, then each predecessor, so that when
	// hiding unchanged context drops the row under the cursor we land on the
	// nearest still-visible row instead of jumping to the top.
	var anchors []*analyzer.DiffNode
	for i := m.cursor; i >= 0 && i < len(m.rows); i-- {
		anchors = append(anchors, m.rows[i].Node)
	}
	m.hideUnchanged = !m.hideUnchanged
	m.rows = m.diff.Flatten(m.hideUnchanged)

	index := make(map[*analyzer.DiffNode]int, len(m.rows))
	for i, r := range m.rows {
		index[r.Node] = i
	}
	m.cursor = 0
	for _, a := range anchors {
		if i, ok := index[a]; ok {
			m.cursor = i
			break
		}
	}
	m.clampScroll()
}

func (m *Model) clampScroll() {
	if len(m.rows) == 0 {
		m.cursor, m.top = 0, 0
		return
	}
	// Clamp the cursor first: navigation (e.g. go-to-bottom on an empty view)
	// can drive it out of range, and a negative cursor would panic any row
	// lookup downstream.
	if m.cursor < 0 {
		m.cursor = 0
	}
	if m.cursor >= len(m.rows) {
		m.cursor = len(m.rows) - 1
	}
	h := m.bodyHeight()
	if h <= 0 {
		m.top = 0
		return
	}
	if m.cursor < m.top {
		m.top = m.cursor
	}
	if m.cursor >= m.top+h {
		m.top = m.cursor - h + 1
	}
	if m.top < 0 {
		m.top = 0
	}
}

// bodyHeight is the number of tree rows visible between header and footer.
func (m Model) bodyHeight() int {
	h := m.height - headerLines - footerLines
	if h < 1 {
		return 1
	}
	return h
}

const (
	headerLines = 4 // title, before, after, rule
	footerLines = 3 // rule, hint, detail
)

// Run launches the interactive diff TUI.
func Run(d *analyzer.TraceDiff, beforeLabel, afterLabel string) error {
	p := tea.NewProgram(NewModel(d, beforeLabel, afterLabel), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		return fmt.Errorf("diff TUI failed: %w", err)
	}
	return nil
}
