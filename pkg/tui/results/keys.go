package results

import "github.com/charmbracelet/bubbles/key"

// KeyMap defines all key bindings for the TUI
type KeyMap struct {
	Up              key.Binding
	Down            key.Binding
	ShiftUp         key.Binding
	ShiftDown       key.Binding
	Left            key.Binding
	Right           key.Binding
	Enter           key.Binding
	Space           key.Binding
	Open            key.Binding
	Info            key.Binding
	Focus           key.Binding
	Reload          key.Binding
	ToggleExpandAll key.Binding
	Perfetto        key.Binding
	Search          key.Binding
	Mouse           key.Binding
	GoTop           key.Binding
	GoBottom        key.Binding
	LogicalEnd      key.Binding
	Sort            key.Binding
	ResizeLeft      key.Binding
	ResizeRight     key.Binding
	Logs            key.Binding
	NextFailed      key.Binding
	NextBottleneck  key.Binding
	PageUp          key.Binding
	PageDown        key.Binding
	Help            key.Binding
	Quit            key.Binding
}

// DefaultKeyMap returns the default key bindings
func DefaultKeyMap() KeyMap {
	return KeyMap{
		Up: key.NewBinding(
			key.WithKeys("up", "k"),
			key.WithHelp("↑/k", "up"),
		),
		Down: key.NewBinding(
			key.WithKeys("down", "j"),
			key.WithHelp("↓/j", "down"),
		),
		ShiftUp: key.NewBinding(
			key.WithKeys("shift+up", "K"),
			key.WithHelp("shift+↑", "select up"),
		),
		ShiftDown: key.NewBinding(
			key.WithKeys("shift+down", "J"),
			key.WithHelp("shift+↓", "select down"),
		),
		Left: key.NewBinding(
			key.WithKeys("left", "h"),
			key.WithHelp("←/h", "collapse"),
		),
		Right: key.NewBinding(
			key.WithKeys("right", "l"),
			key.WithHelp("→/l", "expand"),
		),
		Enter: key.NewBinding(
			key.WithKeys("enter"),
			key.WithHelp("enter", "toggle"),
		),
		Space: key.NewBinding(
			key.WithKeys(" "),
			key.WithHelp("space", "toggle chart"),
		),
		Open: key.NewBinding(
			key.WithKeys("o"),
			key.WithHelp("o", "open"),
		),
		Info: key.NewBinding(
			key.WithKeys("i"),
			key.WithHelp("i", "info"),
		),
		Focus: key.NewBinding(
			key.WithKeys("f"),
			key.WithHelp("f", "focus"),
		),
		Reload: key.NewBinding(
			key.WithKeys("r"),
			key.WithHelp("r", "reload"),
		),
		ToggleExpandAll: key.NewBinding(
			key.WithKeys("c"),
			key.WithHelp("c", "expand/collapse all"),
		),
		Perfetto: key.NewBinding(
			key.WithKeys("p"),
			key.WithHelp("p", "perfetto"),
		),
		Search: key.NewBinding(
			key.WithKeys("/"),
			key.WithHelp("/", "search"),
		),
		Mouse: key.NewBinding(
			key.WithKeys("m"),
			key.WithHelp("m", "toggle mouse"),
		),
		GoTop: key.NewBinding(
			key.WithKeys("g"),
			key.WithHelp("gg", "go to top"),
		),
		GoBottom: key.NewBinding(
			key.WithKeys("G"),
			key.WithHelp("GG", "go to bottom"),
		),
		LogicalEnd: key.NewBinding(
			key.WithKeys("e"),
			key.WithHelp("e", "mark end"),
		),
		Sort: key.NewBinding(
			key.WithKeys("s"),
			key.WithHelp("s", "cycle sort"),
		),
		ResizeLeft: key.NewBinding(
			key.WithKeys("["),
			key.WithHelp("[", "narrow tree"),
		),
		ResizeRight: key.NewBinding(
			key.WithKeys("]"),
			key.WithHelp("]", "widen tree"),
		),
		Logs: key.NewBinding(
			key.WithKeys("L"),
			key.WithHelp("L", "fetch logs"),
		),
		NextFailed: key.NewBinding(
			key.WithKeys("n"),
			key.WithHelp("n", "next failed"),
		),
		NextBottleneck: key.NewBinding(
			key.WithKeys("N"),
			key.WithHelp("N", "next bottleneck"),
		),
		PageUp: key.NewBinding(
			key.WithKeys("ctrl+u", "pgup"),
			key.WithHelp("ctrl+u", "page up"),
		),
		PageDown: key.NewBinding(
			key.WithKeys("ctrl+d", "pgdown"),
			key.WithHelp("ctrl+d", "page down"),
		),
		Help: key.NewBinding(
			key.WithKeys("?"),
			key.WithHelp("?", "help"),
		),
		Quit: key.NewBinding(
			key.WithKeys("q", "ctrl+c"),
			key.WithHelp("q", "quit"),
		),
	}
}

// HelpMode represents the current UI context for help text
type HelpMode int

const (
	HelpModeNormal HelpMode = iota
	HelpModeSearch
	HelpModeSearchActive // search filter active but not typing
	HelpModeModal
)

// ShortHelpForMode returns context-sensitive help for the footer
func (k KeyMap) ShortHelpForMode(mode HelpMode) string {
	switch mode {
	case HelpModeSearch:
		return "type to search • enter confirm • esc cancel"
	case HelpModeSearchActive:
		return "↑↓ nav • enter/esc clear • / new search • n/N jump • s sort • ? help • q quit"
	case HelpModeModal:
		return "Tab pane • ←→ expand • /search • c copy • o open • [/] item • esc close"
	default:
		return "↑↓ nav • ^u/^d page • n/N jump • s sort • [/] resize • / search • ? help"
	}
}

// ShortHelp returns a short help string for the footer (default mode)
func (k KeyMap) ShortHelp() string {
	return k.ShortHelpForMode(HelpModeNormal)
}

// FullHelp returns all key bindings for the help modal
func (k KeyMap) FullHelp() [][]string {
	return [][]string{
		{"↑/k", "Move up"},
		{"↓/j", "Move down"},
		{"shift+↑/K", "Select up"},
		{"shift+↓/J", "Select down"},
		{"ctrl+u/pgup", "Page up (half screen)"},
		{"ctrl+d/pgdn", "Page down (half screen)"},
		{"←/h", "Collapse / go to parent"},
		{"→/l", "Expand"},
		{"enter", "Toggle expand"},
		{"space", "Toggle chart visibility"},
		{"n", "Jump to next failed item"},
		{"N", "Jump to next bottleneck"},
		{"o", "Open in browser"},
		{"i", "Item info"},
		{"f", "Focus on selection"},
		{"gg", "Go to top"},
		{"GG", "Go to bottom"},
		{"c", "Toggle expand/collapse all"},
		{"e", "Mark logical end"},
		{"s", "Cycle sort (start/duration↓/duration↑)"},
		{"[/]", "Resize tree/timeline split"},


		{"L", "Fetch & parse step logs"},
		{"r", "Reload data"},
		{"p", "Open in Perfetto"},
		{"/", "Search/filter"},
		{"m", "Toggle mouse mode"},
		{"?", "Show this help"},
		{"q", "Quit"},
	}
}
