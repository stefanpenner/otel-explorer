package diff

import "github.com/charmbracelet/bubbles/key"

var (
	keyUp         = key.NewBinding(key.WithKeys("up", "k"), key.WithHelp("↑/k", "up"))
	keyDown       = key.NewBinding(key.WithKeys("down", "j"), key.WithHelp("↓/j", "down"))
	keyPageUp     = key.NewBinding(key.WithKeys("pgup", "ctrl+u"), key.WithHelp("pgup", "page up"))
	keyPageDown   = key.NewBinding(key.WithKeys("pgdown", "ctrl+d"), key.WithHelp("pgdn", "page down"))
	keyTop        = key.NewBinding(key.WithKeys("g", "home"), key.WithHelp("g", "top"))
	keyBottom     = key.NewBinding(key.WithKeys("G", "end"), key.WithHelp("G", "bottom"))
	keyNextChange = key.NewBinding(key.WithKeys("n"), key.WithHelp("n", "next change"))
	keyPrevChange = key.NewBinding(key.WithKeys("N"), key.WithHelp("N", "prev change"))
	keyToggle     = key.NewBinding(key.WithKeys("u"), key.WithHelp("u", "show/hide unchanged"))
	keyHelp       = key.NewBinding(key.WithKeys("?"), key.WithHelp("?", "help"))
	keyQuit       = key.NewBinding(key.WithKeys("q", "esc", "ctrl+c"), key.WithHelp("q", "quit"))
)
