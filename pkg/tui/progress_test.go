package tui

import (
	"strings"
	"testing"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
)

func TestProgressCtrlCInterruptsAndQuits(t *testing.T) {
	interrupted := false
	m := progressModel{
		totalURLs: 1, currentURL: 1, spinner: spinner.New(),
		interrupt: func() { interrupted = true },
	}
	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyCtrlC})
	if !interrupted {
		t.Fatal("ctrl+c should invoke the interrupt handler so in-flight work is cancelled")
	}
	if cmd == nil {
		t.Fatal("ctrl+c should return a command to quit the spinner")
	}
	if _, ok := cmd().(tea.QuitMsg); !ok {
		t.Fatalf("ctrl+c should quit the program, got %T", cmd())
	}
}

func TestProgressCtrlCQuitsWithoutHandler(t *testing.T) {
	// No interrupt handler wired yet: ctrl+c must still tear down the spinner
	// rather than be swallowed.
	m := progressModel{totalURLs: 1, currentURL: 1, spinner: spinner.New()}
	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyCtrlC})
	if cmd == nil {
		t.Fatal("ctrl+c should quit even without an interrupt handler")
	}
	if _, ok := cmd().(tea.QuitMsg); !ok {
		t.Fatalf("ctrl+c should quit the program, got %T", cmd())
	}
}

func TestProgressOtherKeysIgnored(t *testing.T) {
	m := progressModel{totalURLs: 1, currentURL: 1, spinner: spinner.New()}
	if _, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'x'}}); cmd != nil {
		t.Fatal("non-ctrl+c keys should be ignored, not quit")
	}
}

func TestProgressViewShowsAPIMeter(t *testing.T) {
	m := progressModel{
		totalURLs: 1, currentURL: 1, spinner: spinner.New(),
		statsFunc: func() (int, int) { return 47, 4612 },
	}
	out := m.View()
	if !strings.Contains(out, "47 API reqs") {
		t.Errorf("expected live request count, got:\n%s", out)
	}
	if !strings.Contains(out, "4612 left") {
		t.Errorf("expected remaining rate limit, got:\n%s", out)
	}
}

func TestProgressViewOmitsRemainingWhenUnknown(t *testing.T) {
	m := progressModel{
		totalURLs: 1, currentURL: 1, spinner: spinner.New(),
		statsFunc: func() (int, int) { return 5, -1 },
	}
	out := m.View()
	if !strings.Contains(out, "5 API reqs") {
		t.Errorf("expected request count, got:\n%s", out)
	}
	if strings.Contains(out, "left") {
		t.Errorf("should omit remaining when unknown, got:\n%s", out)
	}
}

func TestProgressViewNoMeterBeforeFirstRequest(t *testing.T) {
	m := progressModel{
		totalURLs: 1, currentURL: 1, spinner: spinner.New(),
		statsFunc: func() (int, int) { return 0, -1 },
	}
	if strings.Contains(m.View(), "API reqs") {
		t.Errorf("should not show meter at zero requests, got:\n%s", m.View())
	}
}
