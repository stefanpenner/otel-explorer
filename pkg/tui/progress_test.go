package tui

import (
	"strings"
	"testing"

	"github.com/charmbracelet/bubbles/spinner"
)

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
