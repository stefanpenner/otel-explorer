package tui

import (
	"fmt"
	"io"
	"sync"

	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var (
	headerStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#7aa2f7"))
	infoStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#565f89"))
	urlStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#9ece6a")).
			Underline(true)
	phaseStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#7aa2f7"))
)

type Progress struct {
	program *tea.Program
	done    chan struct{}
	once    sync.Once
}

type startURLMsg struct {
	index int
	total int
	url   string
}

type setURLRunsMsg struct {
	count int
}

type setPhaseMsg struct {
	phase string
}

type setDetailMsg struct {
	detail string
}

type processRunMsg struct{}

type setStatsMsg struct {
	f func() (requests, remaining int)
}

type finishMsg struct{}

type progressModel struct {
	totalURLs      int
	currentURL     int
	currentRun     int
	currentURLRuns int
	currentURLText string
	phase          string
	detail         string
	done           bool
	spinner        spinner.Model
	statsFunc      func() (requests, remaining int)
}

func NewProgress(totalURLs int, output io.Writer) *Progress {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))
	model := progressModel{
		totalURLs: totalURLs,
		spinner:   s,
	}
	program := tea.NewProgram(model, tea.WithOutput(output))
	return &Progress{
		program: program,
		done:    make(chan struct{}),
	}
}

func (p *Progress) Start() {
	go func() {
		defer close(p.done)
		_, _ = p.program.Run()
	}()
}

func (p *Progress) Wait() {
	<-p.done
}

func (p *Progress) StartURL(index int, url string) {
	p.program.Send(startURLMsg{index: index, total: 0, url: url})
}

func (p *Progress) SetURLRuns(runCount int) {
	p.program.Send(setURLRunsMsg{count: runCount})
}

func (p *Progress) SetPhase(phase string) {
	p.program.Send(setPhaseMsg{phase: phase})
}

func (p *Progress) SetDetail(detail string) {
	p.program.Send(setDetailMsg{detail: detail})
}

func (p *Progress) ProcessRun() {
	p.program.Send(processRunMsg{})
}

// SetStatsProvider registers a callback the spinner polls each frame to show a
// live API-request count and remaining rate limit. remaining < 0 means unknown
// (not yet reported by GitHub) and is omitted from the display.
func (p *Progress) SetStatsProvider(f func() (requests, remaining int)) {
	p.program.Send(setStatsMsg{f: f})
}

func (p *Progress) Finish() {
	p.program.Send(finishMsg{})
	p.once.Do(func() {
		p.program.Quit()
	})
}

func (m progressModel) Init() tea.Cmd {
	return m.spinner.Tick
}

func (m progressModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch typed := msg.(type) {
	case startURLMsg:
		m.currentURL = typed.index + 1
		m.currentRun = 0
		m.currentURLText = typed.url
		return m, nil
	case setURLRunsMsg:
		m.currentURLRuns = typed.count
		return m, nil
	case setPhaseMsg:
		m.phase = typed.phase
		return m, nil
	case setDetailMsg:
		m.detail = typed.detail
		return m, nil
	case processRunMsg:
		m.currentRun++
		return m, nil
	case setStatsMsg:
		m.statsFunc = typed.f
		return m, nil
	case finishMsg:
		m.done = true
		return m, tea.Quit
	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(typed)
		return m, cmd
	}
	return m, nil
}

func (m progressModel) View() string {
	if m.done {
		return ""
	}

	header := headerStyle.Render(fmt.Sprintf("🚀 OTel Analyzer (%d/%d URLs)", m.currentURL, m.totalURLs))

	if m.statsFunc != nil {
		if reqs, remaining := m.statsFunc(); reqs > 0 {
			api := fmt.Sprintf(" · %d API reqs", reqs)
			if remaining >= 0 {
				api += fmt.Sprintf(" · %d left", remaining)
			}
			header += infoStyle.Render(api)
		}
	}

	runProgress := ""
	if m.currentURLRuns > 0 {
		runProgress = infoStyle.Render(fmt.Sprintf(" [%d/%d runs]", m.currentRun, m.currentURLRuns))
	}

	urlLine := ""
	if m.currentURLText != "" {
		urlLine = fmt.Sprintf("\n%s %s%s", m.spinner.View(), urlStyle.Render(m.currentURLText), runProgress)
	}

	statusLine := ""
	if m.phase != "" {
		detail := ""
		if m.detail != "" {
			detail = infoStyle.Render(fmt.Sprintf(" (%s)", m.detail))
		}
		statusLine = fmt.Sprintf("\n%s %s%s", infoStyle.Render("  ↳"), phaseStyle.Render(m.phase), detail)
	}

	return "\n" + header + urlLine + statusLine + "\n"
}
