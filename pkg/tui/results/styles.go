package results

import "github.com/charmbracelet/lipgloss"

// Tokyo Night inspired color palette
var (
	ColorPurple      = lipgloss.Color("#bb9af7")
	ColorGreen       = lipgloss.Color("#9ece6a")
	ColorBlue        = lipgloss.Color("#7aa2f7")
	ColorRed         = lipgloss.Color("#f7768e")
	ColorYellow      = lipgloss.Color("#e0af68")
	ColorGray        = lipgloss.Color("#565f89")
	ColorGrayDim     = lipgloss.Color("#3b4261")
	ColorWhite       = lipgloss.Color("#c0caf5")
	ColorOffWhite    = lipgloss.Color("#a9b1d6")
	ColorMagenta     = lipgloss.Color("#bb9af7")
	ColorSelectionBg = lipgloss.Color("#283457")

	// Dimmed colors for selected state
	ColorGreenDim  = lipgloss.Color("#5a7a3a")
	ColorBlueDim   = lipgloss.Color("#4a6199")
	ColorRedDim    = lipgloss.Color("#994455")
	ColorYellowDim = lipgloss.Color("#8a6a3a")

	// Subtle background for duration labels inside bars
	ColorBarLabelBg = lipgloss.Color("#1a1b26")

	// Search match backgrounds
	ColorSearchRowBg  = lipgloss.Color("#1e2030")
	ColorSearchCharBg = lipgloss.Color("#2e2a50")

	// Indent guide color
	ColorIndentGuide = lipgloss.Color("#292e42")

	// Statusline segment colors
	ColorStatusMode  = lipgloss.Color("#1a1b26") // bg for mode pill
	ColorStatusModeN = lipgloss.Color("#7aa2f7") // NORMAL mode fg
	ColorStatusModeS = lipgloss.Color("#e0af68") // SEARCH mode fg
	ColorStatusModeI = lipgloss.Color("#bb9af7") // INFO/modal mode fg

	// Surface colors for layered backgrounds
	ColorSurface0 = lipgloss.Color("#1a1b26") // deepest
	ColorSurface1 = lipgloss.Color("#24283b") // main bg
	ColorSurface2 = lipgloss.Color("#292e42") // raised
)

// Header styles
var (
	HeaderStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(ColorBlue)

	HeaderCountStyle = lipgloss.NewStyle().
				Foreground(ColorGray)

	FooterStyle = lipgloss.NewStyle().
			Foreground(ColorGray)

	BorderStyle = lipgloss.NewStyle().
			Foreground(ColorGrayDim)

	SeparatorStyle = lipgloss.NewStyle().
			Foreground(ColorGrayDim)
)

// SourceHintColor returns the accent color for a provenance label shown in the
// tree (the " ← <source>" hint at a source boundary). Each emitter class gets a
// distinct color so boundaries are scannable at a glance:
//   - runner     -> blue   (native runner spans)
//   - github-api -> gray   (reconstructed from the REST API; de-emphasized)
//   - everything else (a propagated tool's service.name) -> purple
func SourceHintColor(label string) lipgloss.Color {
	switch label {
	case "runner":
		return ColorBlue
	case "github-api":
		return ColorGray
	default:
		return ColorMagenta
	}
}

// Tree item styles
var (
	SelectedStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(ColorWhite).
			Background(ColorSelectionBg)

	HiddenSelectedStyle = lipgloss.NewStyle().
				Foreground(ColorGray).
				Background(ColorSelectionBg)

	SelectedBgStyle = lipgloss.NewStyle().
			Background(ColorSelectionBg)

	NormalStyle = lipgloss.NewStyle()

	HiddenStyle = lipgloss.NewStyle().
			Foreground(ColorGray)

	// FocusDimStyle dims non-focused items during focus mode
	FocusDimStyle = lipgloss.NewStyle().
			Foreground(ColorGray)

	WorkflowStyle = lipgloss.NewStyle().
			Foreground(ColorBlue)

	GroupStyle = lipgloss.NewStyle().
			Foreground(ColorBlue)

	JobStyle = lipgloss.NewStyle()

	// Indent guide style
	IndentGuideStyle = lipgloss.NewStyle().
				Foreground(ColorIndentGuide)
)

// Status styles
var (
	SuccessStyle = lipgloss.NewStyle().
			Foreground(ColorGreen)

	FailureStyle = lipgloss.NewStyle().
			Foreground(ColorRed)

	PendingStyle = lipgloss.NewStyle().
			Foreground(ColorBlue)

	SkippedStyle = lipgloss.NewStyle().
			Foreground(ColorGray)
)

// Logical end badge style
var (
	LogicalEndBadgeStyle = lipgloss.NewStyle().
		Foreground(ColorYellow)
)

// Hidden badge style (shows when item is excluded from chart via x key)
var (
	HiddenBadgeStyle = lipgloss.NewStyle().
		Foreground(ColorGrayDim)
)

// Timeline bar colors
var (
	BarSuccessStyle = lipgloss.NewStyle().
			Foreground(ColorGreen)

	BarFailureStyle = lipgloss.NewStyle().
			Foreground(ColorRed)

	BarFailureNonBlockingStyle = lipgloss.NewStyle().
					Foreground(ColorYellow)

	BarPendingStyle = lipgloss.NewStyle().
			Foreground(ColorBlue)

	BarSkippedStyle = lipgloss.NewStyle().
			Foreground(ColorGray)
)

// Timeline bar colors for collapsed child markers
var (
	BarChildSuccessStyle = lipgloss.NewStyle().
				Foreground(ColorGreenDim)

	BarChildFailureStyle = lipgloss.NewStyle().
				Foreground(ColorRedDim)

	BarChildDefaultStyle = lipgloss.NewStyle().
				Foreground(ColorGrayDim)

	BarChildSuccessSelectedStyle = lipgloss.NewStyle().
					Foreground(ColorGreenDim).
					Background(ColorSelectionBg)

	BarChildFailureSelectedStyle = lipgloss.NewStyle().
					Foreground(ColorRedDim).
					Background(ColorSelectionBg)

	BarChildDefaultSelectedStyle = lipgloss.NewStyle().
					Foreground(ColorGrayDim).
					Background(ColorSelectionBg)
)

// Timeline bar colors for selected state
var (
	BarSuccessSelectedStyle = lipgloss.NewStyle().
				Foreground(ColorGreenDim).
				Background(ColorSelectionBg)

	BarFailureSelectedStyle = lipgloss.NewStyle().
				Foreground(ColorRedDim).
				Background(ColorSelectionBg)

	BarFailureNonBlockingSelectedStyle = lipgloss.NewStyle().
						Foreground(ColorYellowDim).
						Background(ColorSelectionBg)

	BarPendingSelectedStyle = lipgloss.NewStyle().
				Foreground(ColorBlueDim).
				Background(ColorSelectionBg)

	BarSkippedSelectedStyle = lipgloss.NewStyle().
				Foreground(ColorGrayDim).
				Background(ColorSelectionBg)
)

// Time header style
var (
	TimeHeaderStyle = lipgloss.NewStyle().
			Foreground(ColorGray)

	DurationStyle = lipgloss.NewStyle().
			Foreground(ColorGray)
)

// Search styles
var (
	SearchBarStyle = lipgloss.NewStyle().
			Foreground(ColorWhite)

	SearchRowStyle = lipgloss.NewStyle().
			Background(ColorSearchRowBg)

	SearchRowBgStyle = lipgloss.NewStyle().
				Background(ColorSearchRowBg)

	SearchCharStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(ColorPurple).
			Background(ColorSearchCharBg)

	SearchCharSelectedStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(ColorPurple).
				Background(ColorSelectionBg)

	SearchCountStyle = lipgloss.NewStyle().
				Foreground(ColorGray)
)

// Modal styles
var (
	ModalStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(ColorGrayDim).
			Padding(1, 2)

	ModalTitleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(ColorBlue)

	ModalLabelStyle = lipgloss.NewStyle().
			Foreground(ColorGray).
			Width(14)

	ModalValueStyle = lipgloss.NewStyle().
			Foreground(ColorWhite)

	// Floating title badge rendered inline on modal border
	ModalFloatingTitle = lipgloss.NewStyle().
				Bold(true).
				Foreground(ColorSurface0).
				Background(ColorBlue).
				Padding(0, 1)

	// Inspector tree styles
	ModalSelectedStyle = lipgloss.NewStyle().
				Background(ColorSelectionBg)

	ModalIndicatorStyle = lipgloss.NewStyle().
				Foreground(ColorGray)

	ModalGroupLabelStyle = lipgloss.NewStyle().
				Foreground(ColorPurple)

	// Two-pane sidebar styles
	ModalSidebarSelectedStyle = lipgloss.NewStyle().
					Background(ColorSelectionBg).
					Foreground(ColorWhite).
					Bold(true)

	ModalSidebarDimStyle = lipgloss.NewStyle().
				Foreground(ColorGray)

	ModalSidebarFocusCursor = lipgloss.NewStyle().
				Foreground(ColorBlue).
				Bold(true)

	// Inspector search styles
	ModalSearchBarStyle = lipgloss.NewStyle().
				Foreground(ColorWhite)

	ModalSearchMatchStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(ColorPurple).
				Background(ColorSearchCharBg)

	// Copy feedback style
	ModalCopyFeedbackStyle = lipgloss.NewStyle().
				Foreground(ColorGreen).
				Bold(true)

	// Breadcrumb styles for inspector navigation
	ModalBreadcrumbStyle = lipgloss.NewStyle().
				Foreground(ColorGray)

	ModalBreadcrumbActiveStyle = lipgloss.NewStyle().
					Foreground(ColorBlue).
					Bold(true)

	ModalBreadcrumbSepStyle = lipgloss.NewStyle().
				Foreground(ColorGrayDim)
)

// Statusline styles (LazyVim-style mode pill + segments)
var (
	StatusModePill = lipgloss.NewStyle().
			Bold(true).
			Padding(0, 1)

	StatusSegment = lipgloss.NewStyle().
			Foreground(ColorOffWhite).
			Padding(0, 1)

	StatusSegmentDim = lipgloss.NewStyle().
				Foreground(ColorGray).
				Padding(0, 1)

	StatusSep = lipgloss.NewStyle().
			Foreground(ColorGrayDim)

	// Breadcrumb style
	BreadcrumbStyle = lipgloss.NewStyle().
			Foreground(ColorGray)

	BreadcrumbActiveStyle = lipgloss.NewStyle().
				Foreground(ColorBlue)

	BreadcrumbSepStyle = lipgloss.NewStyle().
				Foreground(ColorGrayDim)
)
