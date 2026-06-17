package utils

import (
	"fmt"
	"io"
	"math"
	"net/url"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

type ParsedGitHubURL struct {
	Owner      string
	Repo       string
	Type       string
	Identifier string
}

func HumanizeTime(seconds float64) string {
	if seconds == 0 {
		return "0s"
	}
	prefix := ""
	if seconds < 0 {
		prefix = "-"
		seconds = -seconds
	}
	if seconds < 1 {
		ms := int(seconds*1000 + 0.5)
		if ms == 0 {
			return "0s"
		}
		if ms < 1000 {
			return fmt.Sprintf("%s%dms", prefix, ms)
		}
		// Rounded up to a full second; fall through to the seconds path.
		seconds = 1
	}

	hours := int(seconds) / 3600
	minutes := (int(seconds) % 3600) / 60
	secs := int(seconds) % 60

	var parts []string
	if hours > 0 {
		parts = append(parts, fmt.Sprintf("%dh", hours))
	}
	// Render the minutes placeholder when an interior zero would otherwise be
	// dropped (e.g. 1h0m3s): "1h 3s" reads ambiguously, so keep "1h 0m 3s".
	// Trailing zero components are still suppressed.
	if minutes > 0 || (hours > 0 && secs > 0) {
		parts = append(parts, fmt.Sprintf("%dm", minutes))
	}
	if secs > 0 || len(parts) == 0 {
		parts = append(parts, fmt.Sprintf("%ds", secs))
	}
	return prefix + strings.Join(parts, " ")
}

// RoundTrendDuration snaps a duration (seconds) to the display grid used by the
// trend regression/improvement tables: tenths of a second below one minute,
// whole seconds at or above. Percent changes are computed from these rounded
// values so the Was/Now columns reproduce the displayed Change.
func RoundTrendDuration(seconds float64) float64 {
	if seconds < 60 {
		return math.Round(seconds*10) / 10
	}
	return math.Round(seconds)
}

// FormatTrendDuration formats a duration for the trend regression/improvement
// tables. Sub-minute durations show one decimal place so the percentage change
// is reproducible from the printed value; longer durations use the compact
// HumanizeTime form.
func FormatTrendDuration(seconds float64) string {
	g := RoundTrendDuration(seconds)
	if g > 0 && g < 60 {
		return fmt.Sprintf("%.1fs", g)
	}
	return HumanizeTime(g)
}

func ParseGitHubURL(raw string) (ParsedGitHubURL, error) {
	// If it doesn't have a protocol, but looks like a github URL or org/repo,
	// we'll try to parse it by prepending https://
	input := raw
	if !strings.HasPrefix(input, "http://") && !strings.HasPrefix(input, "https://") {
		if strings.HasPrefix(input, "github.com/") {
			input = "https://" + input
		} else {
			// Assume it's an org/repo/type/id format
			input = "https://github.com/" + input
		}
	}

	parsed, err := url.Parse(input)
	if err != nil || (parsed.Host != "github.com" && parsed.Host != "www.github.com") {
		return ParsedGitHubURL{}, fmt.Errorf("Invalid GitHub URL: %s. Expected format: PR: https://github.com/owner/repo/pull/123, Commit: https://github.com/owner/repo/commit/abc123, or Run: https://github.com/owner/repo/actions/runs/12345", raw)
	}

	parts := strings.FieldsFunc(parsed.Path, func(r rune) bool { return r == '/' })
	// Tolerate trailing segments (e.g. /pull/123/files, /pull/123/commits)
	// that come along when copying URLs from PR tabs.
	if len(parts) >= 4 && parts[2] == "pull" {
		return ParsedGitHubURL{Owner: parts[0], Repo: parts[1], Type: "pr", Identifier: parts[3]}, nil
	}
	if len(parts) >= 4 && parts[2] == "commit" {
		return ParsedGitHubURL{Owner: parts[0], Repo: parts[1], Type: "commit", Identifier: parts[3]}, nil
	}
	if len(parts) >= 5 && parts[2] == "actions" && parts[3] == "runs" {
		return ParsedGitHubURL{Owner: parts[0], Repo: parts[1], Type: "run", Identifier: parts[4]}, nil
	}

	return ParsedGitHubURL{}, fmt.Errorf("Invalid GitHub URL: %s. Expected format: PR: https://github.com/owner/repo/pull/123, Commit: https://github.com/owner/repo/commit/abc123, or Run: https://github.com/owner/repo/actions/runs/12345", raw)
}

// ExpandGitHubURL ensures a GitHub URL has the full https://github.com/ prefix.
// Shorthand forms like "owner/repo/pull/123" are expanded; already-full URLs are returned as-is.
func ExpandGitHubURL(raw string) string {
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		return raw
	}
	if strings.HasPrefix(raw, "github.com/") {
		return "https://" + raw
	}
	return "https://github.com/" + raw
}

func GetJobGroup(jobName string) string {
	parts := strings.Split(jobName, " / ")
	if len(parts) > 1 {
		return parts[0]
	}
	return jobName
}

// colorEnabled gates ANSI color and OSC 8 hyperlink emission. It defaults to
// true and is initialized by the CLI entry point based on whether the output
// destination is a terminal (and NO_COLOR).
var colorEnabled = true

// SetColorEnabled enables or disables ANSI/OSC escape sequence emission.
func SetColorEnabled(enabled bool) {
	colorEnabled = enabled
}

// ColorEnabled reports whether ANSI/OSC escape sequences are emitted.
func ColorEnabled() bool {
	return colorEnabled
}

func MakeClickableLink(urlValue, text string) string {
	displayText := text
	if displayText == "" {
		displayText = urlValue
	}
	if !colorEnabled || !isGitHubURL(urlValue) {
		return displayText
	}
	return fmt.Sprintf("\u001b]8;;%s\u0007%s\u001b]8;;\u0007", urlValue, displayText)
}

func colorText(code, text string) string {
	if !colorEnabled {
		return text
	}
	return fmt.Sprintf("\u001b[%sm%s\u001b[0m", code, text)
}

func GrayText(text string) string {
	return colorText("90", text)
}

func GreenText(text string) string {
	return colorText("32", text)
}

func RedText(text string) string {
	return colorText("31", text)
}

func YellowText(text string) string {
	return colorText("33", text)
}

func BlueText(text string) string {
	return colorText("34", text)
}

func CategorizeStep(stepName string) string {
	name := strings.ToLower(stepName)
	switch {
	case strings.Contains(name, "checkout") || strings.Contains(name, "clone"):
		return "step_checkout"
	case strings.Contains(name, "setup") || strings.Contains(name, "install") || strings.Contains(name, "cache"):
		return "step_setup"
	case strings.Contains(name, "build") || strings.Contains(name, "compile") || strings.Contains(name, "make"):
		return "step_build"
	case strings.Contains(name, "test") || strings.Contains(name, "spec") || strings.Contains(name, "coverage"):
		return "step_test"
	case strings.Contains(name, "lint") || strings.Contains(name, "format") || strings.Contains(name, "check"):
		return "step_lint"
	case strings.Contains(name, "deploy") || strings.Contains(name, "publish") || strings.Contains(name, "release"):
		return "step_deploy"
	case strings.Contains(name, "upload") || strings.Contains(name, "artifact") || strings.Contains(name, "store"):
		return "step_artifact"
	case strings.Contains(name, "security") || strings.Contains(name, "scan") || strings.Contains(name, "audit"):
		return "step_security"
	case strings.Contains(name, "notification") || strings.Contains(name, "slack") || strings.Contains(name, "email"):
		return "step_notify"
	default:
		return "step_other"
	}
}

func GetStepIcon(stepName, conclusion string) string {
	name := strings.ToLower(stepName)
	switch conclusion {
	case "failure":
		return "❌"
	case "cancelled":
		return "🚫"
	case "skipped":
		return "⏭️"
	}

	switch {
	case strings.Contains(name, "checkout") || strings.Contains(name, "clone"):
		return "📥"
	case strings.Contains(name, "setup") || strings.Contains(name, "install"):
		return "⚙️"
	case strings.Contains(name, "cache"):
		return "💾"
	case strings.Contains(name, "build") || strings.Contains(name, "compile"):
		return "🔨"
	case strings.Contains(name, "test") || strings.Contains(name, "spec"):
		return "🧪"
	case strings.Contains(name, "lint") || strings.Contains(name, "format"):
		return "🔍"
	case strings.Contains(name, "deploy") || strings.Contains(name, "publish"):
		return "🚀"
	case strings.Contains(name, "upload") || strings.Contains(name, "artifact"):
		return "📤"
	case strings.Contains(name, "security") || strings.Contains(name, "scan"):
		return "🔒"
	case strings.Contains(name, "notification") || strings.Contains(name, "slack"):
		return "📢"
	case strings.Contains(name, "docker") || strings.Contains(name, "container"):
		return "🐳"
	case strings.Contains(name, "database") || strings.Contains(name, "migrate"):
		return "🗄️"
	default:
		return "▶️"
	}
}

func ParseTime(value string) (time.Time, bool) {
	if value == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

func isGitHubURL(urlValue string) bool {
	return strings.HasPrefix(urlValue, "https://github.com/") || strings.HasPrefix(urlValue, "http://github.com/")
}

func OpenBrowser(url string) error {
	var cmd string
	var args []string

	switch runtime.GOOS {
	case "windows":
		cmd = "rundll32"
		args = []string{"url.dll,FileProtocolHandler", url}
	case "darwin":
		cmd = "open"
		args = []string{url}
	default: // linux, freebsd, openbsd, netbsd
		cmd = "xdg-open"
		args = []string{url}
	}
	return exec.Command(cmd, args...).Start()
}

// CopyToClipboard copies text to the system clipboard.
func CopyToClipboard(text string) error {
	var cmd string
	var args []string

	switch runtime.GOOS {
	case "windows":
		cmd = "clip"
	case "darwin":
		cmd = "pbcopy"
	default:
		cmd = "xclip"
		args = []string{"-selection", "clipboard"}
	}
	c := exec.Command(cmd, args...)
	c.Stdin = strings.NewReader(text)
	return c.Run()
}

func StripANSI(str string) string {
	var b strings.Builder
	b.Grow(len(str))
	inEscape := false
	inOSC := false
	for i := 0; i < len(str); i++ {
		c := str[i]
		if inEscape {
			if c == '[' {
				continue
			}
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '@' {
				inEscape = false
			}
			continue
		}
		if inOSC {
			if c == '\a' || (c == '\\' && i > 0 && str[i-1] == '\u001b') {
				inOSC = false
			}
			continue
		}
		if c == '\u001b' {
			if i+1 < len(str) {
				if str[i+1] == '[' {
					inEscape = true
					i++
					continue
				}
				if str[i+1] == ']' {
					inOSC = true
					i++
					continue
				}
			}
		}
		// Also filter out other control characters that break JSON
		if c < 32 && c != '\t' && c != '\n' && c != '\r' {
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}

// stripANSIWriter removes ANSI/OSC escape sequences from everything written
// through it. Escape sequences must not be split across Write calls (the
// styled renderers write whole lines at a time).
type stripANSIWriter struct{ w io.Writer }

func (s stripANSIWriter) Write(p []byte) (int, error) {
	if _, err := s.w.Write([]byte(StripANSI(string(p)))); err != nil {
		return 0, err
	}
	return len(p), nil
}

// NewStripANSIWriter wraps w so ANSI/OSC escape sequences (including those
// produced by lipgloss styles) are removed from anything written to it.
// Useful when styled output is redirected to a file or pipe.
func NewStripANSIWriter(w io.Writer) io.Writer {
	return stripANSIWriter{w: w}
}

// GlobMatch performs simple glob matching: "*" matches everything,
// "prefix*" matches prefix, "*suffix" matches suffix, "*mid*" matches contains,
// exact match otherwise.
func GlobMatch(pattern, value string) bool {
	if pattern == "*" {
		return true
	}
	if strings.HasPrefix(pattern, "*") && strings.HasSuffix(pattern, "*") {
		return strings.Contains(value, pattern[1:len(pattern)-1])
	}
	if strings.HasPrefix(pattern, "*") {
		return strings.HasSuffix(value, pattern[1:])
	}
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(value, pattern[:len(pattern)-1])
	}
	return pattern == value
}
