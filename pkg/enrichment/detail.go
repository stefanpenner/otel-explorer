package enrichment

import "strings"

// NonRedundantDetail drops detail segments (joined by " · ") that already appear
// verbatim in the span name, so e.g. a GenAI span named "chat claude-opus-4"
// with detail "chat claude-opus-4 · 1.2k→340 tok" yields only the additive
// "1.2k→340 tok" rather than restating the name. Used to append a span's
// semantic Detail inline next to its name in both the CLI and TUI timelines.
func NonRedundantDetail(name, detail string) string {
	segments := strings.Split(detail, " · ")
	kept := segments[:0]
	for _, seg := range segments {
		if seg != "" && !strings.Contains(name, seg) {
			kept = append(kept, seg)
		}
	}
	return strings.Join(kept, " · ")
}
