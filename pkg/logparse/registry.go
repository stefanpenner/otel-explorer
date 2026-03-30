package logparse

import "time"

// Registry holds parsers in priority order. Format-specific parsers are
// tried first; the generic TimestampParser is used as a fallback.
type Registry struct {
	parsers []Parser
}

// NewRegistry creates a registry with the given parsers.
// The TimestampParser fallback is always appended at the end.
func NewRegistry(parsers ...Parser) *Registry {
	all := make([]Parser, len(parsers))
	copy(all, parsers)
	all = append(all, &TimestampParser{})
	return &Registry{parsers: all}
}

// DefaultRegistry returns a registry with all built-in parsers.
func DefaultRegistry() *Registry {
	return NewRegistry(
		&GradleParser{},
		&BazelParser{},
		&SetupJobParser{},
	)
}

// Detect tries each parser's Match in order and returns the first match.
// Always returns a parser (TimestampParser is the fallback).
func (r *Registry) Detect(lines []LogLine) Parser {
	for _, p := range r.parsers {
		if p.Match(lines) {
			return p
		}
	}
	// Should not happen since TimestampParser always matches, but just in case
	return &TimestampParser{}
}

// Parse auto-detects the format and parses in one call.
// The result is filtered to drop spans below 1% of the step duration.
func (r *Registry) Parse(lines []LogLine, stepStart, stepEnd time.Time) (parserName string, spans []ParsedSpan) {
	p := r.Detect(lines)
	return p.Name(), filterBySignificance(p.Parse(lines, stepStart, stepEnd), stepStart, stepEnd)
}
