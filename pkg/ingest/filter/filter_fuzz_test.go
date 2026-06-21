package filter

import (
	"testing"

	"github.com/stefanpenner/otel-explorer/pkg/utils"
)

// FuzzParse drives the filter-expression DSL parser with arbitrary strings.
// Filter expressions are user-supplied (--filter flag), and the parser splits
// on ",", strips a leading "!", and slices around "="; none of that may panic.
func FuzzParse(f *testing.F) {
	for _, s := range []string{
		"", "!", "=", "!=", "key=value", "!key", "a=b,c=d",
		"k=*", "*=v", ",,,", "!=,!", "key=", "=value",
		"otel.span_name=*build*", "a=b,,!c,d=",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, expr string) {
		_, _ = Parse(expr)
	})
}

// FuzzGlobMatch fuzzes the glob primitive the filter applies per span/attr.
// Both pattern and value derive from untrusted input (filter flag + span
// attributes), so it must never panic regardless of "*" placement or length.
func FuzzGlobMatch(f *testing.F) {
	for _, p := range []struct{ pat, val string }{
		{"*", "anything"}, {"**", "x"}, {"*", ""}, {"", ""},
		{"*mid*", "a mid b"}, {"pre*", "prefix"}, {"*suf", "xsuf"},
		{"*", "*"}, {"a*b*c", "axbxc"},
	} {
		f.Add(p.pat, p.val)
	}
	f.Fuzz(func(t *testing.T, pattern, value string) {
		_ = utils.GlobMatch(pattern, value)
	})
}
