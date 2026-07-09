package enrichment

import (
	"strings"
	"testing"
)

// FuzzLoadRulesFromBytes drives the rules-file JSON parser with arbitrary
// bytes. LoadRules reads a user-supplied JSON config file, so for any input
// it must return a value or an error — never panic. Seeds cover valid and
// malformed shapes so the fuzzer reaches every branch.
func FuzzLoadRulesFromBytes(f *testing.F) {
	seeds := [][]byte{
		[]byte(``),
		[]byte(`{}`),
		[]byte(`[]`),
		[]byte(`{"enrichers":[]}`),
		[]byte(`{"enrichers":[{"match":{"name":"*"},"hints":{"category":"x","outcome":"success"}}]}`),
		[]byte(`{"enrichers":[{"match":{"attrs":{"http.method":"GET"}},"hints":{"icon":"↻","is_marker":true}}]}`),
		[]byte(`not json at all`),
		[]byte(`{"enrichers":"should-be-array"}`),
		[]byte(`{` + strings.Repeat(`"x":`, 100) + `1}`),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		e, err := LoadRulesFromBytes(data)
		if err != nil {
			if e != nil {
				t.Fatalf("LoadRulesFromBytes returned non-nil enricher with error: %v", err)
			}
			return
		}
		if e == nil {
			t.Fatal("LoadRulesFromBytes returned nil enricher with nil error")
		}
		// Enrich must not panic on the loaded rules with arbitrary input.
		_ = e.Enrich("any-span-name", map[string]string{"k": "v"}, false)
	})
}
