package enrichment

import (
	"strings"
	"testing"
)

// FuzzEnrichChain drives the full default enricher chain (GHA → CICD → GenAI →
// Generic) with a fuzzer-controlled span name and attribute map. Span names and
// attributes come from arbitrary trace files, so enrichment — which parses
// counts (gen_ai.usage.*, http status), truncates statements, and slices
// strings — must never panic. The attrs map is built by splitting the input
// into "key=value" lines, letting the fuzzer place any value under any key
// (including the semconv keys the enrichers special-case).
func FuzzEnrichChain(f *testing.F) {
	seeds := []string{
		"",
		"db.system=postgresql\ndb.statement=SELECT * FROM users WHERE id = 1",
		"gen_ai.system=anthropic\ngen_ai.request.model=claude\ngen_ai.usage.input_tokens=1200\ngen_ai.usage.output_tokens=340",
		"gen_ai.usage.input_tokens=-9999999999999999999\ngen_ai.usage.output_tokens=abc",
		"http.request.method=GET\nhttp.response.status_code=200\nurl.path=/api/users",
		"graphql.operation.type=query\ngraphql.operation.name=GetUser",
		"cicd.pipeline.name=ci\ncicd.pipeline.run.result=success",
		"gen_ai.tool.name=search\ngen_ai.operation.name=execute_tool",
		// multibyte statement near the 80-char truncation boundary
		"db.system=mysql\ndb.statement=" + strings.Repeat("あ", 90),
	}
	for _, s := range seeds {
		f.Add("span-name", s)
	}

	chain := DefaultEnricher()

	f.Fuzz(func(t *testing.T, name, attrText string) {
		attrs := make(map[string]string)
		for _, line := range strings.Split(attrText, "\n") {
			k, v, found := strings.Cut(line, "=")
			if found {
				attrs[k] = v
			}
		}
		// Must not panic for either duration flag.
		chain.Enrich(name, attrs, false)
		chain.Enrich(name, attrs, true)
	})
}
