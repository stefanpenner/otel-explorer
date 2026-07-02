package filter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// stubSpan builds a ReadOnlySpan with the given name, status code, attributes,
// and events for filter tests.
func stubSpan(name string, code codes.Code, attrs []attribute.KeyValue, events []sdktrace.Event) sdktrace.ReadOnlySpan {
	return tracetest.SpanStub{
		Name:       name,
		Attributes: attrs,
		Events:     events,
		Status:     sdktrace.Status{Code: code},
	}.Snapshot()
}

func TestParse(t *testing.T) {
	tests := []struct {
		expr    string
		wantErr bool
		count   int
	}{
		{"", false, 0},
		{"service.name=checkout", false, 1},
		{"service.name=checkout,http.status_code=5*", false, 2},
		{"!service.name=internal", false, 1},
		{"http.status_code", false, 1}, // bare key = exists check
		{"=value", true, 0},            // empty key
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			f, err := Parse(tt.expr)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.expr == "" {
				if f != nil {
					t.Error("expected nil filter for empty expr")
				}
				return
			}
			if len(f.conditions) != tt.count {
				t.Errorf("expected %d conditions, got %d", tt.count, len(f.conditions))
			}
		})
	}
}

func TestParse_Negation(t *testing.T) {
	f, err := Parse("!service.name=internal")
	if err != nil {
		t.Fatal(err)
	}
	if len(f.conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(f.conditions))
	}
	if !f.conditions[0].negate {
		t.Error("expected negate=true")
	}
	if f.conditions[0].key != "service.name" {
		t.Errorf("expected key 'service.name', got %q", f.conditions[0].key)
	}
	if f.conditions[0].value != "internal" {
		t.Errorf("expected value 'internal', got %q", f.conditions[0].value)
	}
}

func TestParse_BareKey(t *testing.T) {
	f, err := Parse("http.status_code")
	if err != nil {
		t.Fatal(err)
	}
	if f.conditions[0].value != "*" {
		t.Errorf("bare key should have wildcard value, got %q", f.conditions[0].value)
	}
}

func TestErrorsOnly(t *testing.T) {
	f := ErrorsOnly()
	if len(f.conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(f.conditions))
	}
	if f.conditions[0].key != "otel.status_code" {
		t.Errorf("expected key 'otel.status_code', got %q", f.conditions[0].key)
	}
	if f.conditions[0].value != "ERROR" {
		t.Errorf("expected value 'ERROR', got %q", f.conditions[0].value)
	}
}

func TestErrorsOnly_MatchesExceptionEvent(t *testing.T) {
	f := ErrorsOnly()

	// A span with an exception event but UNSET status must pass errors-only.
	excSpan := stubSpan("charge-card", codes.Unset, nil, []sdktrace.Event{
		{Name: "exception", Attributes: []attribute.KeyValue{
			attribute.String("exception.type", "PaymentDeclined"),
		}},
	})
	// A clean OK span must not.
	okSpan := stubSpan("validate-cart", codes.Ok, nil, nil)
	// An explicit ERROR span must still pass.
	errSpan := stubSpan("db-write", codes.Error, nil, nil)

	result := f.Apply([]sdktrace.ReadOnlySpan{excSpan, okSpan, errSpan})
	if len(result) != 2 {
		t.Fatalf("expected 2 spans (exception + error), got %d", len(result))
	}
	if result[0].Name() != "charge-card" || result[1].Name() != "db-write" {
		t.Errorf("unexpected spans passed: %q, %q", result[0].Name(), result[1].Name())
	}
}

func TestMatches_StatusCodeUppercase(t *testing.T) {
	// The OTel convention status string is uppercase; a user filter of
	// otel.status_code=ERROR must match an Error-status span even though the
	// Go SDK renders the code as "Error".
	f, err := Parse("otel.status_code=ERROR")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	errSpan := stubSpan("db-write", codes.Error, nil, nil)
	okSpan := stubSpan("read", codes.Ok, nil, nil)

	result := f.Apply([]sdktrace.ReadOnlySpan{errSpan, okSpan})
	if len(result) != 1 || result[0].Name() != "db-write" {
		t.Fatalf("expected only the error span to match, got %d", len(result))
	}
}

func TestMatches_IntAttributeGlob(t *testing.T) {
	// http.status_code is an int attribute; a 5xx glob must match it. With
	// AsString() the int collapsed to "" and never matched.
	f, err := Parse("http.status_code=5*")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	span5xx := stubSpan("GET /api", codes.Unset, []attribute.KeyValue{
		attribute.Int("http.status_code", 503),
	}, nil)
	span2xx := stubSpan("GET /ok", codes.Unset, []attribute.KeyValue{
		attribute.Int("http.status_code", 200),
	}, nil)

	result := f.Apply([]sdktrace.ReadOnlySpan{span5xx, span2xx})
	if len(result) != 1 || result[0].Name() != "GET /api" {
		t.Fatalf("expected only the 503 span to match, got %d spans", len(result))
	}
}

func TestApply_NilFilter(t *testing.T) {
	var f *Filter
	spans := f.Apply(nil)
	if spans != nil {
		t.Error("expected nil from nil filter")
	}
}

func TestApply_EmptyFilter(t *testing.T) {
	f := &Filter{}
	result := f.Apply(nil)
	if result != nil {
		t.Error("expected nil from empty filter on nil input")
	}
}

func TestMatches_SpanAttrsTakePrecedenceOverResourceAttrs(t *testing.T) {
	tests := []struct {
		name          string
		spanAttrs     []attribute.KeyValue
		resourceAttrs []attribute.KeyValue
		filterExpr    string
		expectMatch   bool
	}{
		{
			name: "span attr wins over resource attr with same key",
			spanAttrs: []attribute.KeyValue{
				attribute.String("service.name", "span-service"),
			},
			resourceAttrs: []attribute.KeyValue{
				attribute.String("service.name", "resource-service"),
			},
			filterExpr:  "service.name=span-service",
			expectMatch: true,
		},
		{
			name: "resource attr value does NOT match when span overrides it",
			spanAttrs: []attribute.KeyValue{
				attribute.String("service.name", "span-service"),
			},
			resourceAttrs: []attribute.KeyValue{
				attribute.String("service.name", "resource-service"),
			},
			filterExpr:  "service.name=resource-service",
			expectMatch: false,
		},
		{
			name: "resource-only attr still available when no span override",
			spanAttrs: []attribute.KeyValue{
				attribute.String("service.name", "span-service"),
			},
			resourceAttrs: []attribute.KeyValue{
				attribute.String("service.name", "resource-service"),
				attribute.String("deployment.environment", "production"),
			},
			filterExpr:  "deployment.environment=production",
			expectMatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := Parse(tt.filterExpr)
			assert.NoError(t, err)

			var res *resource.Resource
			if len(tt.resourceAttrs) > 0 {
				res = resource.NewSchemaless(tt.resourceAttrs...)
			}

			span := tracetest.SpanStub{
				Name:       "test-span",
				Attributes: tt.spanAttrs,
				Resource:   res,
				Status:     sdktrace.Status{Code: codes.Unset},
			}.Snapshot()

			matched := f.matches(span)
			assert.Equal(t, tt.expectMatch, matched)
		})
	}
}
