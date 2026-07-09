package main

import (
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func ms(v int64) time.Time { return time.UnixMilli(v) }

func TestComputeTimeBounds(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		e, l := computeTimeBounds(nil)
		if e != 0 || l != 0 {
			t.Fatalf("expected (0,0), got (%d,%d)", e, l)
		}
	})

	t.Run("single span", func(t *testing.T) {
		s := tracetest.SpanStub{StartTime: ms(1000), EndTime: ms(2000)}.Snapshot()
		e, l := computeTimeBounds([]sdktrace.ReadOnlySpan{s})
		if e != 1000 || l != 2000 {
			t.Fatalf("expected (1000,2000), got (%d,%d)", e, l)
		}
	})

	t.Run("tracks min start and max end", func(t *testing.T) {
		spans := []sdktrace.ReadOnlySpan{
			tracetest.SpanStub{StartTime: ms(1500), EndTime: ms(5000)}.Snapshot(),
			tracetest.SpanStub{StartTime: ms(1000), EndTime: ms(9000)}.Snapshot(),
			tracetest.SpanStub{StartTime: ms(3000), EndTime: ms(7000)}.Snapshot(),
		}
		e, l := computeTimeBounds(spans)
		if e != 1000 {
			t.Fatalf("earliest = %d, want 1000", e)
		}
		if l != 9000 {
			t.Fatalf("latest = %d, want 9000", l)
		}
	})

	t.Run("epoch start handled (no sentinel bug)", func(t *testing.T) {
		// A span at Unix epoch (startMs==0) must not be treated as "unset".
		spans := []sdktrace.ReadOnlySpan{
			tracetest.SpanStub{StartTime: ms(0), EndTime: ms(100)}.Snapshot(),
			tracetest.SpanStub{StartTime: ms(5000), EndTime: ms(6000)}.Snapshot(),
		}
		e, l := computeTimeBounds(spans)
		if e != 0 {
			t.Fatalf("earliest = %d, want 0 (epoch is a real start time)", e)
		}
		if l != 6000 {
			t.Fatalf("latest = %d, want 6000", l)
		}
	})
}
