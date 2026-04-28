// Package observe provides test helpers for OpenTelemetry tracing.
//
// This package lives in testutil/ rather than pkg/observe so that production
// binaries do not pull in the OTel SDK's trace-exporter machinery at link time.
// It is intended for use in test files only (_test.go or testutil/ packages).
//
// Provided types:
//   - SpanRecorder — an in-memory sdktrace.SpanExporter that records every
//     exported span so tests can assert on trace names, attributes, and errors.
//
// Key methods on SpanRecorder:
//   - ExportSpans — called by the OTel SDK; appends spans to an internal slice
//   - Spans        — returns a copy of all recorded spans (thread-safe)
//   - SpansByName  — filters by span name
//   - HasSpan      — convenience predicate for "at least one span with this name"
//   - Clear        — resets the slice (useful for subtests that share a recorder)
//
// Dependencies:
//   - go.opentelemetry.io/otel/sdk/trace (sdktrace.SpanExporter, ReadOnlySpan)
package observe

import (
	"context"
	"sync"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// SpanRecorder is an in-memory span exporter for testing.
// It records all spans so tests can inspect trace data.
// It implements sdktrace.SpanExporter and can be passed directly to
// observe.WithTestExporter.
type SpanRecorder struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

// ExportSpans records spans in memory.
func (sr *SpanRecorder) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.spans = append(sr.spans, spans...)
	return nil
}

// Shutdown is a no-op for the test exporter.
func (sr *SpanRecorder) Shutdown(_ context.Context) error {
	return nil
}

// Spans returns all recorded spans.
func (sr *SpanRecorder) Spans() []sdktrace.ReadOnlySpan {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	return append([]sdktrace.ReadOnlySpan{}, sr.spans...)
}

// SpansByName returns all spans with a given name.
func (sr *SpanRecorder) SpansByName(name string) []sdktrace.ReadOnlySpan {
	var result []sdktrace.ReadOnlySpan
	for _, span := range sr.Spans() {
		if span.Name() == name {
			result = append(result, span)
		}
	}
	return result
}

// HasSpan checks if a span with the given name exists.
func (sr *SpanRecorder) HasSpan(name string) bool {
	return len(sr.SpansByName(name)) > 0
}

// Clear resets the recorded spans.
func (sr *SpanRecorder) Clear() {
	sr.mu.Lock()
	defer sr.mu.Unlock()
	sr.spans = nil
}
