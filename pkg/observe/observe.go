// Package observe provides centralized observability: structured logging, distributed tracing,
// and Prometheus metrics collection.
package observe

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Provider aggregates logging, tracing, and metrics for a service.
type Provider struct {
	// Logger is the structured logger instance.
	Logger *slog.Logger
	// Tracer is the OpenTelemetry tracer for distributed tracing.
	Tracer trace.Tracer
	// Metrics holds registered Prometheus metrics.
	Metrics *Metrics
	// shutdownFuncs are cleanup functions for traces and exporters.
	shutdownFuncs []func(context.Context) error
}

type Option func(*providerOpts)

type providerOpts struct {
	prometheus    prometheus.Registerer
	testExporter  bool
	traceExporter sdktrace.SpanExporter
}

// WithPrometheus sets a custom Prometheus registerer.
func WithPrometheus(reg prometheus.Registerer) Option {
	return func(opts *providerOpts) {
		opts.prometheus = reg
	}
}

// WithTestExporter enables in-memory span recording for tests.
// The returned *SpanRecorder is used to inspect recorded spans.
func WithTestExporter(recorder *SpanRecorder) Option {
	return func(opts *providerOpts) {
		opts.testExporter = true
		opts.traceExporter = recorder
	}
}

// New creates a new observability provider with logger, tracer, and metrics.
// The returned shutdown function must be called at service termination to flush
// buffered traces and close exporters.
func New(serviceName string, opts ...Option) (*Provider, func(), error) {
	var popts providerOpts
	for _, opt := range opts {
		opt(&popts)
	}

	// Setup Prometheus metrics
	if popts.prometheus == nil {
		popts.prometheus = prometheus.NewRegistry()
	}
	metrics := NewMetrics(popts.prometheus)
	metrics.MustRegister(popts.prometheus)

	// Setup logger
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Setup tracer
	var traceExporter sdktrace.SpanExporter
	if popts.traceExporter != nil {
		traceExporter = popts.traceExporter
	} else {
		// Default: stdout exporter (can be disabled in production)
		var err error
		traceExporter, err = stdouttrace.New(stdouttrace.WithWriter(io.Discard))
		if err != nil {
			return nil, nil, fmt.Errorf("creating trace exporter: %w", err)
		}
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
	)
	otel.SetTracerProvider(tp)

	tracer := tp.Tracer(serviceName)

	p := &Provider{
		Logger:  logger,
		Tracer:  tracer,
		Metrics: metrics,
		shutdownFuncs: []func(context.Context) error{
			tp.Shutdown,
			traceExporter.Shutdown,
		},
	}

	return p, p.shutdown, nil
}

func (p *Provider) shutdown() {
	ctx := context.Background()
	for _, fn := range p.shutdownFuncs {
		_ = fn(ctx)
	}
}
