// Package otel bootstraps the OpenTelemetry SDK for traces, metrics, and
// logs over OTLP/HTTP. It returns a *Provider that implements
// lifecycle.Resource so it can be registered with app.App and shut down
// last in the LIFO chain (allowing other components to flush telemetry
// before the exporters close).
//
// Typical usage:
//
//	p, err := otel.Setup(ctx, otel.Config{
//	    OTelHost:      "otel-collector:4318",
//	    ServiceName:   "myservice",
//	    EnableTracer:  true,
//	    EnableMetrics: true,
//	})
//	if err != nil { log.Fatal(err) }
//
//	a := app.New()
//	a.Add(p)   // register first → shuts down last → final flush happens AFTER
//	           // db/redis/etc have stopped emitting.
//	a.Add(httpServer, grpcServer)
//
// Once Setup has run, the global TracerProvider, MeterProvider, and
// LoggerProvider are non-noop and can be used by every other core/* package
// via WithOtel() options.
package otel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"github.com/sergeyslonimsky/core/lifecycle"
)

const defaultMetricsSendInterval = 3 * time.Second

// defaultHistogramBoundaries is the explicit bucket layout used for histogram
// instruments. We use explicit buckets (not exponential) so Prometheus's
// histogram_quantile() returns sensible results without re-bucketing.
var defaultHistogramBoundaries = []float64{ //nolint:gochecknoglobals // package-level default, not mutable state
	5,
	10,
	25,
	50,
	100,
	250,
	500,
	1000,
	2500,
	5000,
	10000,
}

// Config controls which OpenTelemetry signals are enabled and what resource
// attributes describe the producing service. Plain fields, no struct tags —
// consumer apps map their viper keys to fields explicitly inside their own
// config.NewConfig().
//
// To intentionally run without a collector (local dev, tests), set
// Disabled=true — Setup will return a noop Provider. An empty OTelHost
// without Disabled=true is treated as a misconfiguration and returns
// ErrEmptyOTelHost, so a missing environment variable fails loudly instead
// of silently disabling observability.
type Config struct {
	// Disabled explicitly turns off OTel setup. When true, Setup returns a
	// noop Provider regardless of the other fields. Use for local
	// development / tests without a collector.
	Disabled bool

	// OTelHost is the OTLP/HTTP endpoint (host:port). Required when
	// Disabled is false.
	OTelHost string

	// ServiceName is the value of resource.attribute service.name.
	// Required when Disabled is false.
	ServiceName string

	// ServiceVersion populates service.version. Optional.
	ServiceVersion string

	// HostName populates host.name. Optional.
	HostName string

	// K8SNamespace populates k8s.namespace.name. Optional.
	K8SNamespace string

	// K8SNodeName populates k8s.node.name. Optional.
	K8SNodeName string

	// K8SPodName populates k8s.pod.name. Optional.
	K8SPodName string

	// EnableTracer turns on the trace provider with an OTLP/HTTP exporter.
	EnableTracer bool

	// EnableMetrics turns on the meter provider with an OTLP/HTTP exporter
	// and a 3s default send interval.
	EnableMetrics bool

	// EnableLogger turns on the logger provider with an OTLP/HTTP exporter.
	EnableLogger bool
}

// ErrEmptyOTelHost is returned by Setup when Config.Disabled is false but
// Config.OTelHost is empty. Flip Disabled=true to opt out explicitly.
var ErrEmptyOTelHost = errors.New("otel: OTelHost is empty (set Disabled=true to opt out)")

// Option configures Setup.
type Option func(*options)

type options struct {
	metricsSendInterval time.Duration
	histogramBoundaries []float64
}

// WithMetricsSendInterval overrides how often metrics are exported.
// Default: 3 seconds. Smaller values give finer resolution at the cost of
// more network traffic.
func WithMetricsSendInterval(d time.Duration) Option {
	return func(o *options) { o.metricsSendInterval = d }
}

// WithHistogramBuckets overrides the explicit histogram bucket boundaries
// (in milliseconds). Default: [5, 10, 25, 50, 100, 250, 500, 1000, 2500,
// 5000, 10000].
//
// Tune to match the latency distribution of your service. Buckets that are
// too coarse hide regressions; too fine wastes storage.
func WithHistogramBuckets(boundaries []float64) Option {
	return func(o *options) { o.histogramBoundaries = boundaries }
}

// Provider holds the SDK shutdown closures registered during Setup.
// Implements lifecycle.Resource so it integrates with app.App.
type Provider struct {
	shutdownFuncs []func(context.Context) error
	shutdownOnce  sync.Once
	shutdownErr   error
}

// Setup bootstraps the configured OpenTelemetry signals and returns a
// Provider that owns them. Sets the global propagator (W3C TraceContext +
// Baggage) and the global TracerProvider / MeterProvider / LoggerProvider
// for all other otel-aware libraries to discover.
//
// If cfg.Disabled is true, Setup returns a noop Provider. An empty
// cfg.OTelHost with Disabled=false is an error (ErrEmptyOTelHost) — this
// prevents silently running without observability because of a missing
// env var.
//
// On failure, any partially-initialised SDK is shut down before returning.
func Setup(ctx context.Context, cfg Config, opts ...Option) (*Provider, error) {
	if cfg.Disabled {
		slog.Info("otel disabled via Config.Disabled")

		//nolint:exhaustruct // shutdownOnce/shutdownErr have valid zero values
		return &Provider{shutdownFuncs: nil}, nil
	}

	if cfg.OTelHost == "" {
		return nil, ErrEmptyOTelHost
	}

	o := &options{
		metricsSendInterval: defaultMetricsSendInterval,
		histogramBoundaries: defaultHistogramBoundaries,
	}
	for _, apply := range opts {
		apply(o)
	}

	resource := newResource(cfg)

	//nolint:exhaustruct // shutdownOnce/shutdownErr have valid zero values
	p := &Provider{shutdownFuncs: nil}

	otel.SetTextMapPropagator(newPropagator())

	if err := p.initProviders(ctx, cfg, resource, o); err != nil {
		return nil, err
	}

	slog.Info("otel initialized",
		slog.String("host", cfg.OTelHost),
		slog.Bool("trace", cfg.EnableTracer),
		slog.Bool("metrics", cfg.EnableMetrics),
		slog.Bool("logger", cfg.EnableLogger),
	)

	return p, nil
}

// Shutdown invokes every registered SDK shutdown in registration order
// (trace → metrics → logger), joining errors. Implements
// lifecycle.Resource.
//
// Idempotent and concurrent-safe: the shutdown runs exactly once; every
// subsequent call returns the same result (nil on clean shutdown, the
// joined error otherwise).
func (p *Provider) Shutdown(ctx context.Context) error {
	p.shutdownOnce.Do(func() {
		var errs []error

		for _, fn := range p.shutdownFuncs {
			if err := fn(ctx); err != nil {
				errs = append(errs, err)
			}
		}

		p.shutdownFuncs = nil
		p.shutdownErr = errors.Join(errs...)
	})

	return p.shutdownErr
}

func (p *Provider) initProviders(
	ctx context.Context, cfg Config,
	resource *sdkresource.Resource, o *options,
) error {
	if cfg.EnableTracer {
		tracerProvider, err := newTraceProvider(ctx, cfg.OTelHost, resource)
		if err != nil {
			_ = p.Shutdown(ctx)

			return fmt.Errorf("init trace provider: %w", err)
		}

		p.shutdownFuncs = append(p.shutdownFuncs, tracerProvider.Shutdown)
		otel.SetTracerProvider(tracerProvider)
	}

	if cfg.EnableMetrics {
		meterProvider, err := newMeterProvider(ctx, cfg.OTelHost, resource, o)
		if err != nil {
			_ = p.Shutdown(ctx)

			return fmt.Errorf("init meter provider: %w", err)
		}

		p.shutdownFuncs = append(p.shutdownFuncs, meterProvider.Shutdown)
		otel.SetMeterProvider(meterProvider)
	}

	if cfg.EnableLogger {
		loggerProvider, err := newLoggerProvider(ctx, cfg.OTelHost, resource)
		if err != nil {
			_ = p.Shutdown(ctx)

			return fmt.Errorf("init logger provider: %w", err)
		}

		p.shutdownFuncs = append(p.shutdownFuncs, loggerProvider.Shutdown)
		global.SetLoggerProvider(loggerProvider)
	}

	return nil
}

func newResource(cfg Config) *sdkresource.Resource {
	return sdkresource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion(cfg.ServiceVersion),
		semconv.HostName(cfg.HostName),
		semconv.K8SNamespaceName(cfg.K8SNamespace),
		semconv.K8SNodeName(cfg.K8SNodeName),
		semconv.K8SPodName(cfg.K8SPodName),
	)
}

// newPropagator returns the W3C TraceContext + Baggage propagator set.
func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

func newTraceProvider(
	ctx context.Context,
	endpoint string,
	resource *sdkresource.Resource,
) (*sdktrace.TracerProvider, error) {
	exp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(endpoint),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("trace exporter: %w", err)
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(time.Second)),
		sdktrace.WithResource(resource),
	), nil
}

func newMeterProvider(
	ctx context.Context,
	endpoint string,
	resource *sdkresource.Resource,
	o *options,
) (*sdkmetric.MeterProvider, error) {
	exp, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpoint(endpoint),
		otlpmetrichttp.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("metric exporter: %w", err)
	}

	return sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp,
			sdkmetric.WithInterval(o.metricsSendInterval))),
		sdkmetric.WithResource(resource),
		sdkmetric.WithView(sdkmetric.NewView(
			//nolint:exhaustruct
			sdkmetric.Instrument{Kind: sdkmetric.InstrumentKindHistogram},
			//nolint:exhaustruct
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
				Boundaries: o.histogramBoundaries,
			}},
		)),
	), nil
}

func newLoggerProvider(
	ctx context.Context,
	endpoint string,
	resource *sdkresource.Resource,
) (*sdklog.LoggerProvider, error) {
	exp, err := otlploghttp.New(ctx,
		otlploghttp.WithEndpoint(endpoint),
		otlploghttp.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("log exporter: %w", err)
	}

	return sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)),
		sdklog.WithResource(resource),
	), nil
}

// Compile-time check.
var _ lifecycle.Resource = (*Provider)(nil)
