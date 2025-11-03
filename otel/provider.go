package otel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

const defaultMetricsSendInterval = 3 * time.Second

type attributeStorage interface {
	GetString(key string) string
}

type Config struct {
	ServiceName   string
	OTelHost      string
	EnableMetrics bool
	EnableTracer  bool
	EnableLogger  bool
}

// SetupOTelSDK bootstraps the OpenTelemetry pipeline.
// If it does not return an error, make sure to call shutdown for proper cleanup.
func SetupOTelSDK( //nolint:funlen,nonamedreturns
	ctx context.Context,
	cfg Config,
	attributeStorage attributeStorage,
) (shutdown func(context.Context) error, err error) {
	if cfg.OTelHost == "" {
		slog.Info("skipped OTel initialization, empty host")

		return func(_ context.Context) error {
			return nil
		}, nil
	}

	resourceAttributes := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(attributeStorage.GetString("resource.attributes.app.name")),
		semconv.ServiceVersion(attributeStorage.GetString("resource.attributes.app.version")),
		semconv.HostName(attributeStorage.GetString("resource.attributes.app.host")),
		semconv.K8SNamespaceName(attributeStorage.GetString("resource.attributes.pod.namespace")),
		semconv.K8SNodeName(attributeStorage.GetString("resource.attributes.node.name")),
		semconv.K8SPodName(attributeStorage.GetString("resource.attributes.pod.name")),
	)

	var shutdownFuncs []func(context.Context) error
	// shutdown calls cleanup functions registered via shutdownFuncs.
	// The errors from the calls are joined.
	// Each registered cleanup will be invoked once.
	shutdown = func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}

		shutdownFuncs = nil

		return err
	}

	// handleErr calls shutdown for cleanup and makes sure that all errors are returned.
	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(ctx))
	}

	// Set up propagator.
	prop := newPropagator()
	otel.SetTextMapPropagator(prop)

	// Set up trace provider.
	if cfg.EnableTracer {
		tracerProvider, err := newTraceProvider(ctx, cfg.OTelHost, resourceAttributes)
		if err != nil {
			handleErr(err)

			return shutdown, err
		}

		shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)

		otel.SetTracerProvider(tracerProvider)
	}

	// Set up meter provider.
	if cfg.EnableMetrics {
		meterProvider, err := newMeterProvider(ctx, cfg.OTelHost, resourceAttributes)
		if err != nil {
			handleErr(err)

			return shutdown, err
		}

		shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)

		otel.SetMeterProvider(meterProvider)
	}

	// Set up logger provider.
	if cfg.EnableLogger {
		loggerProvider, err := newLoggerProvider(ctx, cfg.OTelHost, resourceAttributes)
		if err != nil {
			handleErr(err)

			return shutdown, err
		}

		shutdownFuncs = append(shutdownFuncs, loggerProvider.Shutdown)

		global.SetLoggerProvider(loggerProvider)
	}

	slog.Info(
		"OTel initialized",
		slog.String("host", cfg.OTelHost),
		slog.Bool("withTracer", cfg.EnableTracer),
		slog.Bool("withMeter", cfg.EnableMetrics),
		slog.Bool("withLogger", cfg.EnableLogger),
	)

	return shutdown, nil
}

func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

func newTraceProvider(ctx context.Context, url string, resource *resource.Resource) (*trace.TracerProvider, error) {
	traceExporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint(url),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	traceProvider := trace.NewTracerProvider(
		trace.WithBatcher(traceExporter, trace.WithBatchTimeout(time.Second)),
		trace.WithResource(resource),
	)

	return traceProvider, nil
}

func newMeterProvider(ctx context.Context, url string, resource *resource.Resource) (*metric.MeterProvider, error) {
	metricExporter, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpoint(url),
		otlpmetrichttp.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize metric exporter: %w", err)
	}

	meterProvider := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(metricExporter, metric.WithInterval(defaultMetricsSendInterval))),
		metric.WithResource(resource),
	)

	return meterProvider, nil
}

func newLoggerProvider(ctx context.Context, url string, resource *resource.Resource) (*log.LoggerProvider, error) {
	logExporter, err := otlploghttp.New(ctx,
		otlploghttp.WithEndpoint(url),
		otlploghttp.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize log exporter: %w", err)
	}

	loggerProvider := log.NewLoggerProvider(
		log.WithProcessor(log.NewBatchProcessor(logExporter)),
		log.WithResource(resource),
	)

	return loggerProvider, nil
}
