# kafka

Wrapper around `github.com/twmb/franz-go` (kgo) that provides three lifecycle-conformant types:

- `Client` — base wrapper, for callers that build their own producer/consumer logic.
- `Producer` — synchronous single-record producer (Resource).
- `Consumer` — manual-commit consumer group host (Runner).

Plus a generic `JSONProducer[T]` helper for encoding typed payloads.

Wraps `github.com/twmb/franz-go` (kgo).

## When to use

Any service that produces or consumes Kafka messages.

## Quickstart

```go
package main

import (
    "context"
    "log"

    "github.com/sergeyslonimsky/core/app"
    "github.com/sergeyslonimsky/core/kafka"
)

type orderProcessor struct{}

func (orderProcessor) Process(ctx context.Context, msg kafka.Message) error {
    log.Printf("got %s/%s: %s", msg.Topic, msg.Key, msg.Message)
    return nil
}

func main() {
    ctx := context.Background()

    producer, err := kafka.NewProducer(kafka.ProducerConfig{
        Brokers: []string{"kafka:9092"},
        Topic:   "orders",
    }, kafka.WithOtel())
    if err != nil { log.Fatal(err) }

    consumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
        Brokers: []string{"kafka:9092"},
        Group:   "order-handler",
        Topics:  []string{"orders"},
        Offset:  "newest",
    }, orderProcessor{}, kafka.WithOtel())
    if err != nil { log.Fatal(err) }

    a := app.New()
    a.Add(producer)   // Resource
    a.Add(consumer)   // Runner
    log.Fatal(a.Run())
}
```

## Configuration

```go
type ClientConfig struct {
    Brokers  []string
    ClientID string
}

type ConsumerConfig struct {
    Brokers []string
    Group   string
    Offset  string  // "" / "newest" / "oldest"
    Topics  []string
}

type ProducerConfig struct {
    Brokers []string
    Topic   string  // informational; Produce takes the topic per call
}
```

App-level mapping example:

```go
kafka.ProducerConfig{
    Brokers: raw.GetStringSlice("kafka.brokers"),
    Topic:   raw.GetString("kafka.producers.orders.topic"),
}
```

`Brokers` is `[]string`. Use `raw.GetStringSlice` or split your own string.

## Options

Same `Option` type works with `NewClient`, `NewProducer`, `NewConsumer` — irrelevant fields are silently ignored:

- `WithLogger(*slog.Logger)` — used for lifecycle events.
- `WithOtel()` — wires kotel hooks for traces and meter metrics.
- `WithKgoOpts(...kgo.Opt)` — escape hatch for any kgo feature without a typed wrapper (custom partitioner, SASL, TLS, etc).
- `WithProduceTimeout(d time.Duration)` — applies to `NewProducer` only. Default: 30s.
- `WithErrorHandler(h ErrorHandler)` — applies to `NewConsumer` only. Controls what happens when `MessageProcessor.Process` returns an error. Default (no handler): log and skip (the offset advances past the failing record). See "Consumer error handling" below.

## Consumer error handling

By default, a failing `Process` call is logged and the record is committed — the consumer keeps moving. That is fine for poison-pill-tolerant pipelines, but risky when a transient downstream outage would silently discard messages.

`WithErrorHandler` lets you inject a decision function that returns one of:

- `kafka.ErrorActionSkip` — log + commit the record (default behavior).
- `kafka.ErrorActionRetry` — do NOT commit. The record stays uncommitted and is eligible for redelivery on the next rebalance or consumer restart. For in-process retry the handler should sleep/backoff before returning.
- `kafka.ErrorActionStop` — terminate the consumer: `Run` returns the wrapped error, which propagates up to `app.App` and triggers service shutdown.

Example — DLQ on first failure, fail-fast on dependency outage:

```go
consumer, err := kafka.NewConsumer(cfg, processor,
    kafka.WithOtel(),
    kafka.WithErrorHandler(func(ctx context.Context, msg kafka.Message, err error) kafka.ErrorAction {
        if errors.Is(err, ErrDependencyDown) {
            return kafka.ErrorActionStop
        }
        _ = dlq.Produce(msg.Topic, msg.Key, msg.Message)
        return kafka.ErrorActionSkip
    }),
)
```

## Observability (`WithOtel`)

Constructs a `kotel.Kotel` with the global tracer/meter providers and attaches its hooks via `kgo.WithHooks`:

- One span per produce.
- One span per consume batch.
- Per-broker meter metrics (latency, throughput).

Imports `github.com/twmb/franz-go/plugin/kotel`. Call `otel.Setup` and register the provider with `app.App` before constructing the kafka components.

## Lifecycle

- `Client`, `Producer` — `lifecycle.Resource`. Register with `app.App.Add(...)`. `Shutdown` closes the underlying `*kgo.Client` (blocking until in-flight produces drain).
- `Consumer` — `lifecycle.Runner`. Register with `app.App.Add(...)`. `Run` polls until `ctx` is cancelled or `Shutdown` is called.
- `Client.Healthcheck` — returns nil if the underlying client has discovered at least one broker.

Recommended ordering inside `app.App`:

```go
a.Add(otelProvider)        // Resource, registered first → shuts down last
a.Add(kafkaProducer)       // Resource
a.Add(kafkaConsumer)       // Runner
```

## Generic JSON producer

```go
type Order struct { ID int; Total float64 }

orderProducer := kafka.NewJSONProducer[Order]("orders", "default-key", producer)
err := orderProducer.Produce(ctx, Order{ID: 1, Total: 99.95})
```

`NewJSONProducer` accepts any `kafka.Publisher` — typically `*kafka.Producer`, but any custom type implementing `Produce(topic, key string, msg []byte) error` works (handy for fakes in tests).

## Extending

```go
raw := client.Unwrap()  // *kgo.Client
admin := kadm.NewClient(raw)  // for admin operations
```

## Testing

Unit tests use the included `MockPublisher` for `JSONProducer`.

Integration tests live in `internal/integration/`.

## See also

- [`core/app`](../app/README.md) — register your kafka components here.
- [`core/lifecycle`](../lifecycle/README.md) — the `Resource` / `Runner` / `Healthchecker` contracts.
- [`core/otel`](../otel/README.md) — bootstrap that powers `WithOtel`.
