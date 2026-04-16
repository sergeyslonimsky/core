# rabbitmq

Wrapper around `github.com/furdarius/rabbitroutine` for AMQP publishing and consumption with the lifecycle contract: typed `Config`, functional `Options`, `Shutdown(ctx)`, explicit `Resource` (`Publisher`) vs `Runner` (`ConsumerHost`) roles.

## When to use

Any service that publishes or consumes RabbitMQ messages.

## Quickstart

```go
package main

import (
    "context"
    "log"

    "github.com/sergeyslonimsky/core/app"
    "github.com/sergeyslonimsky/core/rabbitmq"
)

func main() {
    ctx := context.Background()
    cfg := rabbitmq.Config{Host: "rabbitmq", Port: "5672", User: "app", Password: "secret"}

    publisher, err := rabbitmq.NewPublisher(ctx, cfg)
    if err != nil { log.Fatal(err) }

    host := rabbitmq.NewConsumerHost(cfg)
    host.AddConsumer(rabbitmq.NewConsumer[OrderEvent](
        "orders", &orderProcessor{},
        rabbitmq.WithExchange(rabbitmq.ExchangeConfig{
            Name: "orders.exchange", Kind: rabbitmq.ExchangeKindTopic, Durable: true,
        }),
        rabbitmq.WithQueue(rabbitmq.QueueConfig{Durable: true}),
        rabbitmq.WithBindQueue(rabbitmq.BindQueueConfig{
            Exchange: "orders.exchange", RoutingKey: "orders.*",
        }),
    ))

    a := app.New()
    a.Add(publisher)  // Resource — connects in NewPublisher, shuts down here
    a.Add(host)       // Runner   — Run starts consumer loops
    log.Fatal(a.Run())
}
```

## Configuration

```go
type Config struct {
    Host, Port, User, Password string
}
```

## Options

### Connector

- `WithReconnectAttempts(uint)` — default 3.
- `WithReconnectWait(time.Duration)` — default 2s.
- `WithRetriedListener(func(rabbitroutine.Retried))`
- `WithDialedListener(func(rabbitroutine.Dialed))`
- `WithAMQPNotifiedListener(func(rabbitroutine.AMQPNotified))`

### Publisher

- `WithPublisherLogger(*slog.Logger)` — for lifecycle events.
- `WithPublisherConnector(ConnectorInterface)` — share a connector with a host.
- `WithPublisherConnectorOptions(...ConnectorOption)` — applied to the auto-built connector.
- `WithPublishMaxAttempts(uint)` — default 16.
- `WithPublishLinearDelay(time.Duration)` — default 10ms.

### ConsumerHost

- `WithHostLogger(*slog.Logger)`
- `WithHostConnector(ConnectorInterface)` — share with a publisher.
- `WithHostConnectorOptions(...ConnectorOption)`

### Consumer[I]

Options are NOT generic — the payload type parameter `I` lives on `NewConsumer[I]` and `Consumer[I]` only:

- `WithConsumerConfig(ConsumerConfig)` — Consume call settings (AutoAck, Exclusive, etc.).
- `WithExchange(ExchangeConfig)` — declare exchange in Declare.
- `WithQueue(QueueConfig)` — declare queue.
- `WithBindQueue(BindQueueConfig)` — bind queue to exchange.
- `WithConsumeOpts(ConsumeOpts)` — QoS prefetch.
- `WithWorkerCount(int)` — number of goroutines processing deliveries for this consumer. Default: 1 (ordered, matches default `PrefetchCount=1`). Raise with care: >1 loses per-queue order.
- `WithConsumerLogger(*slog.Logger)` — per-message error logging. Default: `slog.Default()`.

To install a custom body marshaller (the only type-dependent knob), use `Consumer[I].SetMarshaller`:

```go
c := rabbitmq.NewConsumer[OrderEvent]("orders", processor,
    rabbitmq.WithQueue(queueCfg),
)
c.SetMarshaller(func(body []byte, dst *OrderEvent) error { /* ... */ return nil })
host.AddConsumer(c)
```

## Observability

`WithOtel` is intentionally **not** provided yet. The reason: rabbitroutine's reconnect-loop layer makes per-publish span attribution awkward (spans need to survive a reconnect), and there is no upstream `kotel`-equivalent for amqp091-go. Manual span injection is straightforward inside your `Processor.Process` method:

```go
func (p *orderProcessor) Process(ctx context.Context, msg rabbitmq.Message[OrderEvent]) error {
    ctx, span := otel.Tracer("orders").Start(ctx, "process_order")
    defer span.End()
    // ...
}
```

## Lifecycle

- `Publisher` — `lifecycle.Resource`. Connects in the constructor; reconnect loop runs in a background goroutine rooted in `context.Background` (NOT the constructor's ctx — that only bounds the initial dial). `Shutdown(ctx)` cancels the loop and waits with the ctx as a deadline. This is the ONLY correct way to stop the publisher.
- `ConsumerHost` — `lifecycle.Runner`. Connects + starts every registered consumer when `Run(ctx)` is called. `Shutdown(ctx)` cancels and waits.
- `Consumer[I].Consume` uses a bounded worker pool (see `WithWorkerCount`). On ctx cancellation or broker disconnect, all workers drain their current Ack/Nack before the consume loop returns — no Ack-after-close races.
- `Healthchecker` is intentionally **not** implemented — see "Observability" rationale.

Recommended ordering inside `app.App`:

```go
a.Add(otelProvider)  // Resource — first registered, last shut down
a.Add(publisher)     // Resource — sustained connection, drains on shutdown
a.Add(consumerHost)  // Runner   — fans out consumers
```

## Extending

```go
inner := connector.Inner()  // *rabbitroutine.Connector for raw access
```

## Testing

Unit tests cover constructor wiring without a live broker. Integration tests run against a real broker via testcontainers and live in `internal/integration/` (separate Go module).

## See also

- [`core/app`](../app/README.md) — register publishers and hosts here.
- [`core/lifecycle`](../lifecycle/README.md) — the `Resource` / `Runner` contracts.
