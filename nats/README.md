# nats

Wrapper around `github.com/nats-io/nats.go` for Core NATS (at-most-once) and
NATS JetStream (at-least-once) messaging with the `core` lifecycle contract:
typed `Config`, functional `Option`s, `Shutdown(ctx)`, and `Run(ctx)`.

## Primary types

| Type | lifecycle | Use for |
|---|---|---|
| `Publisher` | `Resource` | Publishing to Core NATS subjects or JetStream streams. |
| `Subscriber[I]` | `Runner` | Consuming Core NATS messages (at-most-once). |
| `JetStreamConsumer[I]` | `Runner` | Consuming a durable JetStream consumer (at-least-once). |
| `JSONPublisher[T]` | (wrapper) | Type-safe JSON publishing to a fixed subject. |

Connections are established **eagerly** in the constructors — invalid URLs
or unreachable brokers surface at boot, not on first publish. JetStream
streams and consumers are also resolved at construct time so configuration
drift fails fast.

NATS subscriptions and the JetStream Consume callback are registered in
`Run`. Use `Ready()` (a `<-chan struct{}` closed once subscription is live)
to block deterministically — useful for tests and synchronous startup.

## Quickstart — Core NATS

```go
package main

import (
    "context"
    "log"

    "github.com/sergeyslonimsky/core/app"
    corenats "github.com/sergeyslonimsky/core/nats"
)

type Event struct {
    ID string `json:"id"`
}

type eventProcessor struct{}

func (p *eventProcessor) Process(_ context.Context, msg corenats.Message[Event]) error {
    log.Printf("got %s on %s", msg.Payload.ID, msg.Subject)
    return nil
}

func main() {
    ctx := context.Background()
    cfg := corenats.Config{URL: "nats://localhost:4222"}

    publisher, err := corenats.NewPublisher(ctx, cfg)
    if err != nil { log.Fatal(err) }

    subscriber, err := corenats.NewSubscriber[Event](
        cfg, "events.core", &eventProcessor{},
        corenats.SubscriberHandlers[Event]{}, // zero value = JSON decode, log-only errors
        corenats.WithSubscriberQueueGroup("workers"),
    )
    if err != nil { log.Fatal(err) }

    a := app.New()
    a.Add(publisher)  // Resource
    a.Add(subscriber) // Runner
    log.Fatal(a.Run())
}
```

## Quickstart — JetStream

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/nats-io/nats.go/jetstream"
    "github.com/sergeyslonimsky/core/app"
    corenats "github.com/sergeyslonimsky/core/nats"
)

type Event struct{ ID string `json:"id"` }

type eventProcessor struct{}

func (p *eventProcessor) Process(_ context.Context, msg corenats.Message[Event]) error {
    log.Printf("got %s seq=%d delivery=%d",
        msg.Payload.ID, msg.Sequence, msg.NumDelivered)
    return nil
}

func main() {
    ctx := context.Background()
    cfg := corenats.Config{URL: "nats://localhost:4222"}

    publisher, err := corenats.NewPublisher(ctx, cfg,
        corenats.WithPublisherStream(jetstream.StreamConfig{
            Name:     "EVENTS",
            Subjects: []string{"events.>"},
        }),
    )
    if err != nil { log.Fatal(err) }

    consumer, err := corenats.NewJetStreamConsumer[Event](ctx,
        cfg, "EVENTS", "event-worker", &eventProcessor{},
        corenats.JSConsumerHandlers[Event]{}, // zero value = JSON decode, Nak on error
        corenats.WithJSSubjects("events.js"),
        corenats.WithJSConsumerConfig(func(c *jetstream.ConsumerConfig) {
            c.MaxDeliver = 5
            c.AckWait    = 30 * time.Second
        }),
    )
    if err != nil { log.Fatal(err) }

    a := app.New()
    a.Add(publisher)
    a.Add(consumer)
    log.Fatal(a.Run())
}
```

## Configuration

`Config` is the declarative input — typically populated from env/yaml:

| Field | Default | Purpose |
|---|---|---|
| `URL` | — (required) | Server URL. |
| `User`, `Password` | — | Basic auth. |
| `Token` | — | Token auth. |
| `CredsFile` | — | NATS `.creds` file. |
| `ConnectionName` | `"core-nats"` | Reported to the server (visible in monitoring). |
| `DialTimeout` | `10s` | Initial connect bound. |
| `ReconnectWait` | `2s` | Delay between reconnect attempts. |
| `MaxReconnects` (`*int`) | `nil` → nats.go default (60) | `nil` = library default, `IntPtr(0)` = no retries, `IntPtr(MaxReconnectsForever)` (=-1) = retry forever, `IntPtr(n)` = cap at n. |

For TLS, NKey, JWT, custom inbox prefixes, NoEcho, or anything else not in
`Config`, use `WithNATSOptions(...nats.Option)` (see below).

## Options

Three option families exist — `PublisherOption`, `SubscriberOption`, and
`JSConsumerOption`. **Any `CommonOption` value satisfies all three**, so the
common helpers can be passed to any constructor:

| Option | Publisher | Subscriber | JS Consumer |
|---|:-:|:-:|:-:|
| `WithLogger(*slog.Logger)` | ✓ | ✓ | ✓ |
| `WithConnection(*nats.Conn)` | ✓ | ✓ | ✓ |
| `WithNATSOptions(...nats.Option)` | ✓ | ✓ | ✓ |
| `WithOtel()` | ✓ | ✓ | ✓ |
| `WithPublisherStream(jetstream.StreamConfig)` | ✓ | — | — |
| `WithSubscriberQueueGroup(string)` | — | ✓ | — |
| `WithSubscriberWorkerCount(int)` | — | ✓ | — |
| `WithSubscriberChannelBuffer(int)` | — | ✓ | — |
| `WithJSStream(jetstream.StreamConfig)` | — | — | ✓ |
| `WithJSSubjects(...string)` | — | — | ✓ |
| `WithJSConsumerConfig(func(*jetstream.ConsumerConfig))` | — | — | ✓ |
| `WithJSAssumeExistingConsumer()` | — | — | ✓ |
| `WithJSWorkerCount(int)` | — | — | ✓ |
| `WithJSDeliveryBuffer(int)` | — | — | ✓ |

The Subscriber's subject filter and the JetStreamConsumer's `stream`/`durable`
are **required positional arguments** to the constructor — not options.
Payload-typed callbacks (custom decoder, error handler) live on the
`SubscriberHandlers[I]` / `JSConsumerHandlers[I]` struct, also passed
positionally; pass the zero value for defaults.

### Custom NATS options

```go
publisher, err := corenats.NewPublisher(ctx, cfg,
    corenats.WithNATSOptions(
        nats.Secure(&tls.Config{InsecureSkipVerify: false}),
        nats.RootCAs("/etc/ssl/ca.pem"),
    ),
)
```

### Custom JetStream consumer settings (mutator)

`WithJSConsumerConfig` receives a config pre-populated with `Durable`,
`AckPolicy=Explicit`, and your `WithJSSubjects` filter. Mutate only what you
need:

```go
corenats.WithJSConsumerConfig(func(c *jetstream.ConsumerConfig) {
    c.MaxDeliver = 5
    c.AckWait    = 30 * time.Second
    c.BackOff    = []time.Duration{1*time.Second, 5*time.Second, 30*time.Second}
})
```

`AckPolicy` is automatically restored to `AckExplicitPolicy` if your mutator
leaves it at the zero value (`AckNonePolicy`), so a forgetful mutator
cannot silently downgrade at-least-once delivery. To use a different
non-default ack policy, set it explicitly inside the mutator.

### Externally-managed JetStream consumer

If the durable consumer is provisioned by ops tooling (Terraform, CLI, etc.)
and the app must not modify it:

```go
consumer, err := corenats.NewJetStreamConsumer[Event](ctx,
    cfg, "EVENTS", "event-worker", &processor{},
    corenats.WithJSAssumeExistingConsumer(),
)
```

The constructor calls `js.Consumer(...)` instead of
`CreateOrUpdateConsumer` and fails if the consumer does not exist.
`WithJSConsumerConfig` and `WithJSSubjects` are ignored in this mode.

## Error handling

### Subscriber (Core NATS)

There is no ack model — failing messages cannot be retried at the protocol
level. Install a `SubscriberErrorHandler[I]` via `SubscriberHandlers` for
observability or DLQ side-effects:

```go
subscriber, _ := corenats.NewSubscriber[Event](
    cfg, "events.core", &proc{},
    corenats.SubscriberHandlers[Event]{
        ErrorHandler: func(ctx context.Context, msg corenats.Message[Event], err error) {
            metrics.Counter("nats.subscriber.error").Inc()
            _ = dlqPublisher.PublishJSON(ctx, "dlq.events", dlqWrapper{msg, err.Error()})
        },
    },
)
```

A non-JSON message body with a custom `Content-Type` header (anything other
than empty or `application/json`) is rejected with `ErrUnsupportedMessage`
by the default decoder — install a custom `BodyMarshaller[I]` via
`SubscriberHandlers.Marshaller` to accept other payloads.

### JetStreamConsumer

Processor errors flow through a `JSErrorHandler[I]` that returns one of:

| Action | Effect |
|---|---|
| `JSErrorActionNak` | NAK without delay — broker redelivers after `AckWait`. Default. |
| `JSErrorActionNakDelay` | NAK with the returned delay. |
| `JSErrorActionTerm` | Mark as terminal — broker does NOT redeliver. Poison-pill path. |
| `JSErrorActionAck` | Swallow the error (caller has already DLQ'd, advance the stream). |
| `JSErrorActionStop` | Fatal — `Run` returns the wrapped error (only when ctx is still live; shutdown-time errors are NAK'd instead). |

```go
consumer, _ := corenats.NewJetStreamConsumer[Event](ctx,
    cfg, "EVENTS", "event-worker", &proc{},
    corenats.JSConsumerHandlers[Event]{
        ErrorHandler: func(ctx context.Context, msg corenats.Message[Event], err error) (corenats.JSErrorAction, time.Duration) {
            if errors.Is(err, ErrPoison) {
                return corenats.JSErrorActionTerm, 0
            }
            if errors.Is(err, ErrUpstreamDown) {
                return corenats.JSErrorActionStop, 0
            }
            return corenats.JSErrorActionNakDelay, 5 * time.Second
        },
    },
)
```

Without a handler (zero-value `JSConsumerHandlers`) the default is `Nak`
(redeliver after `AckWait`).

## Publishing

### Headers, reply, and JSON helpers

```go
publisher.PublishJSON(ctx, "events.core", evt,
    corenats.WithHeaders(nats.Header{"Trace-ID": []string{traceID}}),
    corenats.WithReply("inbox.42"),
)
```

`PublishJSON` automatically sets `Content-Type: application/json` if no
explicit value is supplied.

### JetStream — MsgID dedup, expected sequence

```go
ack, err := publisher.PublishJSONJS(ctx, "events.js", evt,
    corenats.WithJSMsgID(evt.ID),                // server-side dedup window
    corenats.WithJSExpectedStream("EVENTS"),     // belt-and-braces
    corenats.WithJSExpectedLastSubjectSequence(prevSeq),
)
if err != nil { return err }
log.Printf("ack seq=%d duplicate=%v", ack.Sequence, ack.Duplicate)
```

### Typed JSON publisher

```go
type Order struct{ ID string `json:"id"` }

pub := corenats.NewJSONPublisher[Order](publisher, "events.orders.created")
_ = pub.Publish(ctx, Order{ID: "o-1"})
_, _ = pub.PublishJS(ctx, Order{ID: "o-1"})
```

## Custom body marshaller

```go
consumer, _ := corenats.NewJetStreamConsumer[Event](ctx,
    cfg, "EVENTS", "event-worker", &proc{},
    corenats.JSConsumerHandlers[Event]{
        Marshaller: func(body []byte, p *Event) error {
            return proto.Unmarshal(body, p)
        },
    },
)
```

The marshaller is type-parameterized (`BodyMarshaller[I]`) and supplied at
construction time, eliminating the race window the previous `SetMarshaller`
method had. When a custom marshaller is installed, the `Content-Type` check
is skipped.

## Concurrency

Both `Subscriber` and `JetStreamConsumer` accept worker-count and
channel-buffer options:

```go
corenats.WithSubscriberWorkerCount(8)
corenats.WithSubscriberChannelBuffer(1024)

corenats.WithJSWorkerCount(8)
corenats.WithJSDeliveryBuffer(1024)
```

Worker count default 1 preserves per-subject ordering. Channel buffer
default 256 absorbs short bursts; raise it if you see SlowConsumer warnings
in the async error log.

For JetStream, keep `worker_count ≤ MaxAckPending` (defaults to 1000) or
the extra workers starve.

## OpenTelemetry

```go
import (
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/propagation"
)

func main() {
    otel.SetTracerProvider(...)
    otel.SetTextMapPropagator(propagation.TraceContext{})

    publisher, _ := corenats.NewPublisher(ctx, cfg, corenats.WithOtel())
    subscriber, _ := corenats.NewSubscriber[Event](
        cfg, "events.core", proc, corenats.SubscriberHandlers[Event]{},
        corenats.WithOtel(),
    )
    ...
}
```

When `WithOtel()` is set:

- **Publisher** opens a `SpanKindProducer` span per `Publish*`/`Publish*JS`
  call and injects the trace context into the outgoing `nats.Header` via
  the global `TextMapPropagator`.
- **Subscriber** / **JetStreamConsumer** extract the trace context from
  incoming headers and open a `SpanKindConsumer` span — chained to the
  producer span — for every `Process` invocation.

Span attributes follow OpenTelemetry messaging conventions:
`messaging.system=nats`, `messaging.destination.name=<subject>`,
`messaging.operation`, plus for JetStream `messaging.message.sequence` and
`messaging.nats.delivery_attempt`.

Register a non-noop `TracerProvider` AND `TextMapPropagator` before the
constructors — without the propagator, trace context is never written to
headers and producer↔consumer spans do not chain.

## Connection sharing

```go
publisher, _ := corenats.NewPublisher(ctx, cfg)
subscriber, _ := corenats.NewSubscriber[Event](
    cfg, "events.core", proc, corenats.SubscriberHandlers[Event]{},
    corenats.WithConnection(publisher.Conn()),
)
```

Components built with `WithConnection` do NOT close the underlying
connection on `Shutdown` — the caller (here: the publisher) retains
ownership. Note that `Publisher.Shutdown` will still `Flush` the connection
even when it does not own it, so pending Core NATS sends are not silently
dropped.

**Shutdown ordering matters when sharing**: shut down the consumers first
(they only release their own runtime state), then the publisher (which
closes the shared connection). The reverse order will close the connection
from under a still-running consumer.

## Shutdown semantics

| Type | Behavior |
|---|---|
| `Publisher.Shutdown(ctx)` | Always flushes buffered Core NATS messages (bounded by ctx; falls back to 10s if no deadline), then closes the connection if owned. Idempotent — subsequent calls return the first flush error. |
| `Subscriber.Shutdown(ctx)` | Cancels `Run`, waits for it to drain (bounded by ctx). Returns `ctx.Err()` on timeout but ALWAYS closes the owned connection. |
| `JetStreamConsumer.Shutdown(ctx)` | Cancels `Run`. Run stops new deliveries, closes the internal channel, waits for in-flight workers, then NAKs anything left buffered so the broker can redeliver immediately. Returns `ctx.Err()` on timeout but ALWAYS closes the owned connection. |

In-flight Core NATS publishes that have not yet been transmitted to the
broker WILL be lost without `Flush` — call `Publisher.Flush(ctx)` before
shutdown when delivery matters.

## Errors

All sentinel errors are exported and can be matched with `errors.Is`:

```go
ErrEmptyURL
ErrNilProcessor
ErrEmptyStream
ErrEmptyDurable
ErrEmptySubjectFilter
ErrAlreadyRunning
ErrInvalidErrorAction
ErrUnsupportedMessage
```
