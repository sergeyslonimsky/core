# Core

[![CI](https://github.com/sergeyslonimsky/core/actions/workflows/ci.yml/badge.svg)](https://github.com/sergeyslonimsky/core/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/sergeyslonimsky/core)](https://go.dev/doc/go1.25)
[![Go Report Card](https://goreportcard.com/badge/github.com/sergeyslonimsky/core)](https://goreportcard.com/report/github.com/sergeyslonimsky/core)
[![Latest Release](https://img.shields.io/github/v/release/sergeyslonimsky/core)](https://github.com/sergeyslonimsky/core/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![codecov](https://codecov.io/gh/sergeyslonimsky/core/branch/master/graph/badge.svg)](https://codecov.io/gh/sergeyslonimsky/core)

A comprehensive Go library for building production-ready microservices with modern infrastructure components.

## Features

- **Dependency Injection & Configuration** - Dynamic config management with etcd support
- **Message Brokers** - Kafka and RabbitMQ clients with comprehensive testing
- **Databases** - PostgreSQL with sqlx and OpenTelemetry integration
- **Caching** - Redis client wrapper
- **Servers** - HTTP/2 and gRPC servers with middleware
- **Authentication** - JWT token handling (HS256)
- **Observability** - OpenTelemetry integration for traces, metrics, and logs
- **Application Lifecycle** - Server lifecycle management

## Installation

```bash
go get github.com/sergeyslonimsky/core
```

## Packages

### `di` - Dependency Injection & Configuration

Dynamic configuration management with support for:
- **File-based config** - YAML/JSON configuration files
- **etcd integration** - Remote configuration with live reloading
- **Watch mode** - Automatic config updates every 5 seconds
- **Thread-safe** - RWMutex protection for concurrent access

```go
import "github.com/sergeyslonimsky/core/di"

// Create config with etcd watching
cfg, err := di.NewConfig(ctx)
if err != nil {
    log.Fatal(err)
}

// Read config values
dbHost := cfg.GetString("database.host")
port := cfg.GetInt("server.port")

// Watch for changes
portWatcher := cfg.WatchInt("server.port")
currentPort := portWatcher() // Always returns fresh value
```

**Environment variables:**
- `APP_ENV` - Application environment (dev, prod)
- `APP_SERVICE_NAME` - Service name for etcd paths
- `APP_CONFIG_FILE_PATHS` - Comma-separated file paths
- `APP_CONFIG_ETCD_ENDPOINT` - etcd endpoint
- `APP_CONFIG_ETCD_DYNAMIC_PATHS` - Config paths with live reload

### `kafka/v2` - Kafka Client

High-performance Kafka client based on [franz-go](https://github.com/twmb/franz-go):
- **Producer** - Sync and async message production
- **Consumer** - Consumer groups with automatic offset management
- **Testing** - Testcontainers integration for integration tests

```go
import v2 "github.com/sergeyslonimsky/core/kafka/v2"

// Create producer (accepts broker string directly)
producer, err := v2.NewSyncProducer("localhost:9092")
if err != nil {
    log.Fatal(err)
}
defer producer.Close()

// Produce message
err = producer.Produce("my-topic", "key", []byte("message"))
```

### `rabbitmq` - RabbitMQ Client

Production-ready RabbitMQ client with:
- **Publisher** - Thread-safe publishing with retry logic
- **Consumer** - Generic consumer with configurable options
- **Exchange types** - Direct, fanout, topic, headers
- **Connection management** - Automatic reconnection with exponential backoff

```go
import "github.com/sergeyslonimsky/core/rabbitmq"

// Create publisher
cfg := rabbitmq.Config{
    Host: "localhost",
    Port: "5672",
    User: "guest",
    Password: "guest",
}
publisher := rabbitmq.NewRabbitPublisher(cfg)

err := publisher.Run(ctx)
if err != nil {
    log.Fatal(err)
}

// Publish message
type MyMessage struct {
    ID   int    `json:"id"`
    Text string `json:"text"`
}

msg := MyMessage{ID: 1, Text: "Hello"}
err = publisher.Publish(ctx, "my-exchange", "routing.key", msg, rabbitmq.PublishOpts{
    ContentType: "application/json",
})
```

### `redis` - Redis Client

Redis client wrapper with connection pooling.

```go
import "github.com/sergeyslonimsky/core/redis"

// Create Redis client with Config
cfg := redis.Config{
    Host:     "localhost",
    Port:     "6379",
    Password: "password",
    DB:       0,
}

client, err := redis.NewRedis(context.Background(), cfg)
if err != nil {
    log.Fatal(err)
}

// Set value
err = client.Set(ctx, "key", "value", time.Hour)

// Get value
val, err := client.Get(ctx, "key")
```

### `sql` - PostgreSQL Client

PostgreSQL client with sqlx and OpenTelemetry instrumentation:
- **Connection pooling** - Configurable pool settings
- **Tracing** - Automatic query tracing with OpenTelemetry
- **Health checks** - Database connectivity monitoring

```go
import "github.com/sergeyslonimsky/core/sql"

// Create database manager
manager := sql.NewManager(sql.Config{
    Host:     "localhost",
    Port:     "5432",
    User:     "postgres",
    Password: "password",
    Database: "mydb",
    SSLMode:  "disable",
})

db, err := manager.Connect(ctx)
if err != nil {
    log.Fatal(err)
}

// Query
var users []User
err = db.Select(&users, "SELECT * FROM users WHERE active = $1", true)
```

### `http2` - HTTP/2 Server

HTTP/2 server with middleware support:
- **Graceful shutdown** - Context-aware lifecycle
- **Middleware** - Request logging, metrics, recovery
- **TLS support** - HTTPS with HTTP/2

```go
import "github.com/sergeyslonimsky/core/http2"

server := http2.NewServer(":8080")

server.Handle("/health", func(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(http.StatusOK)
    w.Write([]byte("OK"))
})

if err := server.Start(ctx); err != nil {
    log.Fatal(err)
}
```

### `grpc` - gRPC Server

gRPC server with interceptors:
- **Unary interceptors** - Request logging, auth, recovery
- **Stream interceptors** - Bidirectional stream support
- **Reflection** - gRPC reflection API

```go
import "github.com/sergeyslonimsky/core/grpc"

server := grpc.NewServer(":9090")

// Register your services
pb.RegisterMyServiceServer(server.Server, &myServiceImpl{})

if err := server.Start(ctx); err != nil {
    log.Fatal(err)
}
```

### `jwt` - JWT Tokens

JWT token generation and validation with HS256:
- **Token generation** - With custom claims
- **Token validation** - Signature and expiration checks
- **Claims extraction** - Type-safe claims parsing

```go
import "github.com/sergeyslonimsky/core/jwt/hs256"

// Generate token
signer := hs256.NewSigner("secret-key")
token, err := signer.Sign(map[string]interface{}{
    "user_id": 123,
    "role": "admin",
}, time.Hour)

// Validate token
verifier := hs256.NewVerifier("secret-key")
claims, err := verifier.Verify(token)
```

### `otel` - OpenTelemetry

OpenTelemetry integration for distributed tracing, metrics, and logging:
- **OTLP exporters** - HTTP exporters for traces, metrics, logs
- **Auto-instrumentation** - Database, HTTP, gRPC
- **Context propagation** - W3C Trace Context

```go
import "github.com/sergeyslonimsky/core/otel"

// Initialize OpenTelemetry
shutdown, err := otel.InitProvider(ctx, otel.Config{
    ServiceName: "my-service",
    Endpoint: "localhost:4318",
})
if err != nil {
    log.Fatal(err)
}
defer shutdown(ctx)

// Now all instrumented code will send telemetry
```

### `server` - Application Server

Application lifecycle management:
- **Graceful shutdown** - Signal handling (SIGTERM, SIGINT)
- **Multiple servers** - Run HTTP and gRPC simultaneously
- **Health checks** - Startup and readiness probes

```go
import "github.com/sergeyslonimsky/core/server"

app := server.NewApplication()

// Add HTTP server
httpServer := http2.NewServer(":8080")
app.AddServer(httpServer)

// Add gRPC server
grpcServer := grpc.NewServer(":9090")
app.AddServer(grpcServer)

// Run with graceful shutdown
if err := app.Run(ctx); err != nil {
    log.Fatal(err)
}
```

## Testing

The library includes comprehensive test coverage with both unit tests and integration tests.

### Run Unit Tests

```bash
make test
```

### Run Integration Tests

Integration tests require Docker (testcontainers):

```bash
make test-integration
```

### Run All Tests

```bash
make test-all
```

### Run Linter

```bash
make lint
```

### Run Benchmarks

```bash
make bench
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Requirements

- Go 1.25.3 or higher
- Docker (for integration tests)

## Support

If you encounter any issues or have questions, please [open an issue](https://github.com/sergeyslonimsky/core/issues).
