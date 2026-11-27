module github.com/sergeyslonimsky/core

go 1.27

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/Masterminds/squirrel v1.5.4
	github.com/alicebob/miniredis/v2 v2.40.0
	github.com/furdarius/rabbitroutine v0.8.2
	github.com/golang-jwt/jwt/v5 v5.3.1
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.13.0
	github.com/jmoiron/sqlx v1.4.0
	github.com/nats-io/nats-server/v2 v2.15.3
	github.com/nats-io/nats.go v1.56.0
	github.com/rabbitmq/amqp091-go v1.17.0
	github.com/redis/go-redis/extra/redisotel/v9 v9.23.0
	github.com/redis/go-redis/v9 v9.23.0
	github.com/stretchr/testify v1.12.1
	github.com/twmb/franz-go v1.22.3
	github.com/twmb/franz-go/plugin/kotel v1.7.1
	github.com/uptrace/opentelemetry-go-extra/otelsql v0.4.0
	github.com/uptrace/opentelemetry-go-extra/otelsqlx v0.4.0
	go.etcd.io/etcd/client/v3 v3.7.4
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.72.0
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.72.0
	go.opentelemetry.io/otel v1.47.0
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp v0.23.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.47.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.47.0
	go.opentelemetry.io/otel/sdk v1.47.0
	go.opentelemetry.io/otel/sdk/log v1.47.0
	go.opentelemetry.io/otel/sdk/metric v1.47.0
	go.opentelemetry.io/otel/trace v1.47.0
	golang.org/x/sync v0.25.0
	google.golang.org/grpc v1.85.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/antithesishq/antithesis-sdk-go v0.9.0 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/coreos/go-systemd/v22 v22.7.0 // indirect
	github.com/felixge/httpsnoop v1.1.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/go-tpm v0.9.8 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.34.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.4 // indirect
	github.com/klauspost/compress v1.20.5 // indirect
	github.com/lann/builder v0.0.0-20180802200727-47ae307949d0 // indirect
	github.com/lann/ps v0.0.0-20150810152359-62de8c46ede0 // indirect
	github.com/lib/pq v1.12.3 // indirect
	github.com/minio/highwayhash v1.0.4 // indirect
	github.com/nats-io/jwt/v2 v2.8.3 // indirect
	github.com/nats-io/nkeys v0.4.17 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.34 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/redis/go-redis/extra/rediscmd/v9 v9.23.0 // indirect
	github.com/stretchr/objx v0.5.3 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.15.1 // indirect
	github.com/yuin/gopher-lua v1.1.2 // indirect
	go.etcd.io/etcd/api/v3 v3.7.4 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.7.4 // indirect
	go.opentelemetry.io/auto/sdk v1.2.2 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.47.0 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
	go.opentelemetry.io/proto/otlp v1.11.1 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.28.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/crypto v0.59.0 // indirect
	golang.org/x/net v0.63.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
	golang.org/x/text v0.44.0 // indirect
	golang.org/x/time v0.16.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20261118221215-c8830f628b3f // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261118221215-c8830f628b3f // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

retract v1.14.0 // Tagged on the wrong commit: identical to v1.13.0, lacks WithUnencryptedHTTP2. Use v1.14.1.
