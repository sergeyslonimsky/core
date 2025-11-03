//go:build integration

package testhelpers

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.in/yaml.v3"
)

// SetupEtcdContainer starts an etcd container for testing.
func SetupEtcdContainer(t *testing.T) (string, func()) {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping etcd integration test in short mode")
	}

	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "quay.io/coreos/etcd:v3.5.9",
		ExposedPorts: []string{EtcdPort + "/tcp"},
		Cmd: []string{
			"/usr/local/bin/etcd",
			"--name=test",
			"--data-dir=/etcd-data",
			"--listen-client-urls=http://0.0.0.0:" + EtcdPort,
			"--advertise-client-urls=http://0.0.0.0:" + EtcdPort,
			"--listen-peer-urls=http://0.0.0.0:2380",
		},
		WaitingFor: wait.ForLog("ready to serve client requests"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "failed to start etcd container")

	mappedPort, err := container.MappedPort(ctx, EtcdPort)
	require.NoError(t, err, "failed to get mapped port")

	hostIP, err := container.Host(ctx)
	require.NoError(t, err, "failed to get container host")

	endpoint := "http://" + net.JoinHostPort(hostIP, mappedPort.Port())

	cleanup := func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate etcd container: %v", err)
		}
	}

	return endpoint, cleanup
}

// PutEtcdValue writes a value to etcd for testing.
func PutEtcdValue(t *testing.T, endpoint, key string, data map[string]interface{}) {
	t.Helper()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: ShortTestTimeout,
	})
	require.NoError(t, err, "failed to create etcd client")

	defer cli.Close()

	yamlData, err := yaml.Marshal(data)
	require.NoError(t, err, "failed to marshal YAML data")

	ctx, cancel := context.WithTimeout(context.Background(), ShortTestTimeout)
	defer cancel()

	_, err = cli.Put(ctx, key, string(yamlData))
	require.NoError(t, err, "failed to put value in etcd")
}

// SetupKafkaContainer starts a Kafka-compatible container for integration testing.
// Uses RedPanda (Kafka-compatible) which is lighter and faster than Kafka.
// Returns the broker URL with a randomly assigned port to enable parallel test execution.
func SetupKafkaContainer(t *testing.T) (string, func()) {
	t.Helper()

	// Use RedPanda - Kafka-compatible, lighter (~250MB vs 800MB+), faster startup
	// Fully compatible with Kafka protocol, no code changes needed
	redpandaContainer, err := redpanda.Run(t.Context(),
		"docker.redpanda.com/redpandadata/redpanda:v24.2.4",
	)
	require.NoError(t, err, "failed to start RedPanda container")

	// Get broker URL with dynamically assigned port
	brokerAddr, err := redpandaContainer.KafkaSeedBroker(t.Context())
	require.NoError(t, err, "failed to get broker address")

	cleanup := func() {
		if err := redpandaContainer.Terminate(t.Context()); err != nil {
			t.Logf("failed to terminate RedPanda container: %v", err)
		}
	}

	return brokerAddr, cleanup
}

// CreateTestTopic creates a topic with the specified number of partitions.
func CreateTestTopic(t *testing.T, brokerURL, topic string, partitions int32) {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokerURL),
		kgo.RequestTimeoutOverhead(DefaultTestTimeout),
	)
	require.NoError(t, err, "failed to create Kafka admin client")

	defer client.Close()

	admin := kadm.NewClient(client)

	ctx, cancel := context.WithTimeout(t.Context(), DefaultTestTimeout)
	defer cancel()

	resp, err := admin.CreateTopics(ctx, partitions, 1, nil, topic)
	require.NoError(t, err, "failed to create topic %s", topic)

	for _, ctr := range resp {
		require.NoError(t, ctr.Err, "topic %s creation failed: %s", ctr.Topic, ctr.ErrMessage)
	}
}

// TestRecord represents a consumed message for testing.
type TestRecord struct {
	Key       []byte
	Value     []byte
	Topic     string
	Partition int32
	Offset    int64
	Headers   map[string]string
}

// ConsumeTestMessages consumes N messages from a topic with a timeout.
func ConsumeTestMessages(t *testing.T, brokerURL, topic string, count int, timeout time.Duration) []TestRecord {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokerURL),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err, "failed to create consumer client for topic %s", topic)

	defer client.Close()

	var records []TestRecord
	for len(records) < count {
		fetches := client.PollFetches(t.Context())
		if err := fetches.Err(); err != nil {
			require.NoError(t, err, "failed to poll messages from topic %s", topic)
		}

		fetches.EachRecord(func(r *kgo.Record) {
			headers := make(map[string]string)
			for _, h := range r.Headers {
				headers[h.Key] = string(h.Value)
			}

			records = append(records, TestRecord{
				Key:       r.Key,
				Value:     r.Value,
				Topic:     r.Topic,
				Partition: r.Partition,
				Offset:    r.Offset,
				Headers:   headers,
			})
		})
	}

	return records
}

// SetupRabbitMQContainer starts a RabbitMQ container for testing.
// Uses RabbitMQ management alpine (~100MB), faster than full version.
// Returns the AMQP URL with a randomly assigned port to enable parallel test execution.
func SetupRabbitMQContainer(t *testing.T) (string, func()) {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping RabbitMQ integration test in short mode")
	}

	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "rabbitmq:3.12-management-alpine",
		ExposedPorts: []string{"5672/tcp", "15672/tcp"},
		WaitingFor:   wait.ForLog("Server startup complete"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "failed to start RabbitMQ container")

	mappedPort, err := container.MappedPort(ctx, "5672")
	require.NoError(t, err, "failed to get mapped port")

	hostIP, err := container.Host(ctx)
	require.NoError(t, err, "failed to get container host")

	amqpURL := "amqp://guest:guest@" + net.JoinHostPort(hostIP, mappedPort.Port())

	cleanup := func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate RabbitMQ container: %v", err)
		}
	}

	return amqpURL, cleanup
}

// CreateRabbitMQQueue creates a queue for testing.
func CreateRabbitMQQueue(t *testing.T, amqpURL, queueName string, durable bool) {
	t.Helper()

	conn, err := amqp.Dial(amqpURL)
	require.NoError(t, err, "failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "failed to open channel")
	defer ch.Close()

	_, err = ch.QueueDeclare(
		queueName,
		durable, // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	require.NoError(t, err, "failed to declare queue %s", queueName)
}

// CreateRabbitMQExchange creates an exchange for testing.
func CreateRabbitMQExchange(t *testing.T, amqpURL, exchangeName, exchangeType string) {
	t.Helper()

	conn, err := amqp.Dial(amqpURL)
	require.NoError(t, err, "failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "failed to open channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		exchangeName,
		exchangeType, // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	require.NoError(t, err, "failed to declare exchange %s", exchangeName)
}

// BindRabbitMQQueue binds a queue to an exchange.
func BindRabbitMQQueue(t *testing.T, amqpURL, queueName, exchangeName, routingKey string) {
	t.Helper()

	conn, err := amqp.Dial(amqpURL)
	require.NoError(t, err, "failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "failed to open channel")
	defer ch.Close()

	err = ch.QueueBind(
		queueName,
		routingKey,
		exchangeName,
		false, // no-wait
		nil,   // arguments
	)
	require.NoError(t, err, "failed to bind queue %s to exchange %s", queueName, exchangeName)
}

// RabbitMQMessage represents a consumed message for testing.
type RabbitMQMessage struct {
	Body        []byte
	ContentType string
	MessageID   string
	RoutingKey  string
	Exchange    string
}

// PublishRabbitMQMessage publishes a test message to RabbitMQ.
func PublishRabbitMQMessage(t *testing.T, amqpURL, exchange, routingKey string, message interface{}) {
	t.Helper()

	conn, err := amqp.Dial(amqpURL)
	require.NoError(t, err, "failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "failed to open channel")
	defer ch.Close()

	body, err := json.Marshal(message)
	require.NoError(t, err, "failed to marshal message")

	err = ch.Publish(
		exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	require.NoError(t, err, "failed to publish message")
}

// ConsumeRabbitMQMessages consumes N messages from a queue with a timeout.
func ConsumeRabbitMQMessages(t *testing.T, amqpURL, queueName string, count int, timeout time.Duration) []RabbitMQMessage {
	t.Helper()

	conn, err := amqp.Dial(amqpURL)
	require.NoError(t, err, "failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err, "failed to open channel")
	defer ch.Close()

	msgs, err := ch.Consume(
		queueName,
		"",    // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	require.NoError(t, err, "failed to consume from queue %s", queueName)

	var result []RabbitMQMessage

	timeoutCh := time.After(timeout)

	for len(result) < count {
		select {
		case msg := <-msgs:
			result = append(result, RabbitMQMessage{
				Body:        msg.Body,
				ContentType: msg.ContentType,
				MessageID:   msg.MessageId,
				RoutingKey:  msg.RoutingKey,
				Exchange:    msg.Exchange,
			})
		case <-timeoutCh:
			t.Fatalf("timeout waiting for messages, got %d out of %d", len(result), count)
		}
	}

	return result
}
