package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/furdarius/rabbitroutine"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultMaxAttempts     = 16
	defaultLinearDelayTime = 10 * time.Millisecond
)

type Publisher interface {
	Publish(ctx context.Context, exchange, routingKey string, message any, opts PublishOpts) error
	Run(ctx context.Context) error
}

type PublishOpts struct {
	ContentType string
	MessageID   string
	AppID       string
	UserID      string
	Priority    uint8
	Type        string
}

type RabbitPublisher struct {
	conn      Connector
	publisher rabbitroutine.Publisher
}

func NewRabbitPublisher(cfg Config) *RabbitPublisher {
	conn := NewRabbitConnector(cfg)

	return NewRabbitPublisherWithConnector(conn)
}

func NewRabbitPublisherWithConnector(conn Connector) *RabbitPublisher {
	pool := rabbitroutine.NewPool(conn.GetConnector())
	ensurePub := rabbitroutine.NewEnsurePublisher(pool)
	pub := rabbitroutine.NewRetryPublisher(
		ensurePub,
		rabbitroutine.PublishMaxAttemptsSetup(defaultMaxAttempts),
		rabbitroutine.PublishDelaySetup(rabbitroutine.LinearDelay(defaultLinearDelayTime)),
	)

	return &RabbitPublisher{
		conn:      conn,
		publisher: pub,
	}
}

func (p *RabbitPublisher) Run(ctx context.Context) error {
	if err := p.conn.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	return nil
}

func (p *RabbitPublisher) Publish(
	ctx context.Context,
	exchange, routingKey string,
	message any,
	opts PublishOpts,
) error {
	body, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	if err := p.publisher.Publish(ctx, exchange, routingKey, amqp.Publishing{ //nolint:exhaustruct
		ContentType: opts.ContentType,
		Priority:    opts.Priority,
		MessageId:   opts.MessageID,
		Type:        opts.Type,
		UserId:      opts.UserID,
		AppId:       opts.AppID,
		Body:        body,
	}); err != nil {
		return fmt.Errorf("publish message: %w", err)
	}

	return nil
}
