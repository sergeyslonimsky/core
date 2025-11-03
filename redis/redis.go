package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type config interface {
	GetAddr() string
	GetPassword() string
	GetDB() int
}

type Client struct {
	client *redis.Client
}

type Options func(*redis.Options)

func NewRedis(ctx context.Context, config config, options ...Options) (*Client, error) {
	opts := &redis.Options{ //nolint:exhaustruct
		Addr:     config.GetAddr(),
		Password: config.GetPassword(),
		DB:       config.GetDB(),
	}

	for _, opt := range options {
		opt(opts)
	}

	client := redis.NewClient(opts)

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis client: %w", err)
	}

	return &Client{client: client}, nil
}

func (c *Client) GetRedisClient() *redis.Client {
	return c.client
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
	result, err := c.client.Get(ctx, key).Result()
	if err != nil {
		return "", fmt.Errorf("get key: %w", err)
	}

	return result, nil
}

func (c *Client) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	if err := c.client.Set(ctx, key, value, ttl).Err(); err != nil {
		return fmt.Errorf("redis set: %w", err)
	}

	return nil
}

func (c *Client) Del(ctx context.Context, keys ...string) error {
	if err := c.client.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("redis delele: %w", err)
	}

	return nil
}
