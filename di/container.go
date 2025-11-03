package di

import (
	"context"
	"fmt"

	"github.com/spf13/viper"
)

type config interface {
	GetAppEnv() string
	GetServiceName() string
	GetStorage() *viper.Viper
}

type serviceContainer interface {
	AddOnClose(callback func() error)
	onClose() func() error
}

type AbstractContainer[C config, S serviceContainer] interface {
	GetConfig() *C
	GetServices() *S
}

type Container[C config, S serviceContainer] struct {
	config   C
	services S
}

type ServiceOpts[S serviceContainer] func(*S)

func NewContainer[C config, S serviceContainer](
	ctx context.Context,
	initConfig func(context.Context) (C, error),
	initServices func(context.Context, C, ...ServiceOpts[S]) (S, error),
	opts ...ServiceOpts[S],
) (*Container[C, S], func() error, error) {
	cnf, err := initConfig(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("init config: %w", err)
	}

	services, err := initServices(ctx, cnf, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("init services: %w", err)
	}

	return &Container[C, S]{
		config:   cnf,
		services: services,
	}, services.onClose(), nil
}

func (c *Container[C, S]) GetConfig() C {
	return c.config
}

func (c *Container[C, S]) GetServices() S {
	return c.services
}
