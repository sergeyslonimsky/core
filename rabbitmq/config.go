package rabbitmq

import (
	"fmt"
	"net"
)

type config interface {
	DSN() string
}

type Config struct {
	Host     string
	Port     string
	User     string
	Password string
}

func (c Config) DSN() string {
	return fmt.Sprintf("amqp://%s:%s@%s", c.User, c.Password, net.JoinHostPort(c.Host, c.Port))
}
