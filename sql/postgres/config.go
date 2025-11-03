package postgres

import (
	"fmt"

	_ "github.com/lib/pq" // postgres connection lib
)

type Config struct {
	Host     string
	Port     string
	User     string
	Password string
	Name     string
	SSLMode  string
}

func (c Config) GetDataSource() string {
	return fmt.Sprintf(
		"user=%s password=%s dbname=%s host=%s port=%s sslmode=%s",
		c.User, c.Password, c.Name, c.Host, c.Port, c.getSSLMode(),
	)
}

func (c Config) GetDriverName() string {
	return "postgres"
}

func (c Config) getSSLMode() string {
	if c.SSLMode == "" {
		return "disable"
	}

	return c.SSLMode
}
