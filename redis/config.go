package redis

type Config struct {
	Host     string
	Port     string
	Password string
	DB       int
}

func (c Config) GetAddr() string {
	return c.Host + ":" + c.Port
}

func (c Config) GetPassword() string {
	return c.Password
}

func (c Config) GetDB() int {
	return c.DB
}
