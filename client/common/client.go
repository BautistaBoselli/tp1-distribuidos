package common

import (
	"net"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ServerConfig struct {
	Address string `mapstructure:"address"`
}

type LogConfig struct {
	Level string `mapstructure:"level"`
}

type BatchConfig struct {
	Amount int `mapstructure:"amount"`
}

type Config struct {
	ID     string       `mapstructure:"id"`
	Server ServerConfig `mapstructure:"server"`
	Log    LogConfig    `mapstructure:"log"`
	Batch  BatchConfig  `mapstructure:"batch"`
}

type Client struct {
	config Config
	conn   net.Conn
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config Config) *Client {
	conn, err := net.Dial("tcp", config.Server.Address)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client: %v | error: %v",
			config.ID,
			err,
		)
	}

	client := &Client{
		config: config,
		conn:   conn,
	}
	return client
}

func (c *Client) Cancel() {
	log.Debugf("action: cerrar_conexion | result: success | client: %v", c.config.ID)
	c.Close()
}

func (c *Client) Close() {
	c.conn.Close()
}
