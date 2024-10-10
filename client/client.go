package main

import (
	"bufio"
	"io"
	"net"
	"os"
	"tp1-distribuidos/shared/protocol"
)

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

func (c *Client) SendGames(file *os.File) error {
	defer file.Close()
	log.Infof("action: enviar_juegos | result: in_progress | client: %v", c.config.ID)

	// reader := csv.NewReader(file)
	// _, _ = reader.Read()

	reader := bufio.NewReader(file)
	reader.ReadString('\n')

	i := 0
	for {
		i++
		// if i > 50 {
		// 	break
		// }
		batch := protocol.ClientGame{
			Lines: make([]string, 0),
		}

		for i := 0; i < c.config.Batch.Amount; i++ {
			record, err := reader.ReadString('\n')
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Errorf("action: enviar_juegos | result: fail | client: %v | error csv: %v", c.config.ID, err)
				return err
			}

			batch.Lines = append(batch.Lines, record[:len(record)-1])
		}

		if len(batch.Lines) == 0 {
			break
		}

		err := protocol.Send(c.conn, &batch)
		if err != nil {
			log.Errorf("action: enviar_juegos | result: fail | client: %v | error: %v", c.config.ID, err)
			return err
		}

	}

	return nil
}

func (c *Client) SendReviews(file *os.File) error {
	defer file.Close()
	log.Infof("action: enviar_reviews | result: in_progress | client: %v", c.config.ID)

	reader := bufio.NewReader(file)
	reader.ReadString('\n')

	// i := 0
	for {
		// i++
		// // if i > 50 {
		// // 	break
		// // }

		batch := protocol.ClientReview{
			Lines: make([]string, 0),
		}

		for i := 0; i < c.config.Batch.Amount; i++ {
			record, err := reader.ReadString('\n')
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Errorf("action: enviar_reviews | result: fail | client: %v | error: %v", c.config.ID, err)
				return err
			}

			batch.Lines = append(batch.Lines, record[:len(record)-1])
		}

		if len(batch.Lines) == 0 {
			break
		}

		err := protocol.Send(c.conn, &batch)
		if err != nil {
			log.Errorf("action: enviar_reviews | result: fail | client: %v | error: %v", c.config.ID, err)
			return err
		}
		// time.Sleep(100 * time.Millisecond)

	}

	return nil
}

func (c *Client) SendAllSent() error {
	return protocol.Send(c.conn, &protocol.AllSent{})
}
