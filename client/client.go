package main

import (
	"bufio"
	"io"
	"net"
	"os"
	"time"
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
		time.Sleep(30 * time.Millisecond)

	}

	return nil
}

func (c *Client) SendAllSent() error {
	return protocol.Send(c.conn, &protocol.AllSent{})
}

func (c *Client) ReceiveResponse() error {
	queriesCompleted := 0

	for {

		if queriesCompleted >= 4 {
			break
		}

		response, err := protocol.Receive(c.conn)
		if err != nil {
			return err
		}

		switch response.MessageType {
		case protocol.MessageTypeClientResponse1:
			var response1 protocol.ClientResponse1
			response1.Decode(response.Data)
			if response1.Last {
				log.Infof("[QUERY 1 - FINAL]: Windows: %d, Mac: %d, Linux: %d", response1.Windows, response1.Mac, response1.Linux)
				queriesCompleted++
			} else {
				log.Infof("[QUERY 1 - PARCIAL]: Windows: %d, Mac: %d, Linux: %d", response1.Windows, response1.Mac, response1.Linux)
			}
		case protocol.MessageTypeClientResponse2:
			var response2 protocol.ClientResponse2
			response2.Decode(response.Data)
			for i, game := range response2.TopGames {
				log.Infof("[QUERY 2]: Top Game %d: %v (%d)", i+1, game.Name, game.Count)
			}
			queriesCompleted++
		case protocol.MessageTypeClientResponse3:
			var response3 protocol.ClientResponse3
			response3.Decode(response.Data)
			for i, game := range response3.TopStats {
				log.Infof("[QUERY 3]: Top Game %d: %v (%d)", i+1, game.Name, game.Count)
			}
			queriesCompleted++
		case protocol.MessageTypeClientResponse4:
			var response4 protocol.ClientResponse4
			response4.Decode(response.Data)
			if response4.Last {
				log.Infof("[QUERY 4 - FINAL]")
				queriesCompleted++
			} else {
				log.Infof("[QUERY 4 - PARCIAL]: Game %v", response4.Game.Name)
			}
		}

	}

	return nil
}
