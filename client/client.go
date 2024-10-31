package main

import (
	"bufio"
	"fmt"
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

type ResultsConfig struct {
	Path string `mapstructure:"path"`
}

type Config struct {
	Server  ServerConfig  `mapstructure:"server"`
	Log     LogConfig     `mapstructure:"log"`
	Batch   BatchConfig   `mapstructure:"batch"`
	Results ResultsConfig `mapstructure:"results"`
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
			"action: connect | result: fail| error: %v",
			err,
		)
		return nil
	}

	client := &Client{
		config: config,
		conn:   conn,
	}
	return client
}

func (c *Client) Cancel() {
	log.Debugf("action: cerrar_conexion | result: success")
	c.Close()
}

func (c *Client) Close() {
	c.conn.Close()
}

func (c *Client) SendGames(file *os.File) error {
	defer file.Close()
	log.Infof("action: enviar_juegos | result: in_progress")

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
				log.Errorf("action: enviar_juegos | result: fail | error csv: %v", err)
				return err
			}

			batch.Lines = append(batch.Lines, record[:len(record)-1])
		}

		if len(batch.Lines) == 0 {
			break
		}

		err := protocol.Send(c.conn, &batch)
		if err != nil {
			log.Errorf("action: enviar_juegos | result: fail | error: %v", err)
			return err
		}

	}

	return nil
}

func (c *Client) SendReviews(file *os.File) error {
	defer file.Close()
	log.Infof("action: enviar_reviews | result: in_progress")

	reader := bufio.NewReader(file)
	reader.ReadString('\n')

	for {
		batch := protocol.ClientReview{
			Lines: make([]string, 0),
		}

		for i := 0; i < c.config.Batch.Amount; i++ {
			record, err := reader.ReadString('\n')
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Errorf("action: enviar_reviews | result: fail | error: %v", err)
				return err
			}

			batch.Lines = append(batch.Lines, record[:len(record)-1])
		}

		if len(batch.Lines) == 0 {
			break
		}

		err := protocol.Send(c.conn, &batch)
		if err != nil {
			log.Errorf("action: enviar_reviews | result: fail | error: %v", err)
			return err
		}
		time.Sleep(5 * time.Millisecond)
	}

	return nil
}

func (c *Client) SendAllSent() error {
	return protocol.Send(c.conn, &protocol.AllSent{})
}

func (c *Client) ReceiveResponse() error {
	resultsFile, err := os.Create(c.config.Results.Path)
	if err != nil {
		return err
	}
	defer resultsFile.Close()

	writer := bufio.NewWriter(resultsFile)

	queriesCompleted := 0

	for {

		if queriesCompleted >= 5 {
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
				logResults(writer, fmt.Sprintf("[QUERY 1 - FINAL]: Windows: %d, Mac: %d, Linux: %d", response1.Windows, response1.Mac, response1.Linux))
				queriesCompleted++
			} else {
				logResults(writer, fmt.Sprintf("[QUERY 1 - PARCIAL]: Windows: %d, Mac: %d, Linux: %d", response1.Windows, response1.Mac, response1.Linux))
			}
		case protocol.MessageTypeClientResponse2:
			var response2 protocol.ClientResponse2
			response2.Decode(response.Data)
			for i, game := range response2.TopGames {
				logResults(writer, fmt.Sprintf("[QUERY 2]: Top Game %d: %v (%d)", i+1, game.Name, game.Count))
			}
			queriesCompleted++
		case protocol.MessageTypeClientResponse3:
			var response3 protocol.ClientResponse3
			response3.Decode(response.Data)
			for i, game := range response3.TopStats {
				logResults(writer, fmt.Sprintf("[QUERY 3]: Top Game %d: %v (%d)", i+1, game.Name, game.Count))
			}
			queriesCompleted++
		case protocol.MessageTypeClientResponse4:
			var response4 protocol.ClientResponse4
			response4.Decode(response.Data)
			if response4.Last {
				logResults(writer, "[QUERY 4 - FINAL]")
				queriesCompleted++
			} else {
				logResults(writer, fmt.Sprintf("[QUERY 4 - PARCIAL]: %v", response4.Game.Name))
			}
		case protocol.MessageTypeClientResponse5:
			var response5 protocol.ClientResponse5
			response5.Decode(response.Data)
			for _, game := range response5.TopStats {
				logResults(writer, fmt.Sprintf("[QUERY 5 - PARCIAL]: %v (%d)", game.Name, game.Count))
			}
			if response5.Last {
				logResults(writer, "[QUERY 5 - FINAL]")
				queriesCompleted++
			}
		}

	}

	return nil
}

func logResults(writer *bufio.Writer, string string) {
	log.Infof(string)
	writer.WriteString(string + "\n")
	writer.Flush()
}
