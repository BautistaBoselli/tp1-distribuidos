package main

import (
	"errors"
	"net"
	"strconv"
	"time"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared/protocol"
)

type ServerConfig struct {
	Address string `mapstructure:"address"`
}

type LogConfig struct {
	Level string `mapstructure:"level"`
}

type Config struct {
	Server ServerConfig `mapstructure:"server"`
	Log    LogConfig    `mapstructure:"log"`
}

type Server struct {
	serverSocket *net.TCPListener
	middleware   *middleware.Middleware
}

func NewServer(address string) (*Server, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return nil, err
	}

	serverSocket, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return nil, err
	}

	middleware, err := middleware.NewMiddleware("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		return nil, err
	}

	return &Server{serverSocket: serverSocket, middleware: middleware}, nil
}

func (s *Server) Close() {
	s.serverSocket.Close()
}

func (s *Server) Run() {
	defer s.Close()

	s.consumeMessages()

	client, err := s.acceptNewConnection()
	if err != nil {
		log.Errorf("action: accept_connections | result: fail | error: %s", err)
		return
	}

	go s.handleConnection(client)

	select {}

}

func (s *Server) acceptNewConnection() (*Client, error) {
	log.Info("action: accept_connections | result: in_progress")

	clientSocket, err := s.serverSocket.AcceptTCP()
	// Si el error es que el socket ya está cerrado, simplemente terminamos el programa
	if errors.Is(err, net.ErrClosed) {
		return nil, err
	}
	// Si ocurre otro error, lo registramos
	if err != nil {
		log.Errorf("action: accept_connections | result: fail | error: %s", err)
		return nil, err
	}

	client := NewClient(clientSocket, "1")

	log.Infof("action: accept_connections | result: success | agency: %d", client.id)

	return client, nil
}

func (s *Server) handleConnection(client *Client) {
	for {
		msg, err := protocol.Receive(client.conn)
		if err != nil {
			s.handleDisconnect(client, err)
			return
		}

		switch msg.MessageType {

		case protocol.MessageTypeGame:
			game := protocol.GameMessage{}
			game.Decode(msg.Data)
			log.Infof("action: receive_games | result: success | message: %s", game.Lines)
			s.handleGames(client, game)

		case protocol.MessageTypeReview:
			review := protocol.ReviewMessage{}
			review.Decode(msg.Data)
			log.Infof("action: receive_reviews | result: success | message: %s", review.Lines)
			s.handleReviews(client, review)

		default:
			log.Errorf("action: handle_message | result: fail | error: mensaje no soportado %s", msg.MessageType)
			return
		}
	}
}

func (s *Server) consumeMessages() {
	log.Info("Starting to consume messages")

	go func() {
		for {
			err := s.middleware.Consume("reviews", "", func(body []byte) error {
				log.Infof("Processed a message in reviews: %s", string(body))
				return nil
			})
			if err != nil {
				log.Errorf("Failed to consume from reviews exchange: %v", err)
				time.Sleep(5 * time.Second)
			}
		}
	}()

	go func() {
		for {
			err := s.middleware.Consume("games", "", func(body []byte) error {
				log.Infof("Processed a message in games: %s", string(body))
				return nil
			})
			if err != nil {
				log.Errorf("Failed to consume from games exchange: %v", err)
				time.Sleep(5 * time.Second)
			}
		}
	}()

	log.Info("Set up consumers, waiting for messages...")

}

func (s *Server) handleGames(client *Client, game protocol.GameMessage) {
	for _, line := range game.Lines {
		err := s.middleware.Publish("games", "", []byte(line))
		if err != nil {
			log.Errorf("Failed to publish game message: %v", err)
		}
	}
}

func (s *Server) handleReviews(client *Client, review protocol.ReviewMessage) {
	for _, line := range review.Lines {
		err := s.middleware.Publish("reviews", "", []byte(line))
		if err != nil {
			log.Errorf("Failed to publish review message: %v", err)
		}
	}
}

func (s *Server) handleDisconnect(client *Client, err error) {
	log.Errorf("action: handle_disconnect | result: fail | error: %s", err)
	client.conn.Close()
}

type Client struct {
	conn *net.TCPConn
	id   int
}

func NewClient(conn *net.TCPConn, id string) *Client {
	idAsNumber, _ := strconv.Atoi(id)
	return &Client{conn: conn, id: idAsNumber}
}
