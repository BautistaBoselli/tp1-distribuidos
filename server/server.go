package main

import (
	"encoding/csv"
	"errors"
	"net"
	"strconv"
	"strings"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared/protocol"
)

type ServerConfig struct {
	Address            string `mapstructure:"address"`
	GamesBatchAmount   int    `mapstructure:"gamesBatchAmount"`
	ReviewsBatchAmount int    `mapstructure:"reviewsBatchAmount"`
}

type LogConfig struct {
	Level string `mapstructure:"level"`
}

type ShardingConfig struct {
	Amount int `mapstructure:"amount"`
}

type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	Log      LogConfig      `mapstructure:"log"`
	Sharding ShardingConfig `mapstructure:"sharding"`
}

type Server struct {
	serverSocket    *net.TCPListener
	middleware      *middleware.Middleware
	games           chan protocol.ClientGame
	gamesFinished   bool
	reviews         chan protocol.ClientReview
	reviewsFinished bool
	config          *Config
}

func NewServer(config *Config) (*Server, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", config.Server.Address)
	if err != nil {
		return nil, err
	}

	serverSocket, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return nil, err
	}

	middleware, err := middleware.NewMiddleware(config.Sharding.Amount)
	if err != nil {
		return nil, err
	}

	return &Server{
		serverSocket:    serverSocket,
		middleware:      middleware,
		games:           make(chan protocol.ClientGame),
		gamesFinished:   false,
		reviews:         make(chan protocol.ClientReview),
		reviewsFinished: false,
		config:          config,
	}, nil
}

func (s *Server) Close() {
	s.serverSocket.Close()
}

func (s *Server) Run() {
	defer s.Close()

	client, err := s.acceptNewConnection()
	if err != nil {
		log.Errorf("action: accept_connections | result: fail | error: %s", err)
		return
	}

	go s.handleConnection(client)
	go s.handleGames()
	go s.handleReviews()

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
			game := protocol.ClientGame{}
			game.Decode(msg.Data)
			s.games <- game

		case protocol.MessageTypeReview:
			if !s.gamesFinished {
				log.Infof("action: receive_games | result: success")
				s.gamesFinished = true
				close(s.games)
			}
			review := protocol.ClientReview{}
			review.Decode(msg.Data)
			s.reviews <- review

		case protocol.MessageTypeAllSent:
			log.Infof("action: receive_reviews | result: success")
			log.Infof("action: receive_all_sent | result: success")
			s.reviewsFinished = true
			close(s.reviews)

		default:
			log.Errorf("action: handle_message | result: fail | error: mensaje no soportado %s", msg.MessageType)
			return
		}
	}
}

func (s *Server) handleGames() {
	for game := range s.games {
		for _, line := range game.Lines {
			reader := csv.NewReader(strings.NewReader(line))
			record, err := reader.Read()
			if err != nil {
				log.Errorf("Failed to read game error: %v", err)
				continue
			}
			gameMsg := middleware.GameMsg{Game: middleware.NewGame(record), Last: false}

			err = s.middleware.SendGameMsg(&gameMsg)
			if err != nil {
				log.Errorf("Failed to publish game message: %v", err)
			}
		}
	}

	err := s.middleware.SendGameFinished()
	if err != nil {
		log.Errorf("Failed to publish game finished message: %v", err)
	}

	log.Info("All games received and sent to middleware")
}

func (s *Server) handleReviews() {
	reviewBatch := make([]middleware.Review, 0)

	for review := range s.reviews {
		for _, line := range review.Lines {
			reader := csv.NewReader(strings.NewReader(line))
			reader.LazyQuotes = true
			reader.FieldsPerRecord = -1
			record, err := reader.Read()
			if err != nil {
				log.Errorf("Failed to read review error: %v", err)
				continue
			}
			reviewBatch = append(reviewBatch, *middleware.NewReview(record))

			// log.Debugf("REVIEW BATCH SIZE: %d, REVIEW: %s", len(reviewBatch), record[1])

			if len(reviewBatch) == s.config.Server.ReviewsBatchAmount {
				// log.Debugf("SENDING REVIEW BATCH: %d", len(reviewBatch))
				err := s.middleware.SendReviewBatch(&middleware.ReviewsBatch{Reviews: reviewBatch})
				if err != nil {
					log.Errorf("Failed to publish review message: %v", err)
				}
				reviewBatch = make([]middleware.Review, 0)
			}
		}
	}

	if len(reviewBatch) > 0 {
		err := s.middleware.SendReviewBatch(&middleware.ReviewsBatch{Reviews: reviewBatch})
		if err != nil {
			log.Errorf("Failed to publish review message: %v", err)
		}
	}

	err := s.middleware.SendReviewsFinished(1)
	if err != nil {
		log.Errorf("Failed to publish review message: %v", err)

	}

	log.Info("All reviews received and sent to middleware")
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
