package main

import (
	"encoding/csv"
	"errors"
	"net"
	"strconv"
	"strings"
	"tp1-distribuidos/config"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared/protocol"
)

type Server struct {
	serverSocket    *net.TCPListener
	middleware      *middleware.Middleware
	config          *config.Config
	clients         []*Client
	clientsReceived int
}

func NewServer(config *config.Config) (*Server, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", config.Server.Address)
	if err != nil {
		return nil, err
	}

	serverSocket, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return nil, err
	}

	middleware, err := middleware.NewMiddleware(config)
	if err != nil {
		return nil, err
	}

	return &Server{
		serverSocket:    serverSocket,
		middleware:      middleware,
		config:          config,
		clients:         make([]*Client, 0),
		clientsReceived: 1000,
	}, nil
}

func (s *Server) Close() {
	s.middleware.Close()
	s.serverSocket.Close()
}

func (s *Server) Run() {
	defer s.Close()

	go s.handleResponses()

	for {
		client, err := s.acceptNewConnection()
		if err != nil {
			log.Errorf("action: accept_connections | result: fail | error: %s", err)
			return
		}

		go client.handleConnection()
		go client.handleGames()
		go client.handleReviews()
	}

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

	s.clientsReceived++
	client := NewClient(strconv.Itoa(s.clientsReceived), clientSocket, s.middleware, s.config.Server.ReviewsBatchAmount)
	s.clients = append(s.clients, client)

	log.Infof("action: accept_connections | result: success | client_id: %d", client.id)

	return client, nil
}

func (s *Server) handleResponses() {
	responseQueue, err := s.middleware.ListenResponses()
	if err != nil {
		log.Errorf("Failed to listen responses: %v", err)
		return
	}

	err = responseQueue.Consume(func(response *middleware.Result) error {
		log.Infof("Response received from query %d and client %s", response.QueryId, response.ClientId)
		for _, client := range s.clients {
			if client.id == response.ClientId {
				client.handleResponse(response)
			}
		}
		return nil
	})
	if err != nil {
		log.Errorf("Failed to consume from responses exchange: %v", err)
	}
}

type Client struct {
	id                 string
	conn               *net.TCPConn
	middleware         *middleware.Middleware
	games              chan protocol.ClientGame
	gamesFinished      bool
	reviews            chan protocol.ClientReview
	reviewsFinished    bool
	reviewsBatchAmount int
	totalGames         int
	totalReviews       int
}

func NewClient(id string, conn *net.TCPConn, m *middleware.Middleware, reviewsBatchAmount int) *Client {
	log.Infof("action: new_client | result: success | id: %s", id)
	return &Client{
		id:                 id,
		conn:               conn,
		middleware:         m,
		games:              make(chan protocol.ClientGame),
		gamesFinished:      false,
		reviews:            make(chan protocol.ClientReview),
		reviewsFinished:    false,
		reviewsBatchAmount: reviewsBatchAmount,
		totalGames:         0,
		totalReviews:       0,
	}
}

func (c *Client) handleDisconnect(err error) {
	log.Infof("action: handle_disconnect | client: %s | EOF received", c.id)
	c.conn.Close()
}

func (c *Client) handleConnection() {
	for {
		msg, err := protocol.Receive(c.conn)
		if err != nil {
			c.handleDisconnect(err)
			return
		}

		switch msg.MessageType {

		case protocol.MessageTypeGame:
			game := protocol.ClientGame{}
			game.Decode(msg.Data)
			c.games <- game

		case protocol.MessageTypeReview:
			if !c.gamesFinished {
				log.Infof("action: receive_games | result: success | client_id: %s", c.id)
				c.gamesFinished = true
				close(c.games)
			}
			review := protocol.ClientReview{}
			review.Decode(msg.Data)
			c.reviews <- review

		case protocol.MessageTypeAllSent:
			log.Infof("action: receive_reviews | result: success")
			log.Infof("action: receive_all_sent | result: success")
			c.reviewsFinished = true
			close(c.reviews)

		default:
			log.Errorf("action: handle_message | result: fail | error: mensaje no soportado %s", msg.MessageType)
			return
		}
	}
}

func (c *Client) handleGames() {
	for game := range c.games {
		for _, line := range game.Lines {
			reader := csv.NewReader(strings.NewReader(line))
			record, err := reader.Read()
			if err != nil {
				log.Errorf("Failed to read game error: %v", err)
				continue
			}
			gameMsg := middleware.GameMsg{ClientId: c.id, Game: middleware.NewGame(record), Last: false}
			if gameMsg.Game == nil {
				continue
			}
			err = c.middleware.SendGameMsg(&gameMsg)
			c.totalGames++
			if err != nil {
				log.Errorf("Failed to publish game message: %v", err)
			}
		}
	}

	err := c.middleware.SendGameFinished(c.id)
	if err != nil {
		log.Errorf("Failed to publish game finished message: %v", err)
	}

	log.Infof("All %d games received and sent to middleware", c.totalGames)
}

func (c *Client) handleReviews() {
	reviewBatch := make([]middleware.Review, 0)

	for msg := range c.reviews {
		for _, line := range msg.Lines {
			reader := csv.NewReader(strings.NewReader(line))
			reader.LazyQuotes = true
			reader.FieldsPerRecord = -1
			record, err := reader.Read()
			if err != nil {
				reader.FieldsPerRecord = -1
				log.Errorf("Failed to read review error: %v", err)
				continue
			}
			review := middleware.NewReview(record, c.totalReviews)
			if review == nil {
				continue
			}
			reviewBatch = append(reviewBatch, *review)
			c.totalReviews++
			if len(reviewBatch) == c.reviewsBatchAmount {
				err := c.middleware.SendReviewBatch(&middleware.ReviewsMsg{ClientId: c.id, Reviews: reviewBatch})
				if err != nil {
					log.Errorf("Failed to publish review message: %v", err)
				}
				reviewBatch = make([]middleware.Review, 0)
			}
		}
	}

	if len(reviewBatch) > 0 {
		err := c.middleware.SendReviewBatch(&middleware.ReviewsMsg{ClientId: c.id, Reviews: reviewBatch})
		if err != nil {
			log.Errorf("Failed to publish review message: %v", err)
		}
	}

	err := c.middleware.SendReviewsFinished(c.id, 1)
	if err != nil {
		log.Errorf("Failed to publish review message: %v", err)

	}

	log.Infof("All %d reviews received and sent to middleware", c.totalReviews)
}

func (c *Client) handleResponse(response *middleware.Result) {
	log.Debugf("Received response from query %d", response.QueryId)
	switch response.QueryId {
	case 1:
		response1 := protocol.ClientResponse1{
			Windows: int(response.Payload.(middleware.Query1Result).Windows),
			Mac:     int(response.Payload.(middleware.Query1Result).Mac),
			Linux:   int(response.Payload.(middleware.Query1Result).Linux),
			Last:    response.IsFinalMessage,
		}
		protocol.Send(c.conn, &response1)
	case 2:
		topGames := []protocol.Game{}
		for _, game := range response.Payload.(middleware.Query2Result).TopGames {
			topGames = append(topGames, protocol.Game{Id: strconv.Itoa(game.AppId), Name: game.Name, Count: int(game.AvgPlaytime)})
		}
		response2 := protocol.ClientResponse2{
			TopGames: topGames,
		}
		protocol.Send(c.conn, &response2)
	case 3:
		topStats := []protocol.Game{}
		for _, stat := range response.Payload.(middleware.Query3Result).TopStats {
			topStats = append(topStats, protocol.Game{Id: strconv.Itoa(stat.AppId), Name: stat.Name, Count: int(stat.Positives)})
		}
		response3 := protocol.ClientResponse3{
			TopStats: topStats,
		}
		protocol.Send(c.conn, &response3)
	case 4:
		response4 := protocol.ClientResponse4{
			Game: protocol.Game{Name: response.Payload.(middleware.Query4Result).Game, Count: 0},
			Last: response.IsFinalMessage,
		}
		protocol.Send(c.conn, &response4)
	case 5:
		topStats := []protocol.Game{}
		for _, stat := range response.Payload.(middleware.Query5Result).Stats {
			topStats = append(topStats, protocol.Game{Id: strconv.Itoa(stat.AppId), Name: stat.Name, Count: int(stat.Negatives)})
		}
		response5 := protocol.ClientResponse5{
			Last:     response.IsFinalMessage,
			TopStats: topStats,
		}
		protocol.Send(c.conn, &response5)
	default:
		log.Errorf("Unknown query id: %d", response.QueryId)
	}

	response.Ack()
}
