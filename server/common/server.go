package common

import (
	"errors"
	"net"
	"strconv"

	"github.com/op/go-logging"
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

var log = logging.MustGetLogger("log")

type Server struct {
	serverSocket *net.TCPListener
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

	return &Server{serverSocket: serverSocket}, nil
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

	s.handleConnection(client)
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
	// for {
	// 	msg, err := protocol.Receive(client.conn)
	// 	if err != nil {
	// 		s.handleDisconnect(client, err)
	// 		return
	// 	}

	// 	switch msg.MessageType {

	// 	case protocol.MessageTypeBetBatch:
	// 		s.handleNewBets(client, msg)

	// 	case protocol.MessageTypeAllBetsSent:
	// 		log.Info("action: total_apuestas_recibidas | result: success")
	// 		return

	// 	default:
	// 		log.Errorf("action: handle_message | result: fail | error: mensaje no soportado %s", msg.MessageType)
	// 		return
	// 	}
	// }
}

type Client struct {
	conn *net.TCPConn
	id   int
}

func NewClient(conn *net.TCPConn, id string) *Client {
	idAsNumber, _ := strconv.Atoi(id)
	return &Client{conn: conn, id: idAsNumber}
}
