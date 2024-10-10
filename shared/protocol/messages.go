package protocol

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/op/go-logging"
)

type MessageType int32

const (
	MessageTypeGame MessageType = iota
	MessageTypeReview
	MessageTypeAllSent
	MessageTypeClientResponse1
	MessageTypeClientResponse2
	MessageTypeClientResponse3
	MessageTypeClientResponse4
)

// Protocolo de comunicacion entre cliente y servidor
// Interfaz para los mensajes que se intercambian
type Message interface {
	GetMessageType() MessageType
	Encode() string
	Decode(data string) error
}

type ClientGame struct {
	Lines []string
}

func (m *ClientGame) GetMessageType() MessageType {
	return MessageTypeGame
}

func (m *ClientGame) Encode() string {
	return strings.Join(m.Lines, "\n")
}

var log = logging.MustGetLogger("DEBUG")

func (m *ClientGame) Decode(data string) error {
	m.Lines = strings.Split(data, "\n")
	return nil
}

type ClientReview struct {
	Lines []string
}

func (m *ClientReview) GetMessageType() MessageType {
	return MessageTypeReview
}

func (m *ClientReview) Encode() string {
	return strings.Join(m.Lines, "\n")
}

func (m *ClientReview) Decode(data string) error {
	m.Lines = strings.Split(data, "\n")
	return nil
}

type AllSent struct{}

func (m *AllSent) GetMessageType() MessageType {
	return MessageTypeAllSent
}

func (m *AllSent) Encode() string {
	return ""
}

func (m *AllSent) Decode(data string) error {
	return nil
}

type ClientResponse1 struct {
	Windows int
	Mac     int
	Linux   int
	Last    bool
}

func (m *ClientResponse1) GetMessageType() MessageType {
	return MessageTypeClientResponse1
}
func (m *ClientResponse1) Encode() string {
	if m.Last {
		return fmt.Sprintf("%d,%d,%d,True", m.Windows, m.Mac, m.Linux)
	}
	return fmt.Sprintf("%d,%d,%d,False", m.Windows, m.Mac, m.Linux)

}

func (m *ClientResponse1) Decode(data string) error {
	parts := strings.Split(data, ",")
	windows, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return err
	}
	mac, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return err
	}
	linux, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return err
	}

	m.Windows = int(windows)
	m.Mac = int(mac)
	m.Linux = int(linux)
	m.Last = parts[3] == "True"
	return nil
}

type Game struct {
	Id    string
	Name  string
	Count int
}

func (m *Game) Encode() string {
	return fmt.Sprintf("%s,%s,%d", m.Id, m.Name, m.Count)
}

func (m *Game) Decode(data string) error {
	parts := strings.Split(data, ",")
	m.Id = parts[0]
	m.Name = parts[1]
	count, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return err
	}
	m.Count = int(count)
	return nil
}

type ClientResponse2 struct {
	TopGames []Game
}

func (m *ClientResponse2) GetMessageType() MessageType {
	return MessageTypeClientResponse2
}
func (m *ClientResponse2) Encode() string {
	games := []string{}
	for _, game := range m.TopGames {
		games = append(games, game.Encode())
	}
	return strings.Join(games, "\n")
}

func (m *ClientResponse2) Decode(data string) error {
	parts := strings.Split(data, "\n")
	for _, part := range parts {
		game := Game{}
		err := game.Decode(part)
		if err != nil {
			return err
		}
		m.TopGames = append(m.TopGames, game)
	}
	return nil
}

type ClientResponse3 struct {
	TopStats []Game
}

func (m *ClientResponse3) GetMessageType() MessageType {
	return MessageTypeClientResponse3
}
func (m *ClientResponse3) Encode() string {
	games := []string{}
	for _, game := range m.TopStats {
		games = append(games, game.Encode())
	}
	return strings.Join(games, "\n")
}

func (m *ClientResponse3) Decode(data string) error {
	parts := strings.Split(data, "\n")
	for _, part := range parts {
		game := Game{}
		err := game.Decode(part)
		if err != nil {
			return err
		}
		m.TopStats = append(m.TopStats, game)
	}
	return nil
}

type ClientResponse4 struct {
	Game Game
	Last bool
}

func (m *ClientResponse4) GetMessageType() MessageType {
	return MessageTypeClientResponse4
}
func (m *ClientResponse4) Encode() string {
	if m.Last {
		return fmt.Sprintf("%s,%s,%d,True", m.Game.Id, m.Game.Name, m.Game.Count)
	}
	return fmt.Sprintf("%s,%s,%d,False", m.Game.Id, m.Game.Name, m.Game.Count)
}

func (m *ClientResponse4) Decode(data string) error {
	parts := strings.Split(data, ",")
	m.Game.Id = parts[0]
	m.Game.Name = parts[1]
	count, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return err
	}
	m.Game.Count = int(count)
	m.Last = parts[3] == "True"
	return nil
}
