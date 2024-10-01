package protocol

import "strings"

type MessageType int32

const (
	MessageTypeGame MessageType = iota
	MessageTypeReview
)

// Protocolo de comunicacion entre cliente y servidor
// Interfaz para los mensajes que se intercambian
type Message interface {
	GetMessageType() MessageType
	Encode() string
	Decode(data string) error
}

type GameMessage struct {
	Lines []string
}

func (m GameMessage) GetMessageType() MessageType {
	return MessageTypeGame
}

func (m GameMessage) Encode() string {
	return strings.Join(m.Lines, "\n")
}

func (m *GameMessage) Decode(data string) error {
	m.Lines = strings.Split(data, "\n")
	return nil
}

type ReviewMessage struct {
	Lines []string
}

func (m ReviewMessage) GetMessageType() MessageType {
	return MessageTypeReview
}

func (m ReviewMessage) Encode() string {
	return strings.Join(m.Lines, "\n")
}

func (m *ReviewMessage) Decode(data string) error {
	m.Lines = strings.Split(data, "\n")
	return nil
}
