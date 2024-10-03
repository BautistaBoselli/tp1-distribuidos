package protocol

import (
	"strings"
)

type MessageType int32

const (
	MessageTypeGame MessageType = iota
	MessageTypeReview
	MessageTypeAllSent
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
