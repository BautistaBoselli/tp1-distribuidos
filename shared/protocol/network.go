package protocol

import (
	"encoding/binary"
	"net"
)

func Send(conn net.Conn, m Message) error {
	data := m.Encode()
	size := int32(len(data))

	err := binary.Write(conn, binary.LittleEndian, size)
	if err != nil {
		return err
	}

	err = binary.Write(conn, binary.LittleEndian, m.GetMessageType())
	if err != nil {
		return err
	}

	wrote := 0
	for wrote < len(data) {
		n, err := conn.Write([]byte(data[wrote:]))
		if err != nil {
			return err
		}
		wrote += n
	}

	return nil
}

// ReceivedMessage is a struct that represents a received message
// to be decoded by the client or server use the MessageType to
// know the type of message and the decode the data with the
// corresponding struct
type ReceivedMessage struct {
	MessageType MessageType
	Size        int32
	Data        string
}

func Receive(conn net.Conn) (*ReceivedMessage, error) {
	size := int32(0)
	err := binary.Read(conn, binary.LittleEndian, &size)
	if err != nil {
		return nil, err
	}

	messageType := int32(0)
	err = binary.Read(conn, binary.LittleEndian, &messageType)
	if err != nil {
		return nil, err
	}

	read := int32(0)
	data := make([]byte, size)
	for read < size {
		n, err := conn.Read(data[read:])
		if err != nil {
			return nil, err
		}
		read += int32(n)
	}

	return &ReceivedMessage{
		MessageType: MessageType(messageType),
		Size:        size,
		Data:        string(data),
	}, nil
}
