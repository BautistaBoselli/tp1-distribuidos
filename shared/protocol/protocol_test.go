package protocol

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSendAndReceiveMessage(t *testing.T) {
	server, err := net.Listen("tcp", "localhost:8080")

	go func() {
		if err != nil {
			panic(err)
		}

		conn, err := server.Accept()
		if err != nil {
			panic(err)
		}

		// time.Sleep(200 * time.Millisecond)
		received, err := Receive(conn)
		if err != nil {
			println("Error receiving message: ", err.Error())
			panic(err)
		}

		assert.Equal(t, received.MessageType, MessageTypeGame)
		game := GameMessage{}
		game.Decode(received.Data)

		assert.Equal(t, len(game.Lines), 2)
		assert.Equal(t, game.Lines[0], "Hello, World!")
		assert.Equal(t, game.Lines[1], "This is a test")

	}()

	conn, err := net.Dial("tcp", "localhost:8080")

	if err != nil {
		panic(err)
	}

	msg := GameMessage{
		Lines: []string{"Hello, World!", "This is a test"},
	}

	err = Send(conn, &msg)
	if err != nil {
		panic(err)
	}
}
