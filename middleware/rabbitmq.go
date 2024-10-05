package middleware

import (
	"bytes"
	"encoding/gob"

	"github.com/op/go-logging"
	"github.com/streadway/amqp"
)

var log = logging.MustGetLogger("log")

type Middleware struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func NewMiddleware() (*Middleware, error) {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	middleware := &Middleware{conn: conn, channel: channel}

	err = middleware.Declare()
	if err != nil {
		return nil, err
	}

	return middleware, nil
}

func (m *Middleware) Close() error {
	return m.conn.Close()
}

func (m *Middleware) PublishExchange(exchange string, key string, body interface{}) error {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	err := encoder.Encode(body)
	if err != nil {
		log.Errorf("Failed to encode message: %v", err)
		return err
	}

	err = m.channel.Publish(
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        buffer.Bytes(),
		},
	)

	if err != nil {
		log.Errorf("Failed to publish message: %v", err)
		return err
	}

	return nil
}

func (m *Middleware) PublishQueue(queue *amqp.Queue, body interface{}) error {

	err := m.PublishExchange("", queue.Name, body)
	if err != nil {
		log.Errorf("Failed to publish message: %v", err)
		return err
	}

	return nil
}

func (m *Middleware) ConsumeQueue(q *amqp.Queue) (<-chan amqp.Delivery, error) {
	msgs, err := m.channel.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Errorf("Failed to register a consumer: %v", err)
		return nil, err
	}

	return msgs, nil
}

func (m *Middleware) BindExchange(exchange string, key string) (*amqp.Queue, error) {
	q, err := m.channel.QueueDeclare(
		exchange+"_queue_bind", // name
		false,                  // durable
		false,                  // delete when unused
		true,                   // exclusive
		false,                  // no-wait
		nil,                    // arguments
	)
	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
		return nil, err
	}

	err = m.channel.QueueBind(
		q.Name,   // queue name
		key,      // routing key
		exchange, // exchange
		false,
		nil,
	)

	if err != nil {
		log.Errorf("Failed to bind queue: %v", err)
		return nil, err
	}

	return &q, nil
}
