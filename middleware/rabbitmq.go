package middleware

import (
	"github.com/op/go-logging"
	"github.com/streadway/amqp"
)

var log = logging.MustGetLogger("log")

type Middleware struct {
	conn *amqp.Connection
}

func NewMiddleware(url string) (*Middleware, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	return &Middleware{conn: conn}, nil
}

func (m *Middleware) Publish(exchange string, key string, body []byte) error {
	ch, err := m.conn.Channel()
	if err != nil {
		log.Errorf("Failed to open a channel: %v", err)
		return err
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		exchange,
		"fanout",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Errorf("Failed to declare exchange: %v", err)
		return err
	}

	err = ch.Publish(
		exchange,
		"", // routing key is empty for fanout
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		},
	)
	if err != nil {
		log.Errorf("Failed to publish message: %v", err)
		return err
	}

	return nil
}

func (m *Middleware) Consume(exchange string, key string, callback func([]byte) error) error {
	ch, err := m.conn.Channel()
	if err != nil {
		log.Errorf("Failed to open a channel: %v", err)
		return err
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		exchange,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
		return err
	}

	err = ch.QueueBind(
		q.Name,   // queue name
		"",       // routing key
		exchange, // exchange
		false,
		nil,
	)
	if err != nil {
		log.Errorf("Failed to bind queue: %v", err)
		return err
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Errorf("Failed to register a consumer: %v", err)
		ch.Close()
	}

	for msg := range msgs {
		if err := callback(msg.Body); err != nil {
			log.Errorf("Error processing message: %v", err)
		}
	}

	return nil
}
