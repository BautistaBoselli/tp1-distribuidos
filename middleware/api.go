package middleware

import (
	"bytes"
	"encoding/gob"
)

func (m *Middleware) Declare() error {
	if err := m.DeclareGamesExchange(); err != nil {
		return err
	}

	if err := m.DeclareReviewsExchange(); err != nil {
		return err
	}

	return nil
}

func (m *Middleware) DeclareGamesExchange() error {
	err := m.channel.ExchangeDeclare(
		"games",
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

	return nil
}

func (m *Middleware) DeclareReviewsExchange() error {
	err := m.channel.ExchangeDeclare(
		"reviews",
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

	return nil
}

func (m *Middleware) SendGameBatch(message *[]Game) error {
	return m.PublishExchange("games", "", message)
}

func (m *Middleware) ConsumeGameBatch(callback func(message *[]Game) error) error {
	msgs, err := m.ConsumeExchange("games", "")
	if err != nil {
		return err
	}

	for msg := range msgs {
		var res []Game

		decoder := gob.NewDecoder(bytes.NewReader(msg.Body))
		err := decoder.Decode(&res)
		if err != nil {
			log.Errorf("Failed to decode message: %v", err)
			continue
		}
		callback(&res)
	}

	return nil
}

func (m *Middleware) SendReviewBatch(message *[]Review) error {
	return m.PublishExchange("reviews", "", message)
}

func (m *Middleware) ConsumeReviewBatch(callback func(message *[]Review) error) error {
	msgs, err := m.ConsumeExchange("reviews", "")
	if err != nil {
		return err
	}

	for msg := range msgs {
		var res []Review

		decoder := gob.NewDecoder(bytes.NewReader(msg.Body))
		err := decoder.Decode(&res)
		if err != nil {
			log.Errorf("Failed to decode message: %v", err)
			continue
		}
		callback(&res)
	}

	return nil
}
