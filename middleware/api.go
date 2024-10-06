package middleware

import (
	"bytes"
	"encoding/gob"
	"strconv"
	"strings"

	"github.com/streadway/amqp"
)

func (m *Middleware) Declare() error {
	if err := m.DeclareGamesExchange(); err != nil {
		return err
	}

	if err := m.DeclareReviewsExchange(); err != nil {
		return err
	}

	if err := m.DeclareStatsExchange(); err != nil {
		return err
	}

	return nil
}

func (m *Middleware) DeclareGamesExchange() error {
	err := m.channel.ExchangeDeclare(
		"games",
		"topic",
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

func (m *Middleware) DeclareStatsExchange() error {
	err := m.channel.ExchangeDeclare(
		"stats",
		"topic",
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

type GamesQueue struct {
	queue      *amqp.Queue
	middleware *Middleware
}

func (m *Middleware) ListenGames(shardId string) (*GamesQueue, error) {
	queue, err := m.BindExchange("games", shardId)
	if err != nil {
		return nil, err
	}

	return &GamesQueue{queue: queue, middleware: m}, nil
}

func (m *Middleware) SendGameBatch(message *GameBatch) error {
	shardId := message.Game.AppId % 2
	stringShardId := strconv.Itoa(shardId)

	return m.PublishExchange("games", stringShardId, message)
}

func (gq *GamesQueue) Consume(callback func(message *GameBatch, ack func()) error) error {
	msgs, err := gq.middleware.ConsumeQueue(gq.queue)
	if err != nil {
		return err
	}

	for msg := range msgs {
		var res GameBatch

		decoder := gob.NewDecoder(bytes.NewReader(msg.Body))
		err := decoder.Decode(&res)
		if err != nil {
			log.Errorf("Failed to decode message: %v", err)
			continue
		}
		callback(&res, func() {
			msg.Ack(false)
		})
	}

	return nil
}

type ReviewsQueue struct {
	queue      *amqp.Queue
	middleware *Middleware
}

func (m *Middleware) ListenReviews() (*ReviewsQueue, error) {
	queue, err := m.BindExchange("reviews", "")
	if err != nil {
		log.Errorf("Failed to bind exchange: %v", err)
		return nil, err
	}

	return &ReviewsQueue{queue: queue, middleware: m}, nil
}

func (m *Middleware) SendReviewBatch(message *[]Review) error {
	return m.PublishExchange("reviews", "", message)
}

func (rq *ReviewsQueue) Consume(callback func(message *[]Review, ack func()) error) error {
	msgs, err := rq.middleware.ConsumeQueue(rq.queue)
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
		callback(&res, func() {
			msg.Ack(false)
		})
	}

	return nil
}

func (m *Middleware) SendStats(message *Stats) error {
	shardId := message.AppId % 2
	stringShardId := strconv.Itoa(shardId)
	topic := stringShardId + "." + strings.Join(message.Genres, ".")

	log.Infof("Sending stats to topic %s", topic)
	return m.PublishExchange("stats", stringShardId, message)

}
