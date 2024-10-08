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

	if err := m.DeclareReviewsQueue(); err != nil {
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

func (m *Middleware) DeclareReviewsQueue() error {
	queue, err := m.channel.QueueDeclare(
		"reviews", // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)

	m.reviewsQueue = &queue

	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
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

func (m *Middleware) ListenGames(shardId string) (*GamesQueue, error) {
	queue, err := m.BindExchange("games", shardId)
	if err != nil {
		return nil, err
	}

	return &GamesQueue{queue: queue, middleware: m}, nil
}

func (m *Middleware) SendGameMsg(message *GameMsg) error {
	shardId := message.Game.AppId % m.shardingAmount
	stringShardId := strconv.Itoa(shardId)

	return m.PublishExchange("games", stringShardId, message)
}

func (m Middleware) SendGameFinished() error {

	for shardId := range m.shardingAmount {
		stringShardId := strconv.Itoa(shardId)
		err := m.PublishExchange("games", stringShardId, &GameMsg{Game: &Game{}, Last: true})
		if err != nil {
			log.Errorf("Failed to send game finished to shard %s: %v", stringShardId, err)
			return err
		}
	}

	return nil
}

type GamesQueue struct {
	queue      *amqp.Queue
	middleware *Middleware
}

func (gq *GamesQueue) Consume(callback func(message *GameMsg, ack func()) error) error {
	msgs, err := gq.middleware.ConsumeQueue(gq.queue)
	if err != nil {
		return err
	}

	for msg := range msgs {
		var res GameMsg

		decoder := gob.NewDecoder(bytes.NewReader(msg.Body))
		err := decoder.Decode(&res)
		if err != nil {
			log.Errorf("Failed to decode message: %v", err)
			continue
		}

		if res.Last {
			msg.Ack(false)
			log.Debugf("LAST GAME")
			break
		}

		callback(&res, func() {
			msg.Ack(false)
		})
	}

	return nil
}

func (m *Middleware) ListenReviews() (*ReviewsQueue, error) {
	return &ReviewsQueue{queue: m.reviewsQueue, middleware: m}, nil
}

func (m *Middleware) SendReviewBatch(message *ReviewsBatch) error {
	return m.PublishQueue(m.reviewsQueue, message)
}

func (m Middleware) SendReviewsFinished(last int) error {
	if last == m.shardingAmount+1 {
		return m.SendStatsFinished()
	}
	return m.PublishQueue(m.reviewsQueue, &ReviewsBatch{Last: last})
}

type ReviewsQueue struct {
	queue      *amqp.Queue
	middleware *Middleware
}

func (rq *ReviewsQueue) Consume(callback func(message *ReviewsBatch, ack func()) error) error {
	msgs, err := rq.middleware.ConsumeQueue(rq.queue)
	if err != nil {
		return err
	}

	for msg := range msgs {
		var res ReviewsBatch

		decoder := gob.NewDecoder(bytes.NewReader(msg.Body))
		err := decoder.Decode(&res)
		if err != nil {
			log.Errorf("Failed to decode message: %v", err)
			continue
		}

		if res.Last > 0 {
			rq.middleware.SendReviewsFinished(res.Last + 1)
			msg.Ack(false)
			break
		}

		callback(&res, func() {
			msg.Ack(false)
		})
	}

	return nil
}

func (m *Middleware) SendStats(message *StatsMsg) error {
	shardId := message.Stats.AppId % m.shardingAmount
	stringShardId := strconv.Itoa(shardId)
	topic := stringShardId + "." + strings.Join(message.Stats.Genres, ".")

	log.Infof("Sending stats to topic %s", topic)
	return m.PublishExchange("stats", topic, message)
}

func (m *Middleware) SendStatsFinished() error {
	for shardId := range m.shardingAmount {
		stringShardId := strconv.Itoa(shardId)
		topic := stringShardId + ".Indie.Action"
		err := m.PublishExchange("stats", topic, &StatsMsg{Stats: &Stats{}, Last: true})
		if err != nil {
			log.Errorf("Failed to send stats finished to shard %s: %v", topic, err)
			return err
		}
	}
	return nil
}

type StatsQueue struct {
	queue      *amqp.Queue
	middleware *Middleware
}

func (m *Middleware) ListenStats(shardId string, genre string) (*StatsQueue, error) {
	queue, err := m.BindExchange("stats", shardId+".#."+genre+".#")
	if err != nil {
		return nil, err
	}

	return &StatsQueue{queue: queue, middleware: m}, nil
}

func (sq *StatsQueue) Consume(callback func(message *StatsMsg, ack func()) error) error {
	msgs, err := sq.middleware.ConsumeQueue(sq.queue)
	if err != nil {
		return err
	}

	for msg := range msgs {
		var res StatsMsg

		decoder := gob.NewDecoder(bytes.NewReader(msg.Body))
		err := decoder.Decode(&res)
		if err != nil {
			log.Errorf("Failed to decode message: %v", err)
			continue
		}

		if res.Last {
			log.Infof("Last stats received, sending to reducer")
			msg.Ack(false)
			break
		}

		callback(&res, func() {
			msg.Ack(false)
		})
	}

	return nil
}
