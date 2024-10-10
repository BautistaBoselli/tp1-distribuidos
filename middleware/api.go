package middleware

import (
	"bytes"
	"encoding/gob"
	"strconv"
	"strings"

	"github.com/streadway/amqp"
)

func (m *Middleware) Declare() error {
	gob.Register(Query1Result{})
	gob.Register(Query2Result{})
	gob.Register(Query3Result{})
	gob.Register(Query4Result{})
	gob.Register(Query5Result{})

	if err := m.DeclareGamesExchange(); err != nil {
		return err
	}

	if err := m.DeclareReviewsQueue(); err != nil {
		return err
	}

	if err := m.DeclareStatsExchange(); err != nil {
		return err
	}

	if err := m.DeclareResultsExchange(); err != nil {
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
	log.Infof("Declared queue: %v", queue.Name)
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

func (m *Middleware) DeclareResultsExchange() error {
	err := m.channel.ExchangeDeclare(
		"results",
		"topic",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Errorf("Failed to declare results exchange: %v", err)
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
	totalInt := 0
	for _, char := range strconv.Itoa(message.Game.AppId) {
		int, _ := strconv.Atoi(string(char))
		totalInt += int
	}
	shardId := totalInt % m.Config.Sharding.Amount
	stringShardId := strconv.Itoa(shardId)

	return m.PublishExchange("games", stringShardId, message)
}

func (m Middleware) SendGameFinished() error {

	for shardId := range m.Config.Sharding.Amount {
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
	if last == m.Config.Mappers.Amount+1 {
		log.Infof("ALL SHARDS SENT STATS, SENDING STATS FINISHED")
		return m.SendStatsFinished()
	}
	log.Infof("Another mapper finished %d", last)
	return m.PublishQueue(m.reviewsQueue, &ReviewsBatch{Last: last})
}

type ReviewsQueue struct {
	queue      *amqp.Queue
	middleware *Middleware
	finished   bool
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
			log.Infof("Received Last message: %v", res.Last)
			if !rq.finished {
				rq.middleware.SendReviewsFinished(res.Last + 1)
				rq.finished = true
				msg.Ack(false)
				continue
			} else {
				log.Infof("Received Last again, ignoring and NACKing...")
				msg.Nack(false, true)
				continue
			}
		}

		callback(&res, func() {
			msg.Ack(false)
		})
	}

	return nil
}

func (m *Middleware) SendStats(message *StatsMsg) error {
	totalInt := 0
	for _, char := range strconv.Itoa(message.Stats.AppId) {
		int, _ := strconv.Atoi(string(char))
		totalInt += int
	}
	shardId := totalInt % m.Config.Sharding.Amount
	topic := strconv.Itoa(shardId) + "." + strings.Join(message.Stats.Genres, ".")

	return m.PublishExchange("stats", topic, message)
}

func (m *Middleware) SendStatsFinished() error {
	for shardId := range m.Config.Sharding.Amount {
		stringShardId := strconv.Itoa(shardId)
		topic := stringShardId + ".Indie.Action"
		log.Infof("Sending stats finished to shard %s", topic)
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

type ResultsQueue struct {
	queue      *amqp.Queue
	middleware *Middleware
}

func (m *Middleware) ListenResults(queryId string) (*ResultsQueue, error) {
	queue, err := m.BindExchange("results", queryId+".#")
	if err != nil {
		return nil, err
	}
	return &ResultsQueue{queue: queue, middleware: m}, nil
}

func (m *Middleware) SendResult(queryId string, result *Result) error {
	log.Infof("Sending result from query %s", queryId)
	return m.PublishExchange("results", queryId, result)
}

func (rq *ResultsQueue) Consume(callback func(message *Result, ack func()) error) error {
	msgs, err := rq.middleware.ConsumeQueue(rq.queue)
	if err != nil {
		return err
	}

	for msg := range msgs {
		var res Result

		decoder := gob.NewDecoder(bytes.NewReader(msg.Body))
		if err := decoder.Decode(&res); err != nil {
			log.Errorf("Failed to decode result message: %v", err)
			continue
		}

		err = callback(&res, func() {
			msg.Ack(false)
		})

		if err != nil {
			log.Errorf("Failed to process result message: %v", err)
		}
	}

	return nil
}
