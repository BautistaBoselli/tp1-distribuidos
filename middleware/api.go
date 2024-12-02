package middleware

import (
	"bytes"
	"encoding/gob"
	"strconv"
	"strings"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (m *Middleware) declare() error {
	gob.Register(Query1Result{})
	gob.Register(Query2Result{})
	gob.Register(Query3Result{})
	gob.Register(Query4Result{})
	gob.Register(Query5Result{})

	if err := m.declareGamesExchange(); err != nil {
		return err
	}

	if err := m.declareReviewsQueue(); err != nil {
		return err
	}

	if err := m.declareStatsExchange(); err != nil {
		return err
	}

	if err := m.declareResultsExchange(); err != nil {
		return err
	}

	if err := m.DeclareResponsesQueue(); err != nil {
		return err
	}

	return nil
}

func (m *Middleware) declareGamesExchange() error {
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

func (m *Middleware) declareReviewsQueue() error {
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

func (m *Middleware) declareStatsExchange() error {
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

func (m *Middleware) declareResultsExchange() error {
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

func (m *Middleware) DeclareResponsesQueue() error {
	queue, err := m.channel.QueueDeclare(
		"responses", // name
		true,        // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	log.Infof("Declared queue: %v", queue.Name)
	m.responsesQueue = &queue

	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
		return err
	}

	return nil
}

func (m *Middleware) ListenGames(shardId string) (*GamesQueue, error) {
	queue, err := m.bindExchange("games", shardId)
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

	return m.publishExchange("games", stringShardId, message)
}

func (m Middleware) SendGameFinished(clientId string) error {

	for shardId := range m.Config.Sharding.Amount {
		stringShardId := strconv.Itoa(shardId)
		err := m.publishExchange("games", stringShardId, &GameMsg{ClientId: clientId, Game: &Game{}, Last: true})
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

func (gq *GamesQueue) Consume(wg *sync.WaitGroup, callback func(message *GameMsg) error) error {
	msgs, err := gq.middleware.consumeQueue(gq.queue)
	if err != nil {
		return err
	}

	wg.Add(1)
	for msg := range msgs {
		var res GameMsg

		decoder := gob.NewDecoder(bytes.NewReader(msg.Body))
		err := decoder.Decode(&res)
		if err != nil {
			log.Errorf("Failed to decode message: %v", err)
			continue
		}

		res.msg = msg

		callback(&res)
	}

	log.Info("HOLA 3 - Games queue finished")

	wg.Done()

	return nil
}

func (m *Middleware) ListenReviews() (*ReviewsQueue, error) {
	return &ReviewsQueue{queue: m.reviewsQueue, middleware: m}, nil
}

func (m *Middleware) SendReviewBatch(message *ReviewsMsg) error {
	return m.publishQueue(m.reviewsQueue, message)
}

func (m Middleware) SendReviewsFinished(clientId string, last int) error {
	// TODO: save on file
	m.clientsLastsDict[clientId] += last
	if m.clientsLastsDict[clientId] == m.Config.Mappers.Amount+1 {
		log.Infof("ALL SHARDS SENT STATS, SENDING STATS FINISHED FOR CLIENT %s", clientId)
		return m.SendStatsFinished(clientId)
	}
	log.Infof("Another mapper finished %d", last)
	return m.publishQueue(m.reviewsQueue, &ReviewsMsg{ClientId: clientId, Last: last})
}

type ReviewsQueue struct {
	queue      *amqp.Queue
	middleware *Middleware
}

func (rq *ReviewsQueue) Consume(wg *sync.WaitGroup, callback func(message *ReviewsMsg) error) error {
	msgs, err := rq.middleware.consumeQueue(rq.queue)
	if err != nil {
		return err
	}

	wg.Add(1)
	for msg := range msgs {
		var res ReviewsMsg

		decoder := gob.NewDecoder(bytes.NewReader(msg.Body))
		err := decoder.Decode(&res)
		if err != nil {
			log.Errorf("Failed to decode message: %v", err)
			continue
		}

		res.msg = msg

		callback(&res)
	}
	wg.Done()

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

	return m.publishExchange("stats", topic, message)
}

func (m *Middleware) SendStatsFinished(clientId string) error {
	for shardId := range m.Config.Sharding.Amount {
		stringShardId := strconv.Itoa(shardId)
		topic := stringShardId + ".Indie.Action"
		log.Infof("Sending stats finished to shard %s for client %s", topic, clientId)
		err := m.publishExchange("stats", topic, &StatsMsg{ClientId: clientId, Stats: &Stats{}, Last: true})
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
	queue, err := m.bindExchange("stats", shardId+".#."+genre+".#")
	if err != nil {
		return nil, err
	}

	return &StatsQueue{queue: queue, middleware: m}, nil
}

func (sq *StatsQueue) Consume(callback func(message *StatsMsg) error) error {
	msgs, err := sq.middleware.consumeQueue(sq.queue)
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

		res.msg = msg

		callback(&res)
	}

	return nil
}

type ResultsQueue struct {
	queue      *amqp.Queue
	middleware *Middleware
}

func (m *Middleware) ListenResults(queryId string) (*ResultsQueue, error) {
	queue, err := m.bindExchange("results", queryId+".#")
	if err != nil {
		return nil, err
	}
	return &ResultsQueue{queue: queue, middleware: m}, nil
}

func (m *Middleware) SendResult(queryId string, result *Result) error {
	log.Infof("Sending result from query %s", queryId)
	return m.publishExchange("results", queryId, result)
}

func (rq *ResultsQueue) Consume(callback func(message *Result) error) error {
	msgs, err := rq.middleware.consumeQueue(rq.queue)
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

		res.msg = msg

		err = callback(&res)

		if err != nil {
			log.Errorf("Failed to process result message: %v", err)
		}

	}
	return nil
}

type ResponsesQueue struct {
	queue      *amqp.Queue
	middleware *Middleware
}

func (m *Middleware) ListenResponses() (*ResponsesQueue, error) {
	return &ResponsesQueue{queue: m.responsesQueue, middleware: m}, nil
}

func (m *Middleware) SendResponse(response *Result) error {
	return m.publishQueue(m.responsesQueue, response)
}

func (rq *ResponsesQueue) Consume(callback func(message *Result) error) error {
	msgs, err := rq.middleware.consumeQueue(rq.queue)
	if err != nil {
		return err
	}

	for msg := range msgs {
		var res Result

		decoder := gob.NewDecoder(bytes.NewReader(msg.Body))
		err := decoder.Decode(&res)
		if err != nil {
			log.Errorf("Failed to decode message: %v", err)
			continue
		}

		res.msg = msg

		callback(&res)
	}

	return nil
}
