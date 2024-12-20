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

	if err := m.declareReviewsProcessedQueue(); err != nil {
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

	if err := m.declareClientsFinishedExchange(); err != nil {
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
	m.reviewsQueue = &queue

	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
		return err
	}

	return nil
}

func (m *Middleware) declareReviewsProcessedQueue() error {
	queue, err := m.channel.QueueDeclare(
		"reviewsProcessed", // name
		true,               // durable
		false,              // delete when unused
		false,              // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	m.reviewsProcessedQueue = &queue

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
	m.responsesQueue = &queue

	if err != nil {
		log.Errorf("Failed to declare queue: %v", err)
		return err
	}

	return nil
}

func (m *Middleware) declareClientsFinishedExchange() error {
	err := m.channel.ExchangeDeclare(
		"clientsFinished",
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

func (m *Middleware) ListenGames(name string, shardId string) (*GamesQueue, error) {
	queue, err := m.bindExchange(name, "games", shardId)
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

	message.ShardId = shardId
	return m.publishExchange("games", stringShardId, message)
}

func (m Middleware) SendGameFinished(clientId string) error {

	for shardId := range m.Config.Sharding.Amount {
		stringShardId := strconv.Itoa(shardId)
		err := m.publishExchange("games", stringShardId, &GameMsg{ClientId: clientId, Game: &Game{}, Last: true, ShardId: shardId})
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

type ReviewsQueue struct {
	queue      *amqp.Queue
	middleware *Middleware
}

func (m *Middleware) ListenReviews() (*ReviewsQueue, error) {
	return &ReviewsQueue{queue: m.reviewsQueue, middleware: m}, nil
}

func (m *Middleware) SendReviewBatch(message *ReviewsMsg) error {
	return m.publishQueue(m.reviewsQueue, message)
}

func (m Middleware) SendReviewsProcessed(message *ReviewsProcessedMsg) error {
	return m.publishQueue(m.reviewsProcessedQueue, message)
}

func (m Middleware) SendReviewsFinished(clientId string, last int) error {
	if last == m.Config.Mappers.Amount+1 {
		log.Infof("ALL MAPPERS FINISHED FOR CLIENT %s", clientId)
		return nil
	}
	return m.publishQueue(m.reviewsQueue, &ReviewsMsg{ClientId: clientId, Last: last})
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

type ReviewsProcessedQueue struct {
	queue      *amqp.Queue
	middleware *Middleware
}

func (m *Middleware) ListenReviewsProcessed() (*ReviewsProcessedQueue, error) {
	return &ReviewsProcessedQueue{queue: m.reviewsProcessedQueue, middleware: m}, nil
}

func (rpq *ReviewsProcessedQueue) Consume(wg *sync.WaitGroup, callback func(message *ReviewsProcessedMsg) error) error {
	msgs, err := rpq.middleware.consumeQueue(rpq.queue)
	if err != nil {
		return err
	}

	for msg := range msgs {
		var res ReviewsProcessedMsg

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

func (m *Middleware) ListenStats(name string, shardId string, genre string) (*StatsQueue, error) {
	queue, err := m.bindExchange(name, "stats", shardId+".#."+genre+".#")
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
	queue, err := m.bindExchange(queryId, "results", queryId+".#")
	if err != nil {
		return nil, err
	}
	return &ResultsQueue{queue: queue, middleware: m}, nil
}

func (m *Middleware) SendResult(queryId string, result *Result) error {
	log.Infof("Sending result from query %s for client %d with id: %v", queryId, result.ClientId, result.Id)
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
	if response.IsFinalMessage {
		log.Infof("Sending final response for client %d with id: %v", response.ClientId, response.Id)
	}
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

type ClientsFinishedQueue struct {
	queue      *amqp.Queue
	middleware *Middleware
}

func (m *Middleware) ListenClientsFinished(name string) (*ClientsFinishedQueue, error) {
	queue, err := m.bindExchange(name, "clientsFinished", "*")
	if err != nil {
		return nil, err
	}

	return &ClientsFinishedQueue{queue: queue, middleware: m}, nil
}

func (cfq *ClientsFinishedQueue) Consume(callback func(message *ClientsFinishedMsg) error) error {
	msgs, err := cfq.middleware.consumeQueue(cfq.queue)
	if err != nil {
		return err
	}

	for msg := range msgs {
		var res ClientsFinishedMsg

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

func (m *Middleware) SendClientsFinished(clientId int) error {
	return m.publishExchange("clientsFinished", "*", &ClientsFinishedMsg{ClientId: clientId})
}
