package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"
	"tp1-distribuidos/config"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"
)

type Mapper struct {
	id                     int
	middleware             *middleware.Middleware
	clients                map[string]*MapperClient
	gamesQueue             *middleware.GamesQueue
	reviewsQueue           *middleware.ReviewsQueue
	FinishedClientsGames   *shared.FinishedClients
	FinishedClientsReviews *shared.FinishedClients
	cancelWg               *sync.WaitGroup
	cancelled              bool
}

func NewMapper(config *config.Config) (*Mapper, error) {
	middleware, err := middleware.NewMiddleware(config)
	if err != nil {
		return nil, err
	}

	gq, err := middleware.ListenGames("mapper"+strconv.Itoa(config.Mappers.Id), "*")
	if err != nil {
		return nil, err
	}

	rq, err := middleware.ListenReviews()
	if err != nil {
		return nil, err
	}

	return &Mapper{
		id:                     0,
		middleware:             middleware,
		clients:                make(map[string]*MapperClient),
		gamesQueue:             gq,
		reviewsQueue:           rq,
		FinishedClientsGames:   shared.NewFinishedClients("finished-mapper-games."+strconv.Itoa(config.Mappers.Id), middleware),
		FinishedClientsReviews: shared.NewFinishedClients("finished-mapper-reviews."+strconv.Itoa(config.Mappers.Id), middleware),
		cancelWg:               &sync.WaitGroup{},
	}, nil
}

func (m *Mapper) Close() error {
	m.cancelled = true
	m.middleware.Close()
	m.cancelWg.Done()
	return nil
}

func (m *Mapper) Run() {
	m.FinishedClientsGames.Consume()
	m.FinishedClientsReviews.Consume()
	time.Sleep(500 * time.Millisecond)

	m.cancelWg.Add(1)
	go m.consumeGameMessages()
	go m.consumeReviewsMessages()

	m.cancelWg.Wait()
	for _, client := range m.clients {
		client.Close()
	}

}

func (m *Mapper) consumeGameMessages() {
	log.Info("Starting to consume messages")

	metric := shared.NewMetric(10000, func(total int, elapsed time.Duration, rate float64) string {
		return fmt.Sprintf("Processed %d games in %s (%.2f games/s)", total, elapsed, rate)
	})

	err := m.gamesQueue.Consume(m.cancelWg, func(msg *middleware.GameMsg) error {
		m.FinishedClientsGames.Lock()
		defer m.FinishedClientsGames.Unlock()

		client, exists := m.clients[msg.ClientId]

		if m.FinishedClientsGames.Contains(msg.ClientId) {
			log.Infof("Ignoring game message from finished client %s", msg.ClientId)
			msg.Ack()
			if exists {
				client.ignoreAllGames()
				client.ignoreAllReviews()
			}
			return nil
		}
		metric.Update(1)

		if !exists {
			log.Infof("New client %s", msg.ClientId)
			client = NewMapperClient(msg.ClientId, m.middleware)
			m.clients[msg.ClientId] = client
		}

		if m.cancelled {
			return nil
		}

		client.games <- *msg
		return nil
	})
	if err != nil {
		log.Errorf("Failed to consume from games exchange: %v", err)
	}
}

func (m *Mapper) consumeReviewsMessages() {
	log.Info("Starting to consume reviews messages")

	metric := shared.NewMetric(10000, func(total int, elapsed time.Duration, rate float64) string {
		return fmt.Sprintf("[Mapper] Processed %d reviews in %s (%.2f reviews/s)", total, elapsed, rate)
	})

	err := m.reviewsQueue.Consume(m.cancelWg, func(msg *middleware.ReviewsMsg) error {
		m.FinishedClientsReviews.Lock()
		defer m.FinishedClientsReviews.Unlock()

		client, exists := m.clients[msg.ClientId]
		if m.FinishedClientsReviews.Contains(msg.ClientId) {
			msg.Ack()
			if exists {
				client.ignoreAllReviews()
				client.ignoreAllReviews()
			}
			return nil
		}
		metric.Update(len(msg.Reviews))
		if !exists {
			log.Infof("New client %s", msg.ClientId)
			client = NewMapperClient(msg.ClientId, m.middleware)
			m.clients[msg.ClientId] = client
		}

		if m.cancelled {
			log.Infof("Ignoring reviews message from cancelled client %s", msg.ClientId)
			return nil
		}

		client.reviews <- *msg
		return nil
	})

	if err != nil {
		log.Errorf("Failed to consume from reviews exchange: %v", err)
	}

	if m.cancelled {
		return
	}

	log.Info("Review messages consumed")
}
