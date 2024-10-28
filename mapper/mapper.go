package main

import (
	"fmt"
	"time"
	"tp1-distribuidos/config"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"
)

type Mapper struct {
	id           int
	middleware   *middleware.Middleware
	clients      map[string]*MapperClient
	gamesQueue   *middleware.GamesQueue
	reviewsQueue *middleware.ReviewsQueue
	cancelled    bool
}

func NewMapper(config *config.Config) (*Mapper, error) {
	middleware, err := middleware.NewMiddleware(config)
	if err != nil {
		return nil, err
	}

	gq, err := middleware.ListenGames("*")
	if err != nil {
		return nil, err
	}

	rq, err := middleware.ListenReviews()
	if err != nil {
		return nil, err
	}

	return &Mapper{
		id:           0,
		middleware:   middleware,
		clients:      make(map[string]*MapperClient),
		gamesQueue:   gq,
		reviewsQueue: rq,
	}, nil
}

func (m *Mapper) Close() error {
	m.cancelled = true
	m.middleware.Close()
	for _, client := range m.clients {
		client.Close()
	}
	return nil
}

func (m *Mapper) Run() {
	//defer m.Close() esto causaba el panic

	go m.consumeGameMessages()
	m.consumeReviewsMessages()
}

func (m *Mapper) consumeGameMessages() {
	log.Info("Starting to consume messages")

	metric := shared.NewMetric(10000, func(total int, elapsed time.Duration, rate float64) string {
		return fmt.Sprintf("Processed %d games in %s (%.2f games/s)", total, elapsed, rate)
	})

	err := m.gamesQueue.Consume(func(msg *middleware.GameMsg) error {
		metric.Update(1)

		if _, exists := m.clients[msg.ClientId]; !exists {
			log.Infof("New client %s", msg.ClientId)
			m.clients[msg.ClientId] = NewMapperClient(msg.ClientId, m.middleware)
		}

		client := m.clients[msg.ClientId]
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
		return fmt.Sprintf("[Mapper %d] Processed %d reviews in %s (%.2f reviews/s)", m.id, total, elapsed, rate)
	})

	err := m.reviewsQueue.Consume(func(msg *middleware.ReviewsMsg) error {
		metric.Update(len(msg.Reviews))

		// log.Infof("consumeRev: client id %s", msg.ClientId)
		client := m.clients[msg.ClientId]
		client.reviews <- *msg
		return nil
	})

	if err != nil {
		log.Errorf("Failed to consume from reviews exchange: %v", err)
		time.Sleep(5 * time.Second)
	}

	if m.cancelled {
		return
	}

	log.Info("Review messages consumed")
}
