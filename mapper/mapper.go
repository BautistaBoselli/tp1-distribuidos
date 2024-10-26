package main

import (
	"time"
	"tp1-distribuidos/config"
	"tp1-distribuidos/middleware"
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
	defer m.Close()

	go m.consumeGameMessages()
	m.consumeReviewsMessages()
}

func (m *Mapper) consumeGameMessages() {
	log.Info("Starting to consume messages")

	err := m.gamesQueue.Consume(func(msg *middleware.GameMsg) error {
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

	if m.cancelled {
		return
	}

	log.Info("Game messages consumed")

}

func (m *Mapper) consumeReviewsMessages() {
	log.Info("Starting to consume reviews messages")

	err := m.reviewsQueue.Consume(func(msg *middleware.ReviewsMsg) error {

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
