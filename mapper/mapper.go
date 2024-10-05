package main

import (
	"os"
	"sync"
	"time"
	"tp1-distribuidos/middleware"
)

type Mapper struct {
	id         int
	middleware *middleware.Middleware
	statsFile  *os.File
	gamesQueue *middleware.GamesQueue
	// reviewsListener?
	reviewsQueue *middleware.ReviewsQueue
}

func NewMapper() (*Mapper, error) {
	statsFile, err := os.Create("stats.csv")
	if err != nil {
		return nil, err
	}

	middleware, err := middleware.NewMiddleware()
	if err != nil {
		return nil, err
	}

	gq, err := middleware.ListenGames()
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
		statsFile:    statsFile,
		gamesQueue:   gq,
		reviewsQueue: rq,
	}, nil
}

func (m *Mapper) Close() error {
	if err := m.statsFile.Close(); err != nil {
		return err
	}
	return nil
}

func (m *Mapper) Run() {
	defer m.Close()

	wg := sync.WaitGroup{}

	wg.Add(1)
	go m.consumeGameMessages(&wg)
	wg.Wait()

	m.consumeReviewsMessages()

	// send stats to stats pubsub

	select {}

}

func (m *Mapper) consumeGameMessages(wg *sync.WaitGroup) {
	log.Info("Starting to consume messages")

	err := m.gamesQueue.Consume(func(gameBatch *middleware.GameBatch, ack func()) error {
		for _, game := range gameBatch.Games {
			log.Infof("MAP GAME: %s", game.Name)
			// escribe en el archivo stats.csv
		}
		ack()
		if gameBatch.Last {
			wg.Done()
		}
		return nil
	})
	if err != nil {
		log.Errorf("Failed to consume from games exchange: %v", err)
		time.Sleep(5 * time.Second)
	}

	log.Info("Game messages consumed")

}

func (m *Mapper) consumeReviewsMessages() {
	log.Info("Starting to consume messages")

	err := m.reviewsQueue.Consume(func(reviewBatch *[]middleware.Review, ack func ()) error {
		for _, review := range *reviewBatch {
			log.Infof("MAP REVIEWS: %s", review.Text)
			// rellena el archivo stats.csv
		}
		ack()
		return nil
	})
	if err != nil {
		log.Errorf("Failed to consume from reviews exchange: %v", err)
		time.Sleep(5 * time.Second)
	}

	log.Info("Review messages consumed")

}
