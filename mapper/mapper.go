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
	gamesFile  *os.File
	gamesQueue *middleware.GamesQueue
	// reviewsListener?
	reviewsQueue *middleware.ReviewsQueue
}

func NewMapper() (*Mapper, error) {
	gamesFile, err := os.Create("games.csv")
	if err != nil {
		return nil, err
	}

	middleware, err := middleware.NewMiddleware()
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
		gamesFile:    gamesFile,
		gamesQueue:   gq,
		reviewsQueue: rq,
	}, nil
}

func (m *Mapper) Close() error {
	if err := m.gamesFile.Close(); err != nil {
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
		if gameBatch.Last {
			wg.Done()
			ack()
			return nil
		}

		log.Infof("MAP GAME: %s", gameBatch.Game.Name)

		// escribe en el archivo games.csv

		// log.Infof("GAME STATS: %s", gameStats)
		ack()
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

	err := m.reviewsQueue.Consume(func(reviewBatch *[]middleware.Review, ack func()) error {
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
