package main

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"strings"
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

		writer := csv.NewWriter(m.gamesFile)

		gameStats := []string{
			strconv.Itoa(gameBatch.Game.AppId),
			gameBatch.Game.Name,
			strconv.Itoa(gameBatch.Game.Year),
			strings.Join(gameBatch.Game.Genres, ","),
		}

		if err := writer.Write(gameStats); err != nil {
			log.Errorf("Failed to write to games.csv: %v", err)
		}
		writer.Flush()

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

	file, err := os.Open("games.csv")
	if err != nil {
		log.Errorf("action: open file | result: fail")
		return
	}
	defer file.Close()

	err = m.reviewsQueue.Consume(func(reviewBatch *[]middleware.Review, ack func()) error {
		for _, review := range *reviewBatch {
			log.Infof("MAP REVIEWS: %s", review.Text)

			if _, err := file.Seek(0, 0); err != nil {
				log.Errorf("action: reset file reader | result: fail")
				return err
			}

			reader := csv.NewReader(file)
			for {
				record, err := reader.Read()
				if err == io.EOF {
					log.Errorf("action: crear_stats | result: fail")

					break
				}
				if err != nil {
					log.Errorf("action: crear_stats | result: fail")
					return err
				}
				if record[0] == review.AppId {
					stats := middleware.NewStats(record, &review)
					log.Infof("MAP STATS: %s", stats)
					err := m.middleware.SendStats(stats)
					if err != nil {
						log.Errorf("Failed to publish stats message: %v", err)
					}
					break
				}
			}
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
