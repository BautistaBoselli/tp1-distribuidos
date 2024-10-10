package main

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
	"tp1-distribuidos/config"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"
)

type Mapper struct {
	id         int
	middleware *middleware.Middleware
	gamesFiles []*os.File
	gamesFile  *os.File
	gamesQueue *middleware.GamesQueue
	// reviewsListener?
	reviewsQueue *middleware.ReviewsQueue
}

func NewMapper(config *config.Config) (*Mapper, error) {
	file, err := os.Create("store.csv")
	if err != nil {
		return nil, err
	}
	gamesFiles, err := shared.InitStoreFiles("store", 100)
	if err != nil {
		return nil, err
	}

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
		gamesFiles:   gamesFiles,
		gamesFile:    file,
		gamesQueue:   gq,
		reviewsQueue: rq,
	}, nil
}

func (m *Mapper) Close() error {
	for _, file := range m.gamesFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (m *Mapper) Run() {
	defer m.Close()

	m.consumeGameMessages()
	m.consumeReviewsMessages()
}

func (m *Mapper) consumeGameMessages() {
	log.Info("Starting to consume messages")

	err := m.gamesQueue.Consume(func(gameBatch *middleware.GameMsg, ack func()) error {
		total := 0
		for i, char := range strconv.Itoa(gameBatch.Game.AppId) {
			total += int(char) * i
		}
		hash := total % 100
		file := m.gamesFiles[hash]

		writer := csv.NewWriter(file)

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
	log.Info("Starting to consume reviews messages")

	i := 0
	err := m.reviewsQueue.Consume(func(reviewBatch *middleware.ReviewsBatch, ack func()) error {
		go func() error {
			i += len(reviewBatch.Reviews)
			if i%100000 == 0 {
				log.Infof("Processed %d reviews", i)
			}
			for _, review := range reviewBatch.Reviews {
				file, err := shared.GetStoreRWriter("store", review.AppId, 100)
				if err != nil {
					log.Errorf("Failed to get store file: %v", err)
					return err
				}

				if _, err := file.Seek(0, 0); err != nil {
					log.Errorf("action: reset file reader | result: fail")
					file.Close()
					return err
				}

				reader := csv.NewReader(file)

				for {
					record, err := reader.Read()
					if err == io.EOF {
						break // siguiente review
					}
					if err != nil {
						log.Errorf("action: crear_stats | result: fail | error: %v", err)
						file.Close()
						return err
					}
					if record[0] == review.AppId {
						stats := middleware.NewStats(record, &review)
						err := m.middleware.SendStats(&middleware.StatsMsg{Stats: stats})
						if err != nil {
							log.Errorf("Failed to publish stats message: %v", err)
						}
						break
					}
				}
				file.Close()
			}
			ack()
			return nil
		}()
		return nil
	})
	if err != nil {
		log.Errorf("Failed to consume from reviews exchange: %v", err)
		time.Sleep(5 * time.Second)
	}

	log.Info("Review messages consumed")

	select {}
}
