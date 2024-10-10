package main

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"strings"
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

const shardingAmount = 5

func NewMapper() (*Mapper, error) {
	gamesFile, err := os.Create("games.csv")
	if err != nil {
		return nil, err
	}

	middleware, err := middleware.NewMiddleware(shardingAmount)
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

	m.consumeGameMessages()
	m.consumeReviewsMessages()
}

func (m *Mapper) consumeGameMessages() {
	log.Info("Starting to consume messages")

	err := m.gamesQueue.Consume(func(gameBatch *middleware.GameMsg, ack func()) error {
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
	log.Info("Starting to consume reviews messages")

	i := 0
	err := m.reviewsQueue.Consume(func(reviewBatch *middleware.ReviewsBatch, ack func()) error {
		go func() error {
			file, err := os.Open("games.csv")
			if err != nil {
				log.Errorf("action: open file | result: fail")
				return err
			}
			defer file.Close()
			i += len(reviewBatch.Reviews)
			if i%1000 == 0 {
				log.Infof("Processed %d reviews", i)
			}
			for _, review := range reviewBatch.Reviews {
				if _, err := file.Seek(0, 0); err != nil {
					log.Errorf("action: reset file reader | result: fail")
					return err
				}

				reader := csv.NewReader(file)

				for {
					record, err := reader.Read()
					if err == io.EOF {
						// log.Errorf("action: crear_stats | result: fail | error: %v", err)
						break // siguiente review
					}
					if err != nil {
						log.Errorf("action: crear_stats | result: fail | error: %v", err)
						return err
					}
					if record[0] == review.AppId {
						stats := middleware.NewStats(record, &review)
						// log.Debugf("MAP STATS: %d", stats.AppId)

						err := m.middleware.SendStats(&middleware.StatsMsg{Stats: stats})
						if err != nil {
							log.Errorf("Failed to publish stats message: %v", err)
						}
						break
					}
				}
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
