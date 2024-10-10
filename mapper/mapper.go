package main

import (
	"encoding/csv"
	"fmt"
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
	gamesFiles []*os.File
	gamesFile  *os.File
	gamesQueue *middleware.GamesQueue
	// reviewsListener?
	reviewsQueue *middleware.ReviewsQueue
}

const shardingAmount = 5

func NewMapper() (*Mapper, error) {
	file, err := os.Create("store.csv")
	if err != nil {
		return nil, err
	}
	gamesFiles := make([]*os.File, 100)
	for i := range 100 {
		fileName := fmt.Sprintf("store_%d.csv", i)
		file, err := os.Create(fileName)
		if err != nil {
			return nil, err
		}
		gamesFiles[i] = file
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
				file, err := m.getStoreFile(review.AppId)
				if err != nil {
					log.Errorf("Failed to get store file: %v", err)
					return err
				}
				defer file.Close()

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

func (m *Mapper) getStoreFile(appId string) (*os.File, error) {
	total := 0
	for i, char := range appId {
		total += int(char) * i
	}
	hash := total % 100

	return os.Open(fmt.Sprintf("store_%d.csv", hash))
	// return os.Open("store.csv")
	// return m.gamesFiles[hash], nil
}
