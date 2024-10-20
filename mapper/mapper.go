package main

import (
	"encoding/csv"
	"io"
	"slices"
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
	// gamesFiles []*os.File
	gamesDirs  map[int]shared.Directory
	gamesQueue *middleware.GamesQueue
	// reviewsListener?
	reviewsQueue *middleware.ReviewsQueue
	cancelled    bool
	totalGames   int
}

func NewMapper(config *config.Config) (*Mapper, error) {
	// gamesFiles, err := shared.InitStoreFiles("store", 100)
	// if err != nil {
	// 	return nil, err
	// }

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
		id:         0,
		middleware: middleware,
		gamesDirs:  make(map[int]shared.Directory),
		// gamesFiles:   gamesFiles,
		gamesQueue:   gq,
		reviewsQueue: rq,
		totalGames:   0,
	}, nil
}

func (m *Mapper) Close() error {
	m.cancelled = true
	m.middleware.Close()
	for _, dir := range m.gamesDirs {
		dir.Delete()

		// for _, file := range dir {
		// 	if err := file.Close(); err != nil {
		// 		return err
		// 	}
		// }
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
		if gameBatch.Last {
			log.Infof("Received last game message")
			log.Infof("Total games processed: %d", m.totalGames)

			return nil
		}

		if _, exists := m.gamesDirs[gameBatch.ClientId]; !exists {
			id := strconv.Itoa(gameBatch.ClientId)
			dir, err := shared.InitStoreFiles(id, "store", 100)
			if err != nil {
				log.Errorf("Failed to init store files: %v", err)
				return err
			}
			m.gamesDirs[gameBatch.ClientId] = dir
		}

		total := 0
		for i, char := range strconv.Itoa(gameBatch.Game.AppId) {
			total += int(char) * i
		}
		hash := total % 100
		file := m.gamesDirs[gameBatch.ClientId].Files[hash]

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

		m.totalGames++

		ack()
		return nil

	})
	if err != nil {
		log.Errorf("Failed to consume from games exchange: %v", err)
		time.Sleep(5 * time.Second)
	}

	if m.cancelled {
		return
	}

	log.Info("Game messages consumed")

}

func (m *Mapper) consumeReviewsMessages() {
	log.Info("Starting to consume reviews messages")

	i := 0
	err := m.reviewsQueue.Consume(func(reviewBatch *middleware.ReviewsMsg, ack func()) error {
		go func() error {
			clientId := strconv.Itoa(reviewBatch.ClientId)
			i += len(reviewBatch.Reviews)
			if i%100000 == 0 {
				log.Infof("Processed %d reviews", i)
			}
			for _, review := range reviewBatch.Reviews {

				file, err := shared.GetStoreRWriter(shared.GetFilename(clientId, "store", review.AppId, 100))
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
						if slices.Contains(stats.Genres, "Action") || slices.Contains(stats.Genres, "Indie") {
							err := m.middleware.SendStats(&middleware.StatsMsg{Stats: stats})
							if err != nil {
								log.Errorf("Failed to publish stats message: %v", err)
							}
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

	if m.cancelled {
		return
	}

	log.Info("Review messages consumed")
}
