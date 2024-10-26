package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"
)

type MapperClient struct {
	id         string
	middleware *middleware.Middleware
	games      chan middleware.GameMsg
	gamesDir   shared.Directory
	reviews    chan middleware.ReviewsMsg
	totalGames int
}

func NewMapperClient(id string, m *middleware.Middleware) *MapperClient {
	// dir, err := shared.InitStoreFiles(id, "store", 100)
	// if err != nil {
	// 	log.Errorf("Failed to init store files: %v", err)
	// }

	os.MkdirAll(fmt.Sprintf("database/%s", id), 0644)

	client := &MapperClient{
		id:         id,
		middleware: m,
		games:      make(chan middleware.GameMsg),
		// gamesDir:   dir,
		reviews: make(chan middleware.ReviewsMsg),
	}

	go client.consumeGames()
	return client
}

func (c *MapperClient) Close() {
	close(c.games)
	close(c.reviews)
	c.gamesDir.Delete()
}

func (c *MapperClient) consumeGames() {
	i := 0
	lastTimestamp := time.Now()
	lastI := 0

	for game := range c.games {
		i++
		if i%10000 == 0 {
			elapsed := time.Since(lastTimestamp)
			log.Infof("Processed %d games for client %s in %s (%.2f games/s)", i, c.id, elapsed, float64(i-lastI)/elapsed.Seconds())
			lastTimestamp = time.Now()
			lastI = i
		}
		if game.Last {
			go c.consumeReviews()
			log.Infof("last game for client %s", c.id)
			log.Infof("total games for client %s: %d", c.id, c.totalGames)
			return
		}

		file, err := os.OpenFile(fmt.Sprintf("database/%s/%d.csv", c.id, game.Game.AppId), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Errorf("Failed to open games.csv: %v", err)
			return
		}

		writer := csv.NewWriter(file)

		gameStats := []string{
			strconv.Itoa(game.Game.AppId),
			game.Game.Name,
			strconv.Itoa(game.Game.Year),
			strings.Join(game.Game.Genres, ","),
		}

		if err := writer.Write(gameStats); err != nil {
			log.Errorf("Failed to write to games.csv: %v", err)
		}
		writer.Flush()

		c.totalGames++

		file.Close()

		game.Ack()
	}
}

func (c *MapperClient) consumeReviews() {
	log.Infof("Starting to consume reviews")
	i := 0
	lastTimestamp := time.Now()
	lastI := 0
	for reviewBatch := range c.reviews {
		// go func() {
		i += len(reviewBatch.Reviews)
		if i%10000 == 0 {
			elapsed := time.Since(lastTimestamp)
			log.Infof("Processed %d reviews for client %s in %s (%.2f reviews/s)", i, c.id, elapsed, float64(i-lastI)/elapsed.Seconds())
			lastTimestamp = time.Now()
			lastI = i
		}
		for _, review := range reviewBatch.Reviews {

			// file, err := shared.GetStoreRWriter(shared.GetFilename(reviewBatch.ClientId, "store", review.AppId, 100))
			file, err := os.Open(fmt.Sprintf("database/%s/%s.csv", c.id, review.AppId))
			if err != nil {
				// log.Errorf("Failed to open reviews file: %v", err)
				continue
			}

			// if _, err := file.Seek(0, 0); err != nil {
			// 	log.Errorf("action: reset file reader | result: fail")
			// 	file.Close()
			// 	continue
			// }

			reader := csv.NewReader(file)

			for {
				record, err := reader.Read()
				if err == io.EOF {
					break // siguiente review
				}
				if err != nil {
					log.Errorf("action: crear_stats | result: fail | error: %v", err)
					file.Close()
					return
				}
				if record[0] == review.AppId {
					stats := middleware.NewStats(record, &review)
					if slices.Contains(stats.Genres, "Action") || slices.Contains(stats.Genres, "Indie") {
						err := c.middleware.SendStats(&middleware.StatsMsg{ClientId: c.id, Stats: stats})
						if err != nil {
							log.Errorf("Failed to publish stats message: %v", err)
						}
					}
					break
				}
			}
			file.Close()
		}

		reviewBatch.Ack()
		// }()
	}
}
