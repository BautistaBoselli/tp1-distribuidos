package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
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
	finished   bool
}

func NewMapperClient(id string, m *middleware.Middleware) *MapperClient {
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

	pendingLast := c.middleware.Config.Sharding.Amount
	for game := range c.games {

		if game.Last {
			pendingLast--
			if pendingLast == 0 {
				go c.consumeReviews()
				close(c.games)
			}
			game.Ack()
			continue
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

	for reviewBatch := range c.reviews {

		if reviewBatch.Last > 0 {
			c.handleFinsished(reviewBatch)
			continue
		}

		for _, review := range reviewBatch.Reviews {

			file, err := os.Open(fmt.Sprintf("database/%s/%s.csv", c.id, review.AppId))
			if err != nil {
				continue // no existe el juego
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
	}
}

func (c *MapperClient) handleFinsished(reviewBatch middleware.ReviewsMsg) {
	log.Infof("Received Last message for client %s: %v", reviewBatch.ClientId, reviewBatch.Last)
	if c.finished {
		log.Infof("Received Last again, ignoring and NACKing...")
		reviewBatch.Nack()
		return
	}
	c.middleware.SendReviewsFinished(reviewBatch.ClientId, reviewBatch.Last+1)
	c.finished = true
	reviewBatch.Ack()
	os.RemoveAll(fmt.Sprintf("database/%s", c.id))
}
