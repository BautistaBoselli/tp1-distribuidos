package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"
)

type MapperClient struct {
	id            string
	middleware    *middleware.Middleware
	games         chan middleware.GameMsg
	reviews       chan middleware.ReviewsMsg
	finishedGames *shared.Processed
	finishedSteps *shared.Processed
	cancelWg      *sync.WaitGroup
}

const GEOMETRY_DASH_APP_ID = "322170"

type finishedSteps int

const (
	FINISHED_FILE finishedSteps = iota
	FINISHED
)

func NewMapperClient(id string, m *middleware.Middleware) *MapperClient {
	os.MkdirAll(fmt.Sprintf("database/%s", id), 0755)

	client := &MapperClient{
		id:            id,
		middleware:    m,
		games:         make(chan middleware.GameMsg),
		reviews:       make(chan middleware.ReviewsMsg),
		finishedGames: shared.NewProcessed(fmt.Sprintf("database/%s/processed_games.bin", id)),
		finishedSteps: shared.NewProcessed(fmt.Sprintf("database/%s/processed_steps.bin", id)),
		cancelWg:      &sync.WaitGroup{},
	}

	go client.consumeGames()
	if client.finishedGames.Count() == m.Config.Sharding.Amount {
		log.Infof("client %s After reviving, consuming reviews", client.id)
		go client.consumeReviews()
	}

	client.cancelWg.Add(1)
	return client
}

func (c *MapperClient) Close() {
	if c.finishedSteps.Contains(int64(FINISHED)) {
		return
	}
	c.finishedSteps.Add(int64(FINISHED))
	c.cancelWg.Done()
	if c.finishedGames.Count() != c.middleware.Config.Sharding.Amount {
		close(c.games)
	}
	close(c.reviews)
	c.cancelWg.Wait()
	log.Infof("action: mapper_client_close | result: success | client_id: %s", c.id)
}

func (c *MapperClient) consumeGames() {

	c.cancelWg.Add(1)
	for game := range c.games {
		log.Infof("Game: %d", game.Game.AppId)

		if c.finishedGames.Count() == c.middleware.Config.Sharding.Amount {
			game.Ack()
			continue
		}

		if game.Last {
			shared.TestTolerance(1, 4, "Exiting at last game before adding")
			c.finishedGames.Add(int64(game.ShardId))
			shared.TestTolerance(1, 4, "Exiting at last game after adding")
			if c.finishedGames.Count() == c.middleware.Config.Sharding.Amount {
				go c.consumeReviews()
			}
			game.Ack()
			continue
		}

		if !slices.Contains(game.Game.Genres, "Action") && !slices.Contains(game.Game.Genres, "Indie") {
			game.Ack()
			continue
		}

		file, err := os.OpenFile(fmt.Sprintf("database/%s/%d.csv", c.id, game.Game.AppId), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)
		if err != nil {
			log.Errorf("Failed to open games.csv: %v", err)
			return
		}

		writer := csv.NewWriter(file)

		shared.TestTolerance(1, 5000, "Exiting at game before writing")

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

		shared.TestTolerance(1, 5000, "Exiting at game after writing")

		file.Close()

		game.Ack()
	}
	log.Infof("Mapper client %s finished consuming games", c.id)
	c.cancelWg.Done()
}

func (c *MapperClient) consumeReviews() {
	log.Infof("Starting to consume reviews")

	c.cancelWg.Add(1)
	for reviewBatch := range c.reviews {

		if reviewBatch.Last > 0 {
			c.handleFinsished(reviewBatch)
			continue
		}

		for _, review := range reviewBatch.Reviews {
			log.Infof("Review: %d", review.Id)

			file, err := os.Open(fmt.Sprintf("database/%s/%s.csv", c.id, review.AppId))
			if err != nil {
				continue // no existe el juego
			}

			if review.AppId == GEOMETRY_DASH_APP_ID {
				shared.TestTolerance(1, 13000, "Exiting at review before reading")
			}

			reader := csv.NewReader(file)
			record, _ := reader.Read()

			stats := middleware.NewStats(record, &review)

			if slices.Contains(stats.Genres, "Action") || slices.Contains(stats.Genres, "Indie") {
				err := c.middleware.SendStats(&middleware.StatsMsg{ClientId: c.id, Stats: stats})
				if err != nil {
					log.Errorf("Failed to publish stats message: %v", err)
				}
			}

			if review.AppId == GEOMETRY_DASH_APP_ID {
				shared.TestTolerance(1, 13000, "Exiting at review after reading")
			}

			file.Close()

		}

		c.middleware.SendReviewsProcessed(&middleware.ReviewsProcessedMsg{ClientId: c.id, BatchId: reviewBatch.Id})
		reviewBatch.Ack()

	}
	log.Infof("Mapper client %s finished consuming reviews", c.id)
	c.cancelWg.Done()
}

func (c *MapperClient) ignoreAllGames() {
	log.Infof("Ignoring all games for client %s", c.id)

	// Drain and close games channel
	for {
		select {
		case msg, ok := <-c.games:
			if !ok {
				return
			}
			msg.Ack()
		default:
			close(c.games)
			return
		}
	}
}

func (c *MapperClient) ignoreAllReviews() {
	log.Infof("Ignoring all reviews for client %s", c.id)

	// Drain and close reviews channel
	for {
		select {
		case msg, ok := <-c.reviews:
			if !ok {
				return
			}
			msg.Ack()
		default:
			close(c.reviews)
			return
		}
	}
}

func (c *MapperClient) handleFinsished(reviewBatch middleware.ReviewsMsg) {
	log.Debugf("Received Last message for client %s: %v", reviewBatch.ClientId, reviewBatch.Last)
	if c.finishedSteps.Contains(int64(FINISHED)) {
		log.Debugf("Received Last again, ignoring and NACKing...")
		go func() {
			time.Sleep(500 * time.Millisecond)
			c.middleware.SendReviewsFinished(reviewBatch.ClientId, reviewBatch.Last)
			shared.TestTolerance(1, 8, "Exiting at review processed after sending finished")
			reviewBatch.Ack()
		}()
		return
	}

	c.middleware.SendReviewsFinished(reviewBatch.ClientId, reviewBatch.Last+1)
	shared.TestTolerance(1, 8, "Exiting at review processed after sending finished")
	c.finishedSteps.Add(int64(FINISHED))
	reviewBatch.Ack()
	os.RemoveAll(fmt.Sprintf("database/%s", c.id))
}
