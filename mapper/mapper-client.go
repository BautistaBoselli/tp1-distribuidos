package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"tp1-distribuidos/middleware"
)

type MapperClient struct {
	id                string
	middleware        *middleware.Middleware
	games             chan middleware.GameMsg
	reviews           chan middleware.ReviewsMsg
	reviewsFile       *os.File
	reviewsFileWriter *csv.Writer
	finishedFile      bool
	finished          bool
	finishedGames     bool
	cancelWg          *sync.WaitGroup
}

func NewMapperClient(id string, m *middleware.Middleware) *MapperClient {
	os.MkdirAll(fmt.Sprintf("database/%s", id), 0755)

	reviewsFile, err := os.OpenFile(fmt.Sprintf("database/%s/reviews.csv", id), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		log.Errorf("Failed to open reviews.csv: %v", err)
		return nil
	}

	client := &MapperClient{
		id:                id,
		middleware:        m,
		games:             make(chan middleware.GameMsg),
		reviews:           make(chan middleware.ReviewsMsg),
		reviewsFile:       reviewsFile,
		reviewsFileWriter: csv.NewWriter(reviewsFile),
		finishedFile:      false,
		finished:          false,
		finishedGames:     false,
		cancelWg:          &sync.WaitGroup{},
	}

	go client.consumeGames()

	client.cancelWg.Add(1)
	return client
}

func (c *MapperClient) Close() {
	if c.finished {
		return
	}
	c.finished = true
	c.reviewsFile.Close()
	c.cancelWg.Done()
	if !c.finishedGames {
		close(c.games)
	}
	close(c.reviews)
	log.Infof("Waiting for mapper client %s to finish", c.id)
	c.cancelWg.Wait()
	log.Infof("Mapper client %s finished", c.id)

	if err := os.RemoveAll(fmt.Sprintf("database/%s", c.id)); err != nil {
		log.Errorf("Failed to remove mapper client database: %v", err)
	}
	log.Infof("action: mapper_client_close | result: success | client_id: %s", c.id)
}

func (c *MapperClient) consumeGames() {

	pendingLast := c.middleware.Config.Sharding.Amount

	c.cancelWg.Add(1)
	for game := range c.games {

		if game.Last {
			pendingLast--
			if pendingLast == 0 {
				c.finishedGames = true
				go c.consumeReviews()
				go c.writeFileToChan()
				close(c.games)
			}
			game.Ack()
			continue
		}

		file, err := os.OpenFile(fmt.Sprintf("database/%s/%d.csv", c.id, game.Game.AppId), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0755)
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
	log.Infof("Mapper client %s finished consuming reviews", c.id)
	c.cancelWg.Done()
}

func (c *MapperClient) storeReviews(reviews *middleware.ReviewsMsg) {
	for _, review := range reviews.Reviews {
		reviewStats := []string{
			strconv.Itoa(review.Id),
			review.AppId,
			review.Text,
			strconv.Itoa(review.Score),
		}

		if err := c.reviewsFileWriter.Write(reviewStats); err != nil {
			log.Errorf("Failed to write to reviews.csv: %v", err)
		}
	}
	c.reviewsFileWriter.Flush()
	reviews.Ack()
}

func (c *MapperClient) writeFileToChan() {
	file, err := os.Open(fmt.Sprintf("database/%s/reviews.csv", c.id))
	if err != nil {
		log.Errorf("action: write_file_to_chan | result: fail | error: %v", err)
		return
	}

	reader := csv.NewReader(file)
	batch := middleware.ReviewsMsg{ClientId: c.id, Reviews: make([]middleware.Review, 0)}

	c.cancelWg.Add(1)
	var last middleware.Review
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Errorf("action: write_file_to_chan | result: fail | error: %v", err)
			log.Infof("Last review: %v", last)
			continue
		}

		id, err := strconv.Atoi(record[0])
		if err != nil {
			log.Errorf("action: write_file_to_chan | result: fail | error: %v", err)
			continue
		}

		score, err := strconv.Atoi(record[3])
		if err != nil {
			log.Errorf("action: write_file_to_chan | result: fail | error: %v", err)
			continue
		}

		review := middleware.Review{
			Id:    id,
			AppId: record[1],
			Text:  record[2],
			Score: score,
		}

		last = review

		batch.Reviews = append(batch.Reviews, review)

		if len(batch.Reviews) == 100 {
			c.reviews <- batch
			batch.Reviews = make([]middleware.Review, 0)
			time.Sleep(10 * time.Millisecond)
		}
	}

	log.Infof("Finished reading reviews file for client %s", c.id)
	c.reviews <- batch
	c.finishedFile = true
	c.cancelWg.Done()
}

func (c *MapperClient) handleFinsished(reviewBatch middleware.ReviewsMsg) {
	log.Debugf("Received Last message for client %s: %v", reviewBatch.ClientId, reviewBatch.Last)
	if c.finished {
		log.Debugf("Received Last again, ignoring and NACKing...")
		reviewBatch.Nack()
		return
	}
	if !c.finishedFile {
		log.Debugf("Received Last but not finished reviews file, ignoring and NACKing...")
		reviewBatch.Nack()
		return
	}
	c.middleware.SendReviewsFinished(reviewBatch.ClientId, reviewBatch.Last+1)
	c.finished = true
	reviewBatch.Ack()
	os.RemoveAll(fmt.Sprintf("database/%s", c.id))
}
