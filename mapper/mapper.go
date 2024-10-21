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

type Client struct {
	id         string
	games      chan middleware.GameMsg
	gamesDir   shared.Directory
	reviews    chan middleware.ReviewsMsg
	totalGames int
}

func NewClient(id string) *Client {
	dir, err := shared.InitStoreFiles(id, "store", 100)
	if err != nil {
		log.Errorf("Failed to init store files: %v", err)
	}

	client := &Client{
		id:       id,
		games:    make(chan middleware.GameMsg),
		gamesDir: dir,
		reviews:  make(chan middleware.ReviewsMsg),
	}

	go client.consumeGames()
	return client
}

func (c *Client) Close() {
	close(c.games)
	close(c.reviews)
	c.gamesDir.Delete()
}

func (c *Client) consumeGames() {
	for game := range c.games {
		if game.Last {
			// c.consumeReviews()
			log.Infof("last game for client %s", c.id)
			log.Infof("total games for client %s: %d", c.id, c.totalGames)
			return
		}

		log.Infof("game %d for client %s", game.Game.AppId, c.id)

		total := 0
		for i, char := range strconv.Itoa(game.Game.AppId) {
			total += int(char) * i
		}
		hash := total % 100
		file := c.gamesDir.Files[hash]

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

		game.Ack()
	}
}

type Mapper struct {
	id           int
	middleware   *middleware.Middleware
	clients      map[string]*Client
	gamesQueue   *middleware.GamesQueue
	reviewsQueue *middleware.ReviewsQueue
	cancelled    bool
	// totalGames   int
}

func NewMapper(config *config.Config) (*Mapper, error) {
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
		clients:      make(map[string]*Client),
		gamesQueue:   gq,
		reviewsQueue: rq,
	}, nil
}

func (m *Mapper) Close() error {
	m.cancelled = true
	m.middleware.Close()
	for _, client := range m.clients {
		client.Close()
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

	err := m.gamesQueue.Consume(func(msg *middleware.GameMsg) error {
		if _, exists := m.clients[msg.ClientId]; !exists {
			log.Infof("New client %s", msg.ClientId)
			m.clients[msg.ClientId] = NewClient(msg.ClientId)
		}

		client := m.clients[msg.ClientId]
		client.games <- *msg
		return nil
	})
	if err != nil {
		log.Errorf("Failed to consume from games exchange: %v", err)
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
			i += len(reviewBatch.Reviews)
			if i%100000 == 0 {
				log.Infof("Processed %d reviews", i)
			}
			for _, review := range reviewBatch.Reviews {

				file, err := shared.GetStoreRWriter(shared.GetFilename(reviewBatch.ClientId, "store", review.AppId, 100))
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
