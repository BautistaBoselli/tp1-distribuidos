package queries

import (
	"fmt"
	"tp1-distribuidos/middleware"
)

type Query2 struct {
	middleware *middleware.Middleware
	shardId    int
	topPlayed  []middleware.Game
}

func NewQuery2(m *middleware.Middleware, shardId int) *Query2 {

	return &Query2{
		middleware: m,
		shardId:    shardId,
		topPlayed:  make([]middleware.Game, 0, 10),
	}
}

func (q *Query2) Close() {
	q.middleware.Close()
}

func (q *Query2) Run() {
	log.Info("Query 1 running")

	gamesQueue, err := q.middleware.ListenGames(fmt.Sprintf("%d", q.shardId))
	if err != nil {
		log.Errorf("Error listening games: %s", err)
		return
	}

	gamesQueue.Consume(func(message *middleware.GameBatch, ack func()) error {
		if message.Last {
			q.sendResult()
			ack()
			return nil
		}

		q.processGame(message.Game)
		ack()
		return nil
	})
}

func (q *Query2) processGame(game *middleware.Game) {
	// Find the position where the new game should be inserted
	insertIndex := len(q.topPlayed)
	for i, topGame := range q.topPlayed {
		if game.AvgPlaytime > topGame.AvgPlaytime {
			insertIndex = i
			break
		}
	}

	// If the game should be in the top 10
	if insertIndex < 10 {
		q.topPlayed = append(q.topPlayed, middleware.Game{})
		copy(q.topPlayed[insertIndex+1:], q.topPlayed[insertIndex:])
		q.topPlayed[insertIndex] = *game
	}

	if len(q.topPlayed) > 10 {
		q.topPlayed = q.topPlayed[:10]
	}

}

func (q *Query2) sendResult() {
	log.Infof("Query 2 [FINAL]")

	for i, game := range q.topPlayed {
		log.Debugf("Top %d game: %s (%.2f)", i+1, game.Name, game.AvgPlaytime)
	}
}
