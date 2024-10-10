package queries

import (
	"fmt"
	"slices"
	"tp1-distribuidos/middleware"
)

const QUERY2_TOP_SIZE = 10

type Query2 struct {
	middleware *middleware.Middleware
	shardId    int
	topPlayed  []middleware.Game
}

func NewQuery2(m *middleware.Middleware, shardId int) *Query2 {

	return &Query2{
		middleware: m,
		shardId:    shardId,
		topPlayed:  make([]middleware.Game, 0, QUERY2_TOP_SIZE),
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

	gamesQueue.Consume(func(message *middleware.GameMsg, ack func()) error {
		if message.Last {
			q.sendResult()
			ack()
			return nil
		}

		q.processGame(message.Game)
		ack()
		return nil
	})

	q.sendResult()
}

func (q *Query2) processGame(game *middleware.Game) {
	if game.Year < 2010 || game.Year > 2019 || !slices.Contains(game.Genres, "Indie") {
		return
	}

	// Find the position where the new game should be inserted
	insertIndex := len(q.topPlayed)
	for i, topGame := range q.topPlayed {
		if game.AvgPlaytime > topGame.AvgPlaytime {
			insertIndex = i
			break
		}
	}

	// If the game should be in the top TOP_SIZE
	if insertIndex < QUERY2_TOP_SIZE {
		q.topPlayed = append(q.topPlayed, middleware.Game{})
		copy(q.topPlayed[insertIndex+1:], q.topPlayed[insertIndex:])
		q.topPlayed[insertIndex] = *game
	}

	if len(q.topPlayed) > QUERY2_TOP_SIZE {
		q.topPlayed = q.topPlayed[:QUERY2_TOP_SIZE]
	}

}

func (q *Query2) sendResult() {
	log.Infof("Query 2 [FINAL]")
	query2Result := middleware.Query2Result{
		TopGames: q.topPlayed,
	}

	result := middleware.Result{
		QueryId:        2,
		IsFinalMessage: true,
		// IsFragmentedMessage: false,
		Payload: query2Result,
	}

	if err := q.middleware.SendResult("2", &result); err != nil {
		log.Errorf("Failed to send result: %v", err)
	}
	for i, game := range q.topPlayed {
		log.Debugf("Top %d game: %s (%d)", i+1, game.Name, game.AvgPlaytime)
	}
}
