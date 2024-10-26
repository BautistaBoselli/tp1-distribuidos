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
	topPlayed  map[string][]middleware.Game
	cancelled  bool
}

func NewQuery2(m *middleware.Middleware, shardId int) *Query2 {

	return &Query2{
		middleware: m,
		shardId:    shardId,
		topPlayed:  make(map[string][]middleware.Game),
	}
}

func (q *Query2) Close() {
	q.cancelled = true
}

func (q *Query2) Run() {
	log.Info("Query 1 running")

	gamesQueue, err := q.middleware.ListenGames(fmt.Sprintf("%d", q.shardId))
	if err != nil {
		log.Errorf("Error listening games: %s", err)
		return
	}

	gamesQueue.Consume(func(message *middleware.GameMsg) error {
		if message.Last {
			q.sendResult(message.ClientId)
			message.Ack()

			return nil
		}

		q.processGame(message.ClientId, message.Game)
		message.Ack()

		return nil
	})

}

func (q *Query2) processGame(clientId string, game *middleware.Game) {
	if game.Year < 2010 || game.Year > 2019 || !slices.Contains(game.Genres, "Indie") {
		return
	}

	// Find the position where the new game should be inserted
	insertIndex := len(q.topPlayed[clientId])
	for i, topGame := range q.topPlayed[clientId] {
		if game.AvgPlaytime > topGame.AvgPlaytime {
			insertIndex = i
			break
		}
	}

	// If the game should be in the top TOP_SIZE
	if insertIndex < QUERY2_TOP_SIZE {
		q.topPlayed[clientId] = append(q.topPlayed[clientId], middleware.Game{})
		copy(q.topPlayed[clientId][insertIndex+1:], q.topPlayed[clientId][insertIndex:])
		q.topPlayed[clientId][insertIndex] = *game
	}

	if len(q.topPlayed[clientId]) > QUERY2_TOP_SIZE {
		q.topPlayed[clientId] = q.topPlayed[clientId][:QUERY2_TOP_SIZE]
	}

}

func (q *Query2) sendResult(clientId string) {
	log.Infof("Query 2 [FINAL]")
	query2Result := middleware.Query2Result{
		TopGames: q.topPlayed[clientId],
	}

	result := middleware.Result{
		ClientId:       clientId,
		QueryId:        2,
		IsFinalMessage: true,
		Payload:        query2Result,
	}

	if err := q.middleware.SendResult("2", &result); err != nil {
		log.Errorf("Failed to send result: %v", err)
	}
	for i, game := range q.topPlayed[clientId] {
		log.Debugf("Top %d game: %s (%d)", i+1, game.Name, game.AvgPlaytime)
	}
}
