package queries

import (
	"fmt"
	"tp1-distribuidos/middleware"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type Query1 struct {
	middleware     *middleware.Middleware
	shardId        int
	resultInterval int
	processedGames int64
	result         middleware.Query1Result
}

func NewQuery1(m *middleware.Middleware, shardId int, resultInterval int) *Query1 {

	return &Query1{
		middleware:     m,
		shardId:        shardId,
		resultInterval: resultInterval,
		result: middleware.Query1Result{
			Windows: 0,
			Mac:     0,
			Linux:   0,
			Final:   false,
		},
	}
}

func (q *Query1) Close() {
	q.middleware.Close()
}

func (q *Query1) Run() {
	log.Info("Query 1 running")

	gamesQueue, err := q.middleware.ListenGames(fmt.Sprintf("%d", q.shardId))
	if err != nil {
		log.Errorf("Error listening games: %s", err)
		return
	}

	gamesQueue.Consume(func(message *middleware.GameBatch, ack func()) error {
		if message.Last {
			q.sendResult(true)
			ack()
			return nil
		}

		q.processGame(message.Game)
		ack()
		return nil
	})
}

func (q *Query1) processGame(game *middleware.Game) {
	q.processedGames++
	if game.Windows {
		q.result.Windows++
	}
	if game.Linux {
		q.result.Linux++
	}
	if game.Mac {
		q.result.Mac++
	}

	if q.processedGames%int64(q.resultInterval) == 0 {
		q.sendResult(false)
	}
}

func (q *Query1) sendResult(final bool) {
	q.result.Final = final

	if q.result.Final {
		log.Infof("Query 1 [FINAL] - Shard %d - Windows: %d, Linux: %d, Mac: %d", q.shardId, q.result.Windows, q.result.Linux, q.result.Mac)
	} else {
		log.Infof("Query 1 [PARTIAL] - Shard %d - Windows: %d, Linux: %d, Mac: %d", q.shardId, q.result.Windows, q.result.Linux, q.result.Mac)
	}

	q.result = middleware.Query1Result{
		Windows: 0,
		Mac:     0,
		Linux:   0,
		Final:   false,
	}
}
