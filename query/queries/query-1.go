package queries

import (
	"tp1-distribuidos/middleware"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type Query1 struct {
	middleware     *middleware.Middleware
	shardId        int
	resultInterval int
	processedGames int64
	windows        int64
	mac            int64
	linux          int64
}

func NewQuery1(middleware *middleware.Middleware, shardId int, resultInterval int) *Query1 {
	return &Query1{
		middleware:     middleware,
		shardId:        shardId,
		resultInterval: resultInterval,
	}
}

func (q *Query1) Close() {
	q.middleware.Close()
}

func (q *Query1) Run() {
	log.Info("Query 1 running")

	gamesQueue, err := q.middleware.ListenGames()
	if err != nil {
		log.Errorf("Error listening games: %s", err)
		return
	}

	gamesQueue.Consume(func(message *middleware.GameBatch, ack func()) error {
		for _, game := range message.Games {
			q.processGame(&game)
		}

		if message.Last {
			q.sendResult(true)
		}
		ack()
		return nil
	})
}

func (q *Query1) processGame(game *middleware.Game) {
	q.processedGames++
	if game.Windows {
		q.windows++
	}
	if game.Linux {
		q.linux++
	}
	if game.Mac {
		q.mac++
	}

	if q.processedGames%int64(q.resultInterval) == 0 {
		q.sendResult(false)
	}
}

func (q *Query1) sendResult(final bool) {
	if final {
		log.Infof("Query 1 [FINAL] - Shard %d - Windows: %d, Linux: %d, Mac: %d", q.shardId, q.windows, q.linux, q.mac)
	} else {
		log.Infof("Query 1 [PARTIAL] - Shard %d - Windows: %d, Linux: %d, Mac: %d", q.shardId, q.windows, q.linux, q.mac)
	}
}
