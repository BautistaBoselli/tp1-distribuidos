package queries

import (
	"fmt"
	"time"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type Query1 struct {
	middleware     *middleware.Middleware
	shardId        int
	resultInterval int
	processedGames int64
	results        map[string]*middleware.Query1Result
	cancelled      bool
}

func NewQuery1(m *middleware.Middleware, shardId int, resultInterval int) *Query1 {

	return &Query1{
		middleware:     m,
		shardId:        shardId,
		resultInterval: resultInterval,
		results:        make(map[string]*middleware.Query1Result),
	}
}

func (q *Query1) Close() {
	q.cancelled = true
}

func (q *Query1) Run() {
	log.Info("Query 1 running")

	gamesQueue, err := q.middleware.ListenGames(fmt.Sprintf("%d", q.shardId))
	if err != nil {
		log.Errorf("Error listening games: %s", err)
		return
	}

	metric := shared.NewMetric(10000, func(total int, elapsed time.Duration, rate float64) string {
		return fmt.Sprintf("[Query 1-%d] Processed %d games in %s (%.2f games/s)", q.shardId, total, elapsed, rate)
	})

	gamesQueue.Consume(func(message *middleware.GameMsg) error {
		metric.Update(1)

		if message.Last {
			q.sendResult(message.ClientId, true)
			message.Ack()
			return nil
		}

		q.processGame(message.ClientId, message.Game)
		message.Ack()
		return nil
	})

}

func (q *Query1) processGame(clientId string, game *middleware.Game) {
	q.processedGames++
	result, exists := q.results[clientId]
	if !exists {
		result = &middleware.Query1Result{
			Windows: 0,
			Mac:     0,
			Linux:   0,
		}
		q.results[clientId] = result
	}
	if game.Windows {
		result.Windows++
	}
	if game.Linux {
		result.Linux++
	}
	if game.Mac {
		result.Mac++
	}

	if q.processedGames%int64(q.resultInterval) == 0 {
		q.sendResult(clientId, false)
	}
}

func (q *Query1) sendResult(clientId string, final bool) {
	result, exists := q.results[clientId]
	if !exists {
		log.Errorf("Query 1 - Client %s not found", clientId)
		return
	}
	result.Final = final

	resultMsg := &middleware.Result{
		ClientId:       clientId,
		QueryId:        1,
		IsFinalMessage: final,
		Payload:        result,
	}

	if resultMsg.IsFinalMessage {
		log.Infof("Query 1 [FINAL] - Query 1-%d - Windows: %d, Linux: %d, Mac: %d", q.shardId, result.Windows, result.Linux, result.Mac)

		if err := q.middleware.SendResult("1", resultMsg); err != nil {
			log.Errorf("Failed to send result: %v", err)
		}

	} else {
		log.Infof("Query 1 [PARTIAL] - Query 1-%d - Windows: %d, Linux: %d, Mac: %d", q.shardId, result.Windows, result.Linux, result.Mac)

		if err := q.middleware.SendResult("1", resultMsg); err != nil {
			log.Errorf("Failed to send result: %v", err)
		}
	}

	q.results[clientId] = &middleware.Query1Result{
		Windows: 0,
		Mac:     0,
		Linux:   0,
		Final:   false,
	}
}
