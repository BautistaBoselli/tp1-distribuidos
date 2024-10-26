package queries

import (
	"strconv"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"
)

const QUERY3_TOP_SIZE = 5

type Query3 struct {
	middleware *middleware.Middleware
	shardId    int
	cancelled  bool
	directory  shared.Directory
}

func NewQuery3(m *middleware.Middleware, shardId int) *Query3 {
	return &Query3{
		middleware: m,
		shardId:    shardId,
		directory:  shared.Directory{},
	}
}

func (q *Query3) Close() {
	q.cancelled = true
}

func (q *Query3) Run() {
	log.Info("Query 3 running")

	statsQueue, err := q.middleware.ListenStats(strconv.Itoa(q.shardId), "Indie")
	if err != nil {
		log.Errorf("Error listening stats: %s", err)
		return
	}

	i := 0
	statsQueue.Consume(func(message *middleware.StatsMsg) error {
		i++
		if i%25000 == 0 {
			log.Infof("Query 3 Processed %d stats", i)
		}

		if message.Last {
			q.sendResult(message.ClientId)
			message.Ack()
			return nil
		}

		q.processStats(message)
		message.Ack()
		return nil
	})

	if q.cancelled {
		return
	}

}

func (q *Query3) processStats(message *middleware.StatsMsg) {
	shared.UpsertStats(message.ClientId, message.Stats)
}

func (q *Query3) sendResult(clientId string) {
	log.Infof("Sending result for client %s", clientId)
	top := shared.GetTopStatsFS(clientId, QUERY3_TOP_SIZE, func(a *middleware.Stats, b *middleware.Stats) bool {
		return a.Positives > b.Positives
	})

	log.Infof("Query 3 [FINAL]")
	for _, game := range top {
		log.Infof("Game: %s (Positives: %d, Negatives: %d)", game.Name, game.Positives, game.Negatives)
	}

	query3Result := middleware.Query3Result{
		TopStats: top,
	}

	result := &middleware.Result{
		Payload:        query3Result,
		IsFinalMessage: true,
		QueryId:        3,
	}

	if err := q.middleware.SendResult("3", result); err != nil {
		log.Errorf("Failed to send result: %v", err)
	}

}
