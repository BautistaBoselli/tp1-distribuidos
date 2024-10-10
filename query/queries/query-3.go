package queries

import (
	"fmt"
	"os"
	"strconv"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"
)

const QUERY3_TOP_SIZE = 5

type Query3 struct {
	middleware *middleware.Middleware
	shardId    int
	files      []*os.File
}

func NewQuery3(m *middleware.Middleware, shardId int) *Query3 {
	files := make([]*os.File, 100)
	for i := 0; i < 100; i++ {
		filename := fmt.Sprintf("query-3-%d.csv", i)
		file, err := os.Create(filename)
		if err != nil {
			log.Errorf("Error creating file: %s", err)
			return nil
		}
		files[i] = file
	}

	return &Query3{
		middleware: m,
		shardId:    shardId,
		files:      files,
	}
}

func (q *Query3) Close() {
	q.middleware.Close()
}

func (q *Query3) Run() {
	log.Info("Query 3 running")

	statsQueue, err := q.middleware.ListenStats(strconv.Itoa(q.shardId), "Indie")
	if err != nil {
		log.Errorf("Error listening stats: %s", err)
		return
	}

	i := 0
	statsQueue.Consume(func(message *middleware.StatsMsg, ack func()) error {
		i++
		if i % 100000 == 0 {
			log.Infof("Query 3 Processed %d stats", i)
		}
		q.processStats(message.Stats)
		ack()
		return nil
	})

	q.sendResult()

	select {}
}

func (q *Query3) processStats(message *middleware.Stats) {
	shared.UpsertStatsFile("query-3", message)
}

func (q *Query3) sendResult() {
	top := shared.GetTopStats("query-3", QUERY3_TOP_SIZE, func(a *middleware.Stats, b *middleware.Stats) bool {
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
		Payload:             query3Result,
		IsFinalMessage:      true,
		QueryId:             3,
		IsFragmentedMessage: false,
	}

	if err := q.middleware.SendResult("3", result); err != nil {
		log.Errorf("Failed to send result: %v", err)
	}

}
