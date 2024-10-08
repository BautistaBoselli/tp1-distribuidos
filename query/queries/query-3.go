package queries

import (
	"fmt"
	"os"
	"strconv"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"
)

const QUERY3_TOP_SIZE = 2

type Query3 struct {
	middleware *middleware.Middleware
	shardId    int
	filename   string
}

func NewQuery3(m *middleware.Middleware, shardId int) *Query3 {
	filename := fmt.Sprintf("query-3-%d.csv", shardId)

	file, err := os.Create(filename)
	if err != nil {
		log.Errorf("Error creating file: %s", err)
		return nil
	}
	defer file.Close()

	return &Query3{
		middleware: m,
		shardId:    shardId,
		filename:   filename,
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

	statsQueue.Consume(func(message *middleware.Stats, ack func()) error {
		q.processStats(message)
		ack()
		q.sendResult()
		return nil
	})
}

func (q *Query3) processStats(message *middleware.Stats) {
	shared.UpsertStatsFile(q.filename, message)
}

func (q *Query3) sendResult() {
	top := shared.GetTopStats(q.filename, QUERY3_TOP_SIZE, func(a *middleware.Stats, b *middleware.Stats) bool {
		return a.Positives > b.Positives
	})

	log.Infof("Query 3 [FINAL]")
	for _, game := range top {
		log.Infof("Game: %s (Positives: %d, Negatives: %d)", game.Name, game.Positives, game.Negatives)
	}
}
