package queries

import (
	"fmt"
	"os"
	"strconv"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"
)

type Query4 struct {
	middleware *middleware.Middleware
	shardId    int
}

const QUERY4_MIN_NEGATIVES = 2

func NewQuery4(m *middleware.Middleware, shardId int) *Query4 {

	for i := 0; i < 100; i++ {
		filename := fmt.Sprintf("query-4-%d-%d.csv", shardId, i)
		file, err := os.Create(filename)
		if err != nil {
			log.Errorf("Error creating file: %s", err)
			return nil
		}
		defer file.Close()
	}

	return &Query4{
		middleware: m,
		shardId:    shardId,
	}
}

func (q *Query4) Close() {
	q.middleware.Close()
}

func (q *Query4) Run() {
	log.Info("Query 4 running")

	statsQueue, err := q.middleware.ListenStats(strconv.Itoa(q.shardId), "*")
	if err != nil {
		log.Errorf("Error listening stats: %s", err)
		return
	}

	statsQueue.Consume(func(message *middleware.StatsMsg, ack func()) error {
		q.processStats(message.Stats)
		ack()
		return nil
	})

	q.sendResultFinal()
}

func (q *Query4) processStats(message *middleware.Stats) {
	if !isEnglish(message) {
		return
	}

	updatedStat := shared.UpsertStatsFile("query-4", message)

	if message.Negatives == 1 && updatedStat.Negatives == QUERY4_MIN_NEGATIVES {
		q.sendResult(updatedStat)
	}
}

func (q *Query4) sendResult(message *middleware.Stats) {
	log.Infof("Query 4 [PARTIAL]: %s", message.Name)
	query4Result := middleware.Query4Result{
		Game: message.Name,
	}

	result := &middleware.Result{
		QueryId:             4,
		Payload:             query4Result,
		IsFragmentedMessage: false,
		IsFinalMessage:      false,
	}

	if err := q.middleware.SendResult("4", result); err != nil {
		log.Errorf("Failed to send result: %v", err)
	}
}

func (q *Query4) sendResultFinal() {
	log.Infof("Query 4 [FINAL]")
	result := &middleware.Result{
		QueryId:             4,
		IsFinalMessage:      true,
		IsFragmentedMessage: false,
	}

	if err := q.middleware.SendResult("4", result); err != nil {
		log.Errorf("Failed to send result: %v", err)
	}
}

func isEnglish(message *middleware.Stats) bool {
	return true
}
