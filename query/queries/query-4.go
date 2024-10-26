package queries

import (
	"fmt"
	"strconv"
	"time"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"

	"github.com/rylans/getlang"
)

type Query4 struct {
	middleware *middleware.Middleware
	shardId    int
	cancelled  bool
}

func NewQuery4(m *middleware.Middleware, shardId int) *Query4 {
	return &Query4{
		middleware: m,
		shardId:    shardId,
	}
}

func (q *Query4) Close() {
	q.cancelled = true
}

func (q *Query4) Run() {
	log.Info("Query 4 running")

	statsQueue, err := q.middleware.ListenStats(strconv.Itoa(q.shardId), "Action")
	if err != nil {
		log.Errorf("Error listening stats: %s", err)
		return
	}

	metric := shared.NewMetric(25000, func(total int, elapsed time.Duration, rate float64) string {
		return fmt.Sprintf("[Query 4-%d] Processed %d stats in %s (%.2f stats/s)", q.shardId, total, elapsed, rate)
	})
	statsQueue.Consume(func(message *middleware.StatsMsg) error {
		metric.Update(1)

		if message.Last {
			q.sendResultFinal()
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

	q.sendResultFinal()
}

func (q *Query4) processStats(message *middleware.StatsMsg) {
	if message.Stats.Negatives == 0 {
		return
	}

	if !isEnglish(message.Stats) {
		return
	}

	isNegative := message.Stats.Negatives == 1
	updatedStat := shared.UpsertStats(message.ClientId, message.Stats)

	if isNegative && updatedStat.Negatives == q.middleware.Config.Query.MinNegatives {
		q.sendResult(message.ClientId, updatedStat)
	}
}

func (q *Query4) sendResult(clientId string, message *middleware.Stats) {
	log.Infof("Query 4 [PARTIAL]: %s", message.Name)
	query4Result := middleware.Query4Result{
		Game: message.Name,
	}

	result := &middleware.Result{
		ClientId:       clientId,
		QueryId:        4,
		Payload:        query4Result,
		IsFinalMessage: false,
	}

	if err := q.middleware.SendResult("4", result); err != nil {
		log.Errorf("Failed to send result: %v", err)
	}
}

func (q *Query4) sendResultFinal() {
	log.Infof("Query 4 [FINAL]")
	result := &middleware.Result{
		QueryId:        4,
		IsFinalMessage: true,
	}

	if err := q.middleware.SendResult("4", result); err != nil {
		log.Errorf("Failed to send result: %v", err)
	}
}

func isEnglish(message *middleware.Stats) bool {
	lang := getlang.FromString(message.Text)
	// log.Infof("Language: %s", lang.LanguageName())
	return lang.LanguageName() == "English"
}
