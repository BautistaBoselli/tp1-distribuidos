package queries

import (
	"strconv"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"

	"github.com/rylans/getlang"
)

type Query4 struct {
	middleware *middleware.Middleware
	shardId    int
}

const QUERY4_MIN_NEGATIVES = 5000

func NewQuery4(m *middleware.Middleware, shardId int) *Query4 {

	files, err := shared.InitStoreFiles("query-4", 100)
	if err != nil {
		log.Errorf("Error initializing store files: %s", err)
		return nil
	}

	for _, file := range files {
		file.Close()
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

	statsQueue, err := q.middleware.ListenStats(strconv.Itoa(q.shardId), "Action")
	if err != nil {
		log.Errorf("Error listening stats: %s", err)
		return
	}

	i := 0
	statsQueue.Consume(func(message *middleware.StatsMsg, ack func()) error {
		i++
		if i%25000 == 0 {
			log.Infof("Query 4 Processed %d stats", i)
		}
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

	updatedStat := shared.UpsertStatsFile("query-4", 100, message)

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
