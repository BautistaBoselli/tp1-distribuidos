package queries

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"

	"github.com/rylans/getlang"
)

type Query4 struct {
	middleware *middleware.Middleware
	shardId    int
	finishedWg map[string]*sync.WaitGroup
}

func NewQuery4(m *middleware.Middleware, shardId int) *Query4 {
	return &Query4{
		middleware: m,
		shardId:    shardId,
		finishedWg: make(map[string]*sync.WaitGroup),
	}
}

func (q *Query4) Close() {
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
	messagesChan := make(chan *middleware.StatsMsg)
	go q.updateStats(messagesChan)

	statsQueue.Consume(func(message *middleware.StatsMsg) error {
		metric.Update(1)

		if _, exists := q.finishedWg[message.ClientId]; !exists {
			q.finishedWg[message.ClientId] = &sync.WaitGroup{}
		}

		if message.Last {
			q.finishedWg[message.ClientId].Wait()
			q.sendResultFinal(message.ClientId)
			os.RemoveAll(fmt.Sprintf("./database/%s", message.ClientId))
			message.Ack()

			return nil
		}

		go q.processStats(message, messagesChan)
		message.Ack()
		return nil
	})

	for _, wg := range q.finishedWg {
		wg.Wait()
	}
}

func (q *Query4) updateStats(messages chan *middleware.StatsMsg) {

	for message := range messages {
		isNegative := message.Stats.Negatives == 1
		updatedStat := shared.UpsertStats(message.ClientId, message.Stats)
		if updatedStat == nil {
			log.Errorf("Failed to update stat, could not retrieve file for client %s", message.ClientId)
			continue
		}

		if isNegative && updatedStat.Negatives == q.middleware.Config.Query.MinNegatives {
			q.sendResult(message.ClientId, updatedStat)
		}
	}
}

func (q *Query4) processStats(message *middleware.StatsMsg, messagesChan chan *middleware.StatsMsg) {
	q.finishedWg[message.ClientId].Add(1)
	defer q.finishedWg[message.ClientId].Done()
	if message.Stats.Negatives == 0 {
		return
	}

	if !isEnglish(message.Stats) {
		return
	}

	messagesChan <- message
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

func (q *Query4) sendResultFinal(clientId string) {
	log.Infof("Query 4 [FINAL]")
	result := &middleware.Result{
		ClientId:       clientId,
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
