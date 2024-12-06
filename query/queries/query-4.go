package queries

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"

	"github.com/rylans/getlang"
)

type Query4 struct {
	middleware      *middleware.Middleware
	shardId         int
	clients         map[string]*Query4Client
	commit          *shared.Commit
	FinishedClients *shared.FinishedClients
}

func NewQuery4(m *middleware.Middleware, shardId int) *Query4 {
	return &Query4{
		middleware:      m,
		shardId:         shardId,
		clients:         make(map[string]*Query4Client),
		commit:          shared.NewCommit("./database/commit.csv"),
		FinishedClients: shared.NewFinishedClients("finished-4."+strconv.Itoa(shardId), m),
	}
}

func (q *Query4) Close() {
}

func (q *Query4) Run() {
	go q.FinishedClients.Consume()

	time.Sleep(500 * time.Millisecond)
	log.Info("Query 4 running")

	shared.RestoreCommit("./database/commit.csv", func(commit *shared.Commit) {
		log.Infof("Restored commit: %v", commit)

		os.Rename(commit.Data[0][2], commit.Data[0][3])

		processed := shared.NewProcessed(fmt.Sprintf("./database/%s/processed.bin", commit.Data[0][0]))

		if processed != nil {
			id, _ := strconv.Atoi(commit.Data[0][1])
			processed.Add(int64(id))
		}

		file, _ := os.OpenFile(commit.Data[0][3], os.O_RDWR|os.O_CREATE, 0777)
		reader := csv.NewReader(file)
		val, _ := reader.Read()
		stat, _ := shared.ParseStat(val)
		if stat.Negatives == q.middleware.Config.Query.MinNegatives {
			result := &middleware.Result{
				ClientId: commit.Data[0][0],
				QueryId:  4,
				ShardId:  q.shardId,
				Payload: middleware.Query4Result{
					Game: stat.Name,
				},
				IsFinalMessage: false,
			}

			if err := q.middleware.SendResult("4", result); err != nil {
				log.Errorf("Failed to send result: %v", err)
			}
		}
	})

	statsQueue, err := q.middleware.ListenStats("4."+strconv.Itoa(q.shardId), strconv.Itoa(q.shardId), "Action")
	if err != nil {
		log.Errorf("Error listening stats: %s", err)
		return
	}

	metric := shared.NewMetric(25000, func(total int, elapsed time.Duration, rate float64) string {
		return fmt.Sprintf("[Query 4-%d] Processed %d stats in %s (%.2f stats/s)", q.shardId, total, elapsed, rate)
	})
	messagesChan := make(chan *middleware.StatsMsg)

	go statsQueue.Consume(func(message *middleware.StatsMsg) error {
		q.FinishedClients.Lock()
		defer q.FinishedClients.Unlock()

		if q.FinishedClients.Contains(message.ClientId) {
			message.Ack()
			return nil
		}

		metric.Update(1)

		client, exists := q.clients[message.ClientId]
		if !exists {
			client = NewQuery4Client(q.middleware, q.commit, message.ClientId, q.shardId)
			q.clients[message.ClientId] = client
		}

		if message.Last {
			messagesChan <- message
			return nil
		}

		go client.filterStats(message, messagesChan)
		return nil
	})

	q.consumeFilteredStats(messagesChan)
}

func (q *Query4) consumeFilteredStats(messages chan *middleware.StatsMsg) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
	defer stop()

	for {
		select {
		case <-ctx.Done():
			return
		case message := <-messages:
			q.FinishedClients.Lock()

			if q.FinishedClients.Contains(message.ClientId) {
				message.Ack()
				q.FinishedClients.Unlock()
				continue
			}
			client := q.clients[message.ClientId]
			client.processStat(message)
			q.FinishedClients.Unlock()
		}
	}
}

type Query4Client struct {
	middleware     *middleware.Middleware
	commit         *shared.Commit
	clientId       string
	shardId        int
	processedStats *shared.Processed
	wg             sync.WaitGroup
	cache          *shared.Cache[*middleware.Stats]
}

func NewQuery4Client(m *middleware.Middleware, commit *shared.Commit, clientId string, shardId int) *Query4Client {
	os.MkdirAll(fmt.Sprintf("./database/%s/stats", clientId), 0777)
	return &Query4Client{
		middleware:     m,
		commit:         commit,
		clientId:       clientId,
		shardId:        shardId,
		processedStats: shared.NewProcessed(fmt.Sprintf("./database/%s/processed.bin", clientId)),
		cache:          shared.NewCache[*middleware.Stats](),
		wg:             sync.WaitGroup{},
	}
}

func (qc *Query4Client) filterStats(message *middleware.StatsMsg, messagesChan chan *middleware.StatsMsg) {
	qc.wg.Add(1)
	defer qc.wg.Done()
	if message.Stats.Negatives == 0 {
		message.Ack()
		return
	}

	if !isEnglish(message.Stats) {
		message.Ack()
		return
	}

	messagesChan <- message
}

func (qc *Query4Client) processStat(msg *middleware.StatsMsg) {
	if msg.Last {
		go func() {
			qc.wg.Wait()
			qc.sendResultFinal()
			msg.Ack()
			qc.End()
		}()
		return
	}

	if qc.processedStats.Contains(int64(msg.Stats.Id)) {
		if msg.Stats.Negatives == 1 {
			stat := shared.GetStat(qc.clientId, msg.Stats.AppId)
			if stat.Negatives == qc.middleware.Config.Query.MinNegatives {
				qc.sendResult(msg.Stats)
			}
		}
		msg.Ack()
		return
	}

	tmpFile, err := os.CreateTemp("./database", fmt.Sprintf("%d.csv", msg.Stats.AppId))
	if err != nil {
		log.Errorf("failed to create temp file: %v", err)
		return
	}

	isNegative := msg.Stats.Negatives == 1

	stat := shared.UpdateStat(qc.clientId, msg.Stats, tmpFile, qc.cache)
	if stat == nil {
		log.Errorf("Failed to upsert stats, could not retrieve stat for client %s", qc.clientId)
		return
	}

	realFilename := fmt.Sprintf("./database/%s/stats/%d.csv", qc.clientId, msg.Stats.AppId)

	qc.commit.Write([][]string{
		{qc.clientId, strconv.Itoa(msg.Stats.Id), tmpFile.Name(), realFilename},
	})

	shared.TestTolerance(1, 18000, "Exiting before a commit")

	qc.processedStats.Add(int64(msg.Stats.Id))

	os.Rename(tmpFile.Name(), realFilename)

	if isNegative && stat.Negatives == qc.middleware.Config.Query.MinNegatives {
		shared.TestTolerance(1, 12, "Exiting before sending result")
		qc.sendResult(stat)
		shared.TestTolerance(1, 12, "Exiting after sending result")
	}

	qc.commit.End()
	msg.Ack()
}

func (qc *Query4Client) sendResult(message *middleware.Stats) {
	log.Infof("Query 4 [PARTIAL]: %s", message.Name)
	query4Result := middleware.Query4Result{
		Game: message.Name,
	}

	result := &middleware.Result{
		ClientId:       qc.clientId,
		QueryId:        4,
		ShardId:        qc.shardId,
		Payload:        query4Result,
		IsFinalMessage: false,
	}

	if err := qc.middleware.SendResult("4", result); err != nil {
		log.Errorf("Failed to send result: %v", err)
	}
}

func (qc *Query4Client) sendResultFinal() {
	log.Infof("Query 4 [FINAL]")
	result := &middleware.Result{
		ClientId:       qc.clientId,
		QueryId:        4,
		ShardId:        qc.shardId,
		IsFinalMessage: true,
		Payload:        middleware.Query4Result{},
	}

	if err := qc.middleware.SendResult("4", result); err != nil {
		log.Errorf("Failed to send result: %v", err)
	}
}

func isEnglish(message *middleware.Stats) bool {
	lang := getlang.FromString(message.Text)
	return lang.LanguageName() == "English"
}

func (qc *Query4Client) End() {
	os.RemoveAll(fmt.Sprintf("./database/%s", qc.clientId))
	qc.processedStats.Close()
}
