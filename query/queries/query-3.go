package queries

import (
	"fmt"
	"os"
	"strconv"
	"time"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"
)

const GEOMETRY_DASH_APP_ID = 322170

const QUERY3_TOP_SIZE = 5

type Query3 struct {
	middleware *middleware.Middleware
	shardId    int
	clients    map[string]*Query3Client
	commit     *shared.Commit
}

func NewQuery3(m *middleware.Middleware, shardId int) *Query3 {
	return &Query3{
		middleware: m,
		shardId:    shardId,
		clients:    make(map[string]*Query3Client),
		commit:     shared.NewCommit("./database/commit.csv"),
	}
}

func (q *Query3) Run() {
	time.Sleep(500 * time.Millisecond)
	log.Info("Query 3 running")

	shared.RestoreCommit("./database/commit.csv", func(commit *shared.Commit) {
		log.Infof("Restored commit: %v", commit)

		os.Rename(commit.Data[0][2], commit.Data[0][3])

		processed := shared.NewProcessed(fmt.Sprintf("./database/%s/processed.bin", commit.Data[0][0]))

		if processed != nil {
			id, _ := strconv.Atoi(commit.Data[0][1])
			processed.Add(int64(id))
		}
	})

	statsQueue, err := q.middleware.ListenStats("3."+strconv.Itoa(q.shardId), strconv.Itoa(q.shardId), "Indie")
	if err != nil {
		log.Errorf("Error listening stats: %s", err)
		return
	}

	metric := shared.NewMetric(10000, func(total int, elapsed time.Duration, rate float64) string {
		return fmt.Sprintf("[Query 3-%d] Processed %d stats in %s (%.2f stats/s)", q.shardId, total, elapsed, rate)
	})

	statsQueue.Consume(func(message *middleware.StatsMsg) error {
		metric.Update(1)

		client, exists := q.clients[message.ClientId]
		if !exists {
			client = NewQuery3Client(q.middleware, q.commit, message.ClientId, q.shardId)
			q.clients[message.ClientId] = client
		}

		client.processStat(message)

		return nil
	})

}

type Query3Client struct {
	middleware     *middleware.Middleware
	commit         *shared.Commit
	clientId       string
	shardId        int
	processedStats *shared.Processed
	cache          *shared.Cache[*middleware.Stats]
}

func NewQuery3Client(m *middleware.Middleware, commit *shared.Commit, clientId string, shardId int) *Query3Client {
	os.MkdirAll(fmt.Sprintf("./database/%s/stats", clientId), 0777)
	return &Query3Client{
		middleware:     m,
		commit:         commit,
		clientId:       clientId,
		shardId:        shardId,
		processedStats: shared.NewProcessed(fmt.Sprintf("./database/%s/processed.bin", clientId)),
		cache:          shared.NewCache[*middleware.Stats](),
	}
}

func (qc *Query3Client) processStat(msg *middleware.StatsMsg) {
	if msg.Last {
		qc.sendResult()
		qc.End()
		msg.Ack()
		return
	}

	if qc.processedStats.Contains(int64(msg.Stats.Id)) {
		msg.Ack()
		return
	}

	if msg.Stats.Positives == 0 {
		msg.Ack()
		return
	}

	tmpFile, err := os.CreateTemp("./database", fmt.Sprintf("%d.csv", msg.Stats.AppId))
	if err != nil {
		log.Errorf("failed to create temp file: %v", err)
		return
	}

	if stat := shared.UpdateStat(qc.clientId, msg.Stats, tmpFile, qc.cache); stat == nil {
		log.Errorf("Failed to upsert stats, could not retrieve stat for client %s", qc.clientId)
		return
	}

	if msg.Stats.AppId == GEOMETRY_DASH_APP_ID {
		shared.TestTolerance(1, 8000, fmt.Sprintf("Exiting after tmp (game %d)", msg.Stats.AppId))
	}

	realFilename := fmt.Sprintf("./database/%s/stats/%d.csv", qc.clientId, msg.Stats.AppId)

	qc.commit.Write([][]string{
		{qc.clientId, strconv.Itoa(msg.Stats.Id), tmpFile.Name(), realFilename},
	})

	if msg.Stats.AppId == GEOMETRY_DASH_APP_ID {
		shared.TestTolerance(1, 8000, fmt.Sprintf("Exiting after commit (game %d)", msg.Stats.AppId))
	}
	qc.processedStats.Add(int64(msg.Stats.Id))

	if msg.Stats.AppId == GEOMETRY_DASH_APP_ID {
		shared.TestTolerance(1, 8000, fmt.Sprintf("Exiting after adding processed stat (game %d)", msg.Stats.AppId))
	}

	os.Rename(tmpFile.Name(), realFilename)

	if msg.Stats.AppId == GEOMETRY_DASH_APP_ID {
		shared.TestTolerance(1, 8000, fmt.Sprintf("Exiting after renaming (game %d)", msg.Stats.AppId))
	}

	qc.commit.End()

	msg.Ack()
}

func (qc *Query3Client) sendResult() {
	log.Infof("Sending result for client %s", qc.clientId)

	top := shared.GetTopStatsFS(qc.clientId, QUERY3_TOP_SIZE, func(a *middleware.Stats, b *middleware.Stats) bool {
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
		ClientId:       qc.clientId,
		Payload:        query3Result,
		IsFinalMessage: true,
		QueryId:        3,
	}

	shared.TestTolerance(1, 2, "Exiting before sending result")

	if err := qc.middleware.SendResult("3", result); err != nil {
		log.Errorf("Failed to send result: %v", err)
	}

	shared.TestTolerance(1, 2, "Exiting after sending result")
}

func (qc *Query3Client) End() {
	os.RemoveAll(fmt.Sprintf("./database/%s", qc.clientId))
	qc.processedStats.Close()
}
