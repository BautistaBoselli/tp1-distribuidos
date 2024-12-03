package queries

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"time"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"
)

type Query5 struct {
	middleware *middleware.Middleware
	shardId    int
	clients    map[string]*Query5Client
	commit     *shared.Commit
}

func NewQuery5(m *middleware.Middleware, shardId int) *Query5 {
	return &Query5{
		middleware: m,
		shardId:    shardId,
		clients:    make(map[string]*Query5Client),
		commit:     shared.NewCommit("./database/commit.csv"),
	}
}

func (q *Query5) Run() {
	time.Sleep(500 * time.Millisecond)
	log.Info("Query 5 running")

	shared.RestoreCommit("./database/commit.csv", func(commit *shared.Commit) {
		log.Infof("Restored commit: %v", commit)

		os.Rename(commit.Data[0][2], commit.Data[0][3])

		processed := shared.NewProcessed(fmt.Sprintf("./database/%s/processed.bin", commit.Data[0][0]))

		if processed != nil {
			id, _ := strconv.Atoi(commit.Data[0][1])
			processed.Add(int64(id))
		}
	})

	statsQueue, err := q.middleware.ListenStats("5."+strconv.Itoa(q.shardId), strconv.Itoa(q.shardId), "Action")
	if err != nil {
		log.Errorf("Error listening stats: %s", err)
		return
	}

	metric := shared.NewMetric(25000, func(total int, elapsed time.Duration, rate float64) string {
		return fmt.Sprintf("[Query 5-%d] Processed %d stats in %s (%.2f stats/s)", q.shardId, total, elapsed, rate)
	})
	statsQueue.Consume(func(message *middleware.StatsMsg) error {
		metric.Update(1)

		client, exists := q.clients[message.ClientId]
		if !exists {
			client = NewQuery5Client(q.middleware, q.commit, message.ClientId, q.shardId)
			q.clients[message.ClientId] = client
		}

		client.processStat(message)
		return nil
	})
}

type Query5Client struct {
	middleware         *middleware.Middleware
	commit             *shared.Commit
	clientId           string
	shardId            int
	processedStats     *shared.Processed
	minNegativeReviews int
	cache              *shared.Cache[*middleware.Stats]
}

func NewQuery5Client(m *middleware.Middleware, commit *shared.Commit, clientId string, shardId int) *Query5Client {
	os.MkdirAll(fmt.Sprintf("./database/%s/stats", clientId), 0777)
	return &Query5Client{
		middleware:         m,
		commit:             commit,
		clientId:           clientId,
		shardId:            shardId,
		minNegativeReviews: -1,
		processedStats:     shared.NewProcessed(fmt.Sprintf("./database/%s/processed.bin", clientId)),
		cache:              shared.NewCache[*middleware.Stats](),
	}
}

func (qc *Query5Client) processStat(msg *middleware.StatsMsg) {
	if msg.Last {
		qc.calculatePercentile()
		qc.End()
		msg.Ack()
	}

	if qc.processedStats.Contains(int64(msg.Stats.Id)) {
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

	realFilename := fmt.Sprintf("./database/%s/stats/%d.csv", qc.clientId, msg.Stats.AppId)

	qc.commit.Write([][]string{
		{qc.clientId, strconv.Itoa(msg.Stats.Id), tmpFile.Name(), realFilename},
	})

	qc.processedStats.Add(int64(msg.Stats.Id))

	os.Rename(tmpFile.Name(), realFilename)
	qc.commit.End()

	msg.Ack()
}

func (qc *Query5Client) calculatePercentile() {
	qc.minNegativeReviews = -1

	dentries, err := os.ReadDir(fmt.Sprintf("./database/%s", qc.clientId))
	if err != nil {
		log.Errorf("failed to read directory: %v", err)
	}

	for _, dentry := range dentries {
		func() {
			file, err := os.Open(fmt.Sprintf("./database/%s/%s", qc.clientId, dentry.Name()))
			if err != nil {
				log.Errorf("failed to open file: %v", err)
			}

			defer file.Close()

			reader := csv.NewReader(file)

			for {
				record, err := reader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Errorf("Error reading file: %s", err)
					return
				}

				qc.handleRecord(record)
			}
		}()

	}

	qc.sendResult()

}

func (qc *Query5Client) handleRecord(record []string) {
	path := path.Join("client-"+qc.clientId, "stored.csv")
	stats, err := shared.ParseStat(record)
	if err != nil {
		log.Errorf("Error parsing stats: %s", err)
		return
	}

	// si ya sabemos que va a ser el ultimo, lo agregamos directamente y actualizamos el minimo
	if stats.Negatives < qc.minNegativeReviews {
		sortedFile, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE, 0666)
		if err != nil {
			log.Errorf("Error creating sorted file: %s", err)
			return
		}
		writer := csv.NewWriter(sortedFile)

		writer.Write(record)
		writer.Flush()
		sortedFile.Close()

		qc.minNegativeReviews = stats.Negatives
		return
	}

	// si no, lo agregamos a la lista ordenada
	sortedFile, err := shared.GetStoreRWriter(path)
	if err != nil {
		log.Errorf("Error creating sorted file: %s", err)
		return
	}
	defer sortedFile.Close()

	tempFile, err := os.CreateTemp("", "temp-*.csv")
	if err != nil {
		log.Errorf("Error creating temp file: %s", err)
		return
	}
	defer tempFile.Close()

	reader := csv.NewReader(sortedFile)
	writer := csv.NewWriter(tempFile)

	found := false
	for {
		storedRecord, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Errorf("Error reading file: %s", err)
			return
		}

		storedNegatives, _ := strconv.Atoi(storedRecord[4])

		if !found && stats.Negatives >= storedNegatives {
			found = true
			writer.Write(record)
		}

		writer.Write(storedRecord)
	}

	if !found {
		writer.Write(record)
	}

	writer.Flush()

	os.Rename(tempFile.Name(), path)

}

func (qc *Query5Client) sendResult() {
	path := path.Join("client-"+qc.clientId, "stored.csv")
	file, err := os.Open(path)
	if err != nil {
		log.Errorf("Error opening stored.csv: %s", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)

	result := middleware.Query5Result{
		Stats: make([]middleware.Stats, 0),
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			qc.middleware.SendResult("5", &middleware.Result{
				ClientId:       qc.clientId,
				QueryId:        5,
				IsFinalMessage: true,
				Payload:        result,
			})
			log.Infof("Sending FINAL Query 5 message")
			break
		}
		if err != nil {
			log.Errorf("Error reading stored.csv: %s", err)
			return
		}

		stats, err := shared.ParseStat(record)
		if err != nil {
			log.Errorf("Error parsing stats: %s", err)
			return
		}

		result.Stats = append(result.Stats, *stats)
		if len(result.Stats) == 50 {
			qc.middleware.SendResult("5", &middleware.Result{
				ClientId:       qc.clientId,
				QueryId:        5,
				IsFinalMessage: false,
				Payload:        result,
			})
			result.Stats = make([]middleware.Stats, 0)
		}
	}

}

func (qc *Query5Client) End() {
	// os.RemoveAll(fmt.Sprintf("./database/%s", qc.clientId))
	qc.processedStats.Close()
}
