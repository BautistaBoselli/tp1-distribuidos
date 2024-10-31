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
	middleware         *middleware.Middleware
	shardId            int
	processedStats     int
	minNegativeReviews int
}

func NewQuery5(m *middleware.Middleware, shardId int) *Query5 {
	file, err := os.Create("stored.csv")
	if err != nil {
		log.Errorf("Error creating stored file: %s", err)
		return nil
	}
	file.Close()

	return &Query5{
		middleware: m,
		shardId:    shardId,
	}
}

func (q *Query5) Run() {
	log.Info("Query 5 running")

	statsQueue, err := q.middleware.ListenStats(strconv.Itoa(q.shardId), "Action")
	if err != nil {
		log.Errorf("Error listening stats: %s", err)
		return
	}

	metric := shared.NewMetric(25000, func(total int, elapsed time.Duration, rate float64) string {
		return fmt.Sprintf("[Query 5-%d] Processed %d stats in %s (%.2f stats/s)", q.shardId, total, elapsed, rate)
	})
	statsQueue.Consume(func(message *middleware.StatsMsg) error {
		metric.Update(1)

		if message.Last {
			q.calculatePercentile(message.ClientId)
			message.Ack()
			os.RemoveAll(fmt.Sprintf("./database/%s", message.ClientId))
			return nil
		}

		q.processStats(message)
		message.Ack()
		return nil
	})
}

func (q *Query5) processStats(message *middleware.StatsMsg) {
	shared.UpsertStats(message.ClientId, message.Stats)
}

func (q *Query5) calculatePercentile(clientId string) {
	q.minNegativeReviews = -1

	q.processedStats = 0
	dentries, err := os.ReadDir(fmt.Sprintf("./database/%s", clientId))
	if err != nil {
		log.Errorf("failed to read directory: %v", err)
	}

	for _, dentry := range dentries {
		func() {
			file, err := os.Open(fmt.Sprintf("./database/%s/%s", clientId, dentry.Name()))
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

				q.handleRecord(clientId, record)
			}
		}()

	}

	q.sendResult(clientId)

}

func (q *Query5) handleRecord(clientId string, record []string) {
	path := path.Join("client-"+clientId, "stored.csv")
	stats, err := shared.ParseStat(record)
	if err != nil {
		log.Errorf("Error parsing stats: %s", err)
		return
	}

	// si ya sabemos que va a ser el ultimo, lo agregamos directamente y actualizamos el minimo
	if stats.Negatives < q.minNegativeReviews {
		sortedFile, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE, 0666)
		if err != nil {
			log.Errorf("Error creating sorted file: %s", err)
			return
		}
		writer := csv.NewWriter(sortedFile)

		writer.Write(record)
		writer.Flush()
		sortedFile.Close()

		log.Infof("Appending last game: %s", stats.AppId)

		q.minNegativeReviews = stats.Negatives
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

	q.processedStats++
}

func (q *Query5) sendResult(clientId string) {
	path := path.Join("client-"+clientId, "stored.csv")
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
			q.middleware.SendResult("5", &middleware.Result{
				ClientId:       clientId,
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
			q.middleware.SendResult("5", &middleware.Result{
				ClientId:       clientId,
				QueryId:        5,
				IsFinalMessage: false,
				Payload:        result,
			})
			result.Stats = make([]middleware.Stats, 0)
		}
	}

}
