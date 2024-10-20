package queries

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"
)

type Query5 struct {
	middleware         *middleware.Middleware
	shardId            int
	processedStats     int
	minNegativeReviews int
	cancelled          bool
}

func NewQuery5(m *middleware.Middleware, shardId int) *Query5 {
	// files, err := shared.InitStoreFiles("query-5", 100)
	// if err != nil {
	// 	log.Errorf("Error initializing store files: %s", err)
	// 	return nil
	// }

	// for _, file := range files {
	// 	file.Close()
	// }

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

func (q *Query5) Close() {
	q.cancelled = true
}

func (q *Query5) Run() {
	log.Info("Query 5 running")

	statsQueue, err := q.middleware.ListenStats(strconv.Itoa(q.shardId), "Action")
	if err != nil {
		log.Errorf("Error listening stats: %s", err)
		return
	}

	clientId := 0
	i := 0
	statsQueue.Consume(func(message *middleware.StatsMsg, ack func()) error {
		i++
		if i%25000 == 0 {
			log.Infof("Query 5 Processed %d stats", i)
		}
		q.processStats(message)
		ack()
		clientId = message.ClientId
		return nil
	})

	if q.cancelled {
		return
	}

	q.calculatePercentile(clientId)
}

func (q *Query5) processStats(message *middleware.StatsMsg) {
	clientId := strconv.Itoa(message.ClientId)
	shared.UpsertStatsFile(clientId, "query-5", 100, message.Stats)
}

func (q *Query5) calculatePercentile(clientId int) {
	q.minNegativeReviews = -1

	q.processedStats = 0
	for i := range 100 {
		func() {
			file, err := shared.GetStoreRWriter(fmt.Sprintf("client-%d/query-5-%d.csv", clientId, i))
			if err != nil {
				log.Errorf("Error opening file: %s", err)
				return
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

func (q *Query5) handleRecord(clientId int, record []string) {
	id := strconv.Itoa(clientId)
	path := path.Join("client-"+id, "stored.csv")
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

func (q *Query5) sendResult(clientId int) {
	path := path.Join("client-"+strconv.Itoa(clientId), "stored.csv")
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
				QueryId:        5,
				IsFinalMessage: false,
				Payload:        result,
			})
			result.Stats = make([]middleware.Stats, 0)
		}
	}

}
