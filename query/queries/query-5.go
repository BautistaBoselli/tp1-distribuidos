package queries

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"
)

type Query5 struct {
	middleware     *middleware.Middleware
	shardId        int
	processedStats int
	// gamesNeeded        int
	minNegativeReviews int
}

func NewQuery5(m *middleware.Middleware, shardId int) *Query5 {
	files, err := shared.InitStoreFiles("query-5", 100)
	if err != nil {
		log.Errorf("Error initializing store files: %s", err)
		return nil
	}

	for _, file := range files {
		file.Close()
	}

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
	q.middleware.Close()
}

func (q *Query5) Run() {
	log.Info("Query 5 running")

	statsQueue, err := q.middleware.ListenStats(strconv.Itoa(q.shardId), "Action")
	if err != nil {
		log.Errorf("Error listening stats: %s", err)
		return
	}

	i := 0
	statsQueue.Consume(func(message *middleware.StatsMsg, ack func()) error {
		i++
		if i%25000 == 0 {
			log.Infof("Query 5 Processed %d stats", i)
		}
		q.processStats(message.Stats)
		ack()
		return nil
	})

	// q.calculateGamesNeeded()
	q.calculatePercentile()

	select {}
}

func (q *Query5) processStats(message *middleware.Stats) {
	shared.UpsertStatsFile("query-5", 100, message)
}

// func (q *Query5) calculateGamesNeeded() {
// 	totalGamesWithReviews := 0
// 	for i := range 100 {
// 		func() {
// 			file, err := shared.GetStoreROnly(fmt.Sprintf("query-5-%d.csv", i))
// 			if err != nil {
// 				log.Errorf("Error opening file: %s", err)
// 				return
// 			}
// 			defer file.Close()

// 			count, err := LineCounter(file)
// 			if err != nil {
// 				log.Errorf("Error counting lines: %s", err)
// 				return
// 			}

// 			totalGamesWithReviews += count
// 		}()
// 	}

// 	q.gamesNeeded = int(math.Ceil(float64(totalGamesWithReviews) * 0.1))
// 	log.Infof("Total Games: %d - Games needed: %d", totalGamesWithReviews, q.gamesNeeded)
// }

// func lineCounter(r io.Reader) (int, error) {

// 	var count int
// 	const lineBreak = '\n'

// 	buf := make([]byte, bufio.MaxScanTokenSize)

// 	for {
// 		bufferSize, err := r.Read(buf)
// 		if err != nil && err != io.EOF {
// 			return 0, err
// 		}

// 		var buffPosition int
// 		for {
// 			i := bytes.IndexByte(buf[buffPosition:], lineBreak)
// 			if i == -1 || bufferSize == buffPosition {
// 				break
// 			}
// 			buffPosition += i + 1
// 			count++
// 		}
// 		if err == io.EOF {
// 			break
// 		}
// 	}

// 	return count, nil
// }

func (q *Query5) calculatePercentile() {
	q.minNegativeReviews = -1

	q.processedStats = 0
	for i := range 100 {
		func() {
			file, err := shared.GetStoreROnly(fmt.Sprintf("query-5-%d.csv", i))
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

				q.handleRecord(record)
			}

		}()
	}

	q.sendResult()

}

func (q *Query5) handleRecord(record []string) {
	stats, err := shared.ParseStat(record)
	if err != nil {
		log.Errorf("Error parsing stats: %s", err)
		return
	}

	// Si ya tenemos suficientes juegos y la cantidad de reviews negativas es menor a la mínima, no procesamos más
	// if q.processedStats >= q.gamesNeeded && stats.Negatives <= q.minNegativeReviews {
	// 	log.Infof("Not processing game, not enougth reviews: %d", stats.AppId)
	// 	return
	// }

	// si ya sabemos que va a ser el ultimo, lo agregamos directamente y actualizamos el minimo
	if stats.Negatives < q.minNegativeReviews {
		sortedFile, err := os.OpenFile("stored.csv", os.O_APPEND|os.O_CREATE, 0666)
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
	sortedFile, err := shared.GetStoreROnly("stored.csv")
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

	os.Rename(tempFile.Name(), "stored.csv")

	q.processedStats++
}

func (q *Query5) sendResult() {
	file, err := os.Open("stored.csv")
	if err != nil {
		log.Errorf("Error opening stored.csv: %s", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)

	result := middleware.Query5Result{
		Stats: make([]middleware.Stats, 0),
		// GamesNeeded: q.gamesNeeded,
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			q.middleware.SendResult("5", &middleware.Result{
				QueryId:        5,
				IsFinalMessage: true,
				Payload:        result,
			})
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
