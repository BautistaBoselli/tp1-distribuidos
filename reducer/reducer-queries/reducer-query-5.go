package reducer

import (
	// "encoding/csv"
	// "io"
	"encoding/csv"
	"io"
	"math"
	"os"
	"strconv"

	// "strconv"
	"tp1-distribuidos/middleware"
)

const resultsBatchSize = 50

type ReducerQuery5 struct {
	middleware          *middleware.Middleware
	pendingFinalAnswers int
	totalGames          int
}

func NewReducerQuery5(middleware *middleware.Middleware) *ReducerQuery5 {
	file, err := os.Create("reducer-query-5.csv")
	if err != nil {
		log.Fatalf("action: create file | result: error | message: %s", err)
		return nil
	}
	defer file.Close()

	return &ReducerQuery5{
		middleware:          middleware,
		pendingFinalAnswers: middleware.Config.Sharding.Amount,
		totalGames:          0,
	}
}

func (r *ReducerQuery5) Close() {
	r.middleware.Close()
}

func (r *ReducerQuery5) Run() {
	defer r.Close()

	resultsQueue, err := r.middleware.ListenResults("5")
	if err != nil {
		log.Fatalf("action: listen reviews| result: error | message: %s", err)
		return
	}

	resultsQueue.Consume(func(result *middleware.Result, ack func()) error {
		log.Infof("Result: %v", result)
		if err := r.processResult(result); err != nil {
			log.Fatalf("action: process result | result: error | message: %s", err)
			return err
		}

		ack()

		if result.IsFinalMessage {
			r.pendingFinalAnswers--
		}

		if r.pendingFinalAnswers == 0 {
			r.sendFinalResult()
		}

		return nil
	})
}

func (r *ReducerQuery5) processResult(result *middleware.Result) error {
	tmpFile, err := os.CreateTemp("", "tmp-reducer-query-5.csv")
	if err != nil {
		log.Fatalf("action: create file | result: error | message: %s", err)
		return err
	}
	defer os.Remove(tmpFile.Name())

	file, err := os.Open("reducer-query-5.csv")
	if err != nil {
		log.Fatalf("action: open file | result: error | message: %s", err)
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	writer := csv.NewWriter(tmpFile)

	queryStats := result.Payload.(middleware.Query5Result).Stats
	// gamesNeeded := result.Payload.(middleware.Query5Result).GamesNeeded
	// writtenStats := 0

	for {
		storedRecord, err := reader.Read()
		if err != nil && err != io.EOF {
			log.Fatalf("action: read file | result: error | message: %s", err)
			return err
		}
		if err == io.EOF {
			break
		}

		negatives, err := strconv.Atoi(storedRecord[2])
		if err != nil {
			log.Fatalf("action: convert negative reviews to int | result: error | message: %s", err)
			return err
		}

		for _, stat := range queryStats {
			if stat.Negatives < negatives { // || writtenStats >= gamesNeeded
				break
			}
			writer.Write([]string{strconv.Itoa(stat.AppId), stat.Name, strconv.Itoa(stat.Negatives)})
			r.totalGames++
			queryStats = queryStats[1:]
		}

		writer.Write(storedRecord)
		r.totalGames++
	}

	for _, stat := range queryStats {
		// if q.totalGames >= gamesNeeded {
		// 	break
		// }
		// log.Infof("writing stat query after for %v", stat)
		writer.Write([]string{strconv.Itoa(stat.AppId), stat.Name, strconv.Itoa(stat.Negatives)})
		r.totalGames++
	}

	writer.Flush()
	// replace file with tmp file
	if err := os.Rename(tmpFile.Name(), "reducer-query-5.csv"); err != nil {
		log.Fatalf("action: rename file | result: error | message: %s", err)
		return err
	}
	return nil
}

func (r *ReducerQuery5) sendFinalResult() {
	gamesNeeded := int(math.Ceil(float64(r.totalGames) / 10.0))
	log.Infof("total games: %d, games needed %v", r.totalGames, gamesNeeded)

	file, err := os.Open("reducer-query-5.csv")
	if err != nil {
		log.Fatalf("action: open file | result: error | message: %s", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)

	batch := middleware.Query5Result{
		Stats: make([]middleware.Stats, 0),
	}

	i := 0
	for {
		if i >= gamesNeeded {
			break
		}
		record, err := reader.Read()
		if err == io.EOF {
			break
		}

		appId, err := strconv.Atoi(record[0])
		if err != nil {
			log.Fatalf("action: convert appId to int | result: error | message: %s", err)
			return
		}
		negatives, err := strconv.Atoi(record[2])
		if err != nil {
			log.Fatalf("action: convert negatives to int | result: error | message: %s", err)
			return
		}

		batch.Stats = append(batch.Stats, middleware.Stats{
			AppId:     appId,
			Name:      record[1],
			Negatives: negatives,
		})
		i++

		if len(batch.Stats) == resultsBatchSize {
			// result := middleware.Result{
			// 	QueryId:        5,
			// 	IsFinalMessage: true,
			// 	Payload:        batch,
			// }

			// if err := r.middleware.SendResult("5", &result); err != nil {
			// 	log.Fatalf("action: send final result | result: error | message: %s", err)
			// 	break
			// }
			// log.Infof("sending result %v", result)
			batch.Stats = make([]middleware.Stats, 0)
		}

	}

	result := middleware.Result{
		QueryId:        5,
		IsFinalMessage: true,
		Payload:        batch,
	}

	// if err := r.middleware.SendResult("5", &result); err != nil {
	// 	log.Fatalf("action: send final result | result: error | message: %s", err)
	// }
	// log.Infof("sending result")
	for _, stat := range result.Payload.(middleware.Query5Result).Stats {
		log.Infof("sending stat %s: %d", stat.Name, stat.Negatives)
	}
}
