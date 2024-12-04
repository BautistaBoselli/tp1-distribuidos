package reducer

import (
	// "encoding/csv"
	// "io"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"

	// "strconv"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"
)

const resultsBatchSize = 50

type ReducerQuery5 struct {
	middleware      *middleware.Middleware
	results         chan *middleware.Result
	receivedAnswers *shared.Processed
	totalGames      int
	ClientId        string
	finished        bool
}

func NewReducerQuery5(clientId string, m *middleware.Middleware) *ReducerQuery5 {
	return &ReducerQuery5{
		middleware:      m,
		results:         make(chan *middleware.Result),
		receivedAnswers: shared.NewProcessed(fmt.Sprintf("./database/%s/received.bin", clientId)),
		totalGames:      0,
		ClientId:        clientId,
	}
}

func (r *ReducerQuery5) QueueResult(result *middleware.Result) {
	r.results <- result
}

func (r *ReducerQuery5) Close() {
	if r.finished {
		return
	}
	r.finished = true
	os.RemoveAll(fmt.Sprintf("./database/%s", r.ClientId))
	close(r.results)
}

func (r *ReducerQuery5) Run() {
	for result := range r.results {
		if err := r.processResult(result); err != nil {
			log.Errorf("action: process result | result: error | message: %s", err)
			break
		}

		result.Ack()

		if result.IsFinalMessage {
			r.receivedAnswers.Add(int64(result.ShardId))
		}

		if r.receivedAnswers.Count() == r.middleware.Config.Sharding.Amount {
			r.sendFinalResult()
			r.Close()
			break
		}

	}
}

func (r *ReducerQuery5) processResult(result *middleware.Result) error {
	tmpFile, err := os.CreateTemp(fmt.Sprintf("./database/%s/", r.ClientId), "tmp-reducer-query-5.csv")
	if err != nil {
		log.Errorf("action: create file | result: error | message: %s", err)
		return err
	}
	defer os.Remove(tmpFile.Name())

	file, err := os.OpenFile(fmt.Sprintf("./database/%s/5.csv", r.ClientId), os.O_CREATE, 0755)
	if err != nil {
		log.Errorf("action: open file | result: error | message: %s", err)
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	writer := csv.NewWriter(tmpFile)

	queryStats := result.Payload.(middleware.Query5Result).Stats

	for {
		storedRecord, err := reader.Read()
		if err != nil && err != io.EOF {
			log.Errorf("action: read file | result: error | message: %s", err)
			return err
		}
		if err == io.EOF {
			break
		}

		negatives, err := strconv.Atoi(storedRecord[2])
		if err != nil {
			log.Errorf("action: convert negative reviews to int | result: error | message: %s", err)
			return err
		}

		for _, stat := range queryStats {
			if stat.Negatives < negatives {
				break
			}
			writer.Write([]string{strconv.Itoa(stat.AppId), stat.Name, strconv.Itoa(stat.Negatives)})
			r.totalGames++ // new games
			queryStats = queryStats[1:]
		}

		writer.Write(storedRecord)
	}

	for _, stat := range queryStats {
		if stat.Negatives == 0 {
			break
		}
		writer.Write([]string{strconv.Itoa(stat.AppId), stat.Name, strconv.Itoa(stat.Negatives)})
		r.totalGames++ // new games
	}

	writer.Flush()
	// replace file with tmp file
	if err := os.Rename(tmpFile.Name(), fmt.Sprintf("./database/%s/5.csv", r.ClientId)); err != nil {
		log.Errorf("action: rename file | result: error | message: %s", err)
		return err
	}
	return nil
}

func (r *ReducerQuery5) sendFinalResult() {
	gamesNeeded := int(math.Ceil(float64(r.totalGames) / 10.0))
	log.Infof("total games: %d, games needed %v", r.totalGames, gamesNeeded)

	file, err := os.OpenFile(fmt.Sprintf("./database/%s/5.csv", r.ClientId), os.O_CREATE, 0755)
	if err != nil {
		log.Errorf("action: open file | result: error | message: %s", err)
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
			log.Errorf("action: convert appId to int | result: error | message: %s", err)
			return
		}
		negatives, err := strconv.Atoi(record[2])
		if err != nil {
			log.Errorf("action: convert negatives to int | result: error | message: %s", err)
			return
		}

		batch.Stats = append(batch.Stats, middleware.Stats{
			AppId:     appId,
			Name:      record[1],
			Negatives: negatives,
		})
		i++

		if len(batch.Stats) == resultsBatchSize {
			result := middleware.Result{
				ClientId:       r.ClientId,
				QueryId:        5,
				IsFinalMessage: false,
				Payload:        batch,
			}

			if err := r.middleware.SendResponse(&result); err != nil {
				log.Errorf("action: send final result | result: error | message: %s", err)
				break
			}
			batch.Stats = make([]middleware.Stats, 0)
		}

	}

	result := middleware.Result{
		ClientId:       r.ClientId,
		QueryId:        5,
		IsFinalMessage: true,
		Payload:        batch,
	}

	if err := r.middleware.SendResponse(&result); err != nil {
		log.Errorf("action: send final result | result: error | message: %s", err)
	}
}
