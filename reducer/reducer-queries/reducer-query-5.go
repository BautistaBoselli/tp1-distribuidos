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
	middleware       *middleware.Middleware
	results          chan *middleware.Result
	processedAnswers *shared.Processed
	finalAnswers     *shared.Processed
	totalGames       int
	ClientId         string
	finished         bool
	commit           *shared.Commit
}

func NewReducerQuery5(clientId string, m *middleware.Middleware) *ReducerQuery5 {
	return &ReducerQuery5{
		middleware:       m,
		results:          make(chan *middleware.Result),
		processedAnswers: shared.NewProcessed(fmt.Sprintf("./database/%s/processed.bin", clientId)),
		finalAnswers:     shared.NewProcessed(fmt.Sprintf("./database/%s/received.bin", clientId)),
		totalGames:       0,
		ClientId:         clientId,
		commit:           shared.NewCommit(fmt.Sprintf("./database/%s/commit.csv", clientId)),
	}
}

func (r *ReducerQuery5) QueueResult(result *middleware.Result) {
	if r.finished {
		result.Ack()
		return
	}
	r.results <- result
}

func (r *ReducerQuery5) End() {
	if r.finished {
		return
	}
	r.finished = true
	os.RemoveAll(fmt.Sprintf("./database/%s", r.ClientId))
	close(r.results)
}

func (r *ReducerQuery5) Run() {
	log.Infof("Reducer Query 5 running")
	// r.RestoreResult()

	shared.RestoreCommit(fmt.Sprintf("./database/%s/commit.csv", r.ClientId), func(commit *shared.Commit) {
		log.Infof("Restored commit: %v", commit)
		//TODO
	})

	for result := range r.results {
		r.processResult(result)
	}
}

func (r *ReducerQuery5) processResult(result *middleware.Result) {
	query5Result := result.Payload.(middleware.Query5Result)

	if r.processedAnswers.Contains(int64(result.Id)) {
		log.Infof("Result %d already processed", result.Id)
		result.Ack()
		return
	}

	tmpFile := r.storeResults(query5Result.Stats)
	realFilename := fmt.Sprintf("./database/%s/query-5.csv", r.ClientId)

	r.commit.Write([][]string{
		{r.ClientId, strconv.FormatInt(result.Id, 10), tmpFile.Name(), realFilename, strconv.Itoa(result.ShardId)},
	})

	log.Infof("processed len: %d", r.processedAnswers.Count())
	r.processedAnswers.Add(int64(result.Id))

	// replace file with tmp file
	if err := os.Rename(tmpFile.Name(), realFilename); err != nil {
		log.Errorf("action: rename file | result: error | message: %s", err)
		return
	}

	if result.IsFinalMessage {
		log.Info("Received final message")
		r.finalAnswers.Add(int64(result.ShardId))
	}

	log.Info("final answers: ", r.finalAnswers.Count())
	if r.finalAnswers.Count() == r.middleware.Config.Sharding.Amount {
		r.sendFinalResult()
		r.End()
	}

	r.commit.End()

	result.Ack()
}

func (r *ReducerQuery5) storeResults(stats []middleware.Stats) *os.File {
	tmpFile, err := os.CreateTemp(fmt.Sprintf("./database/%s/", r.ClientId), "tmp-reducer-query-5.csv")
	if err != nil {
		log.Errorf("action: create file | result: error | message: %s", err)
		return nil
	}

	file, err := os.OpenFile(fmt.Sprintf("./database/%s/query-5.csv", r.ClientId), os.O_CREATE, 0755)
	if err != nil {
		log.Errorf("action: open file | result: error | message: %s", err)
		return nil
	}
	defer file.Close()

	reader := csv.NewReader(file)
	writer := csv.NewWriter(tmpFile)

	for {
		storedRecord, err := reader.Read()
		if err != nil && err != io.EOF {
			log.Errorf("action: read file | result: error | message: %s", err)
			return nil
		}
		if err == io.EOF {
			break
		}

		negatives, err := strconv.Atoi(storedRecord[2])
		if err != nil {
			log.Errorf("action: convert negative reviews to int | result: error | message: %s", err)
			return nil
		}

		for _, stat := range stats {
			if stat.Negatives < negatives {
				break
			}
			writer.Write([]string{strconv.Itoa(stat.AppId), stat.Name, strconv.Itoa(stat.Negatives)})
			r.totalGames++ // new games
			stats = stats[1:]
		}

		writer.Write(storedRecord)
	}

	for _, stat := range stats {
		if stat.Negatives == 0 {
			break
		}
		// log.Infof("writing stat %v, total games: %d", stat, r.totalGames)
		writer.Write([]string{strconv.Itoa(stat.AppId), stat.Name, strconv.Itoa(stat.Negatives)})
		r.totalGames++ // new games
	}

	writer.Flush()

	return tmpFile
}

func (r *ReducerQuery5) sendFinalResult() {
	gamesNeeded := int(math.Ceil(float64(r.totalGames) / 10.0))
	log.Infof("total games: %d, games needed %v", r.totalGames, gamesNeeded)

	file, err := os.OpenFile(fmt.Sprintf("./database/%s/query-5.csv", r.ClientId), os.O_CREATE, 0755)
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
