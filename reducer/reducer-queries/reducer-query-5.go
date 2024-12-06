package reducer

import (
	// "encoding/csv"
	// "io"
	"encoding/binary"
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
	totalFile        *os.File
}

func NewReducerQuery5(clientId string, m *middleware.Middleware) *ReducerQuery5 {
	path := fmt.Sprintf("./database/%s/query-5-total.csv", clientId)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		return nil
	}

	return &ReducerQuery5{
		middleware:       m,
		results:          make(chan *middleware.Result),
		processedAnswers: shared.NewProcessed(fmt.Sprintf("./database/%s/processed.bin", clientId)),
		finalAnswers:     shared.NewProcessed(fmt.Sprintf("./database/%s/received.bin", clientId)),
		totalGames:       0,
		ClientId:         clientId,
		commit:           shared.NewCommit(fmt.Sprintf("./database/%s/commit.csv", clientId)),
		totalFile:        file,
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

func (r *ReducerQuery5) RestoreResult() {
	path := fmt.Sprintf("./database/%s/query-5-total.csv", r.ClientId)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0777)
	if err != nil {
		return
	}

	var current int64
	err = binary.Read(file, binary.BigEndian, &current)
	if err == io.EOF {
		return
	}

	r.totalGames = int(current)
}

func (r *ReducerQuery5) Run() {
	log.Infof("Reducer Query 5 running")

	shared.RestoreCommit(fmt.Sprintf("./database/%s/commit.csv", r.ClientId), func(commit *shared.Commit) {
		log.Infof("Restored commit: %v", commit)

		os.Rename(commit.Data[0][2], commit.Data[0][3])
		os.Rename(commit.Data[0][4], commit.Data[0][5])

		id, _ := strconv.ParseInt(commit.Data[0][1], 10, 64)
		r.processedAnswers.Add(id)

		if commit.Data[0][6] == "true" {
			shardId, _ := strconv.Atoi(commit.Data[0][7])
			r.finalAnswers.Add(int64(shardId))
		}

		r.RestoreResult()

		if r.finalAnswers.Count() == r.middleware.Config.Sharding.Amount {
			r.sendFinalResult()
		}

	})

	for result := range r.results {
		log.Infof("Processing client %v id: %d, isFinal: %v", result.ClientId, result.Id, result.IsFinalMessage)
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

	tmpFile, tmpTotalFile := r.storeResults(query5Result.Stats)
	realFilename := fmt.Sprintf("./database/%s/query-5.csv", r.ClientId)
	realTotalFilename := fmt.Sprintf("./database/%s/query-5-total.csv", r.ClientId)

	shared.TestTolerance(1, 10, "Exiting after tmp")

	r.commit.Write([][]string{
		{r.ClientId, strconv.FormatInt(result.Id, 10), tmpFile.Name(), realFilename, tmpTotalFile.Name(),
			realTotalFilename, strconv.FormatBool(result.IsFinalMessage), strconv.Itoa(result.ShardId)},
	})

	shared.TestTolerance(1, 10, "Exiting after creating commit")

	r.processedAnswers.Add(int64(result.Id))

	shared.TestTolerance(1, 10, "Exiting after adding processed answer")

	// replace file with tmp file
	if err := os.Rename(tmpFile.Name(), realFilename); err != nil {
		log.Errorf("action: rename file | result: error | message: %s", err)
		return
	}

	shared.TestTolerance(1, 10, "Exiting after renaming 1")

	if err := os.Rename(tmpTotalFile.Name(), realTotalFilename); err != nil {
		log.Errorf("action: rename file | result: error | message: %s", err)
		return
	}

	shared.TestTolerance(1, 10, "Exiting after renaming 2")

	if result.IsFinalMessage {
		log.Info("Received final message")
		r.finalAnswers.Add(int64(result.ShardId))
	}

	if r.finalAnswers.Count() == r.middleware.Config.Sharding.Amount {
		r.RestoreResult()
		r.sendFinalResult()
		r.End()
	}
	r.commit.End()

	result.Ack()
}

func (r *ReducerQuery5) storeResults(stats []middleware.Stats) (*os.File, *os.File) {
	tmpFile, err := os.CreateTemp(fmt.Sprintf("./database/%s/", r.ClientId), "tmp-reducer-query-5.csv")
	if err != nil {
		log.Errorf("action: create file | result: error | message: %s", err)
		return nil, nil
	}

	file, err := os.OpenFile(fmt.Sprintf("./database/%s/query-5.csv", r.ClientId), os.O_CREATE, 0755)
	if err != nil {
		log.Errorf("action: open file | result: error | message: %s", err)
		return nil, nil
	}
	defer file.Close()

	reader := csv.NewReader(file)
	writer := csv.NewWriter(tmpFile)

	totalGames := 0

	for {
		storedRecord, err := reader.Read()
		if err != nil && err != io.EOF {
			log.Errorf("action: read file | result: error | message: %s", err)
			return nil, nil
		}
		if err == io.EOF {
			break
		}

		negatives, err := strconv.Atoi(storedRecord[2])
		if err != nil {
			log.Errorf("action: convert negative reviews to int | result: error | message: %s", err)
			return nil, nil
		}

		for _, stat := range stats {
			if stat.Negatives < negatives {
				break
			}
			writer.Write([]string{strconv.Itoa(stat.AppId), stat.Name, strconv.Itoa(stat.Negatives)})
			totalGames++ // new games
			stats = stats[1:]
		}

		writer.Write(storedRecord)
	}

	for _, stat := range stats {
		if stat.Negatives == 0 {
			break
		}
		writer.Write([]string{strconv.Itoa(stat.AppId), stat.Name, strconv.Itoa(stat.Negatives)})
		totalGames++ // new games
	}

	writer.Flush()

	tmpTotalFile, err := os.CreateTemp(fmt.Sprintf("./database/%s/", r.ClientId), "tmp-total-reducer-query-5.csv")
	if err != nil {
		log.Errorf("action: create file | result: error | message: %s", err)
		return nil, nil
	}

	totalFile, err := os.OpenFile(fmt.Sprintf("./database/%s/query-5-total.csv", r.ClientId), os.O_CREATE, 0755)
	if err != nil {
		log.Errorf("action: open file | result: error | message: %s", err)
		return nil, nil
	}
	defer file.Close()

	var current int64
	err = binary.Read(totalFile, binary.BigEndian, &current)
	if err != io.EOF && err != nil {
		return nil, nil
	}

	newTotal := current + int64(totalGames)

	err = binary.Write(tmpTotalFile, binary.BigEndian, newTotal)
	if err != nil {
		log.Errorf("failed to write to file: %v", err)
		return nil, nil
	}

	return tmpFile, tmpTotalFile
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
				Id:             r.getNextId(&batch),
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
		Id:             r.getNextId(&batch),
		ClientId:       r.ClientId,
		QueryId:        5,
		IsFinalMessage: true,
		Payload:        batch,
	}

	shared.TestTolerance(1, 2, "Exiting before sending final")

	if err := r.middleware.SendResponse(&result); err != nil {
		log.Errorf("action: send final result | result: error | message: %s", err)
	}
}

func (r *ReducerQuery5) getNextId(batch *middleware.Query5Result) int64 {
	clientId, _ := strconv.Atoi(r.ClientId)

	clientIdHigh := (clientId >> 8) & 0xFF // Get high byte
	clientIdLow := clientId & 0xFF         // Get low byte

	negatives := 0
	for _, stat := range batch.Stats {
		negatives += stat.Negatives
	}

	return int64(clientIdHigh)<<56 | int64(clientIdLow)<<48 | int64(5)<<40 | int64(negatives)
}
