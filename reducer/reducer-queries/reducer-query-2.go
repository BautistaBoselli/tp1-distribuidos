package reducer

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"
)

const topGamesSize = 10

type ReducerQuery2 struct {
	middleware      *middleware.Middleware
	results         chan *middleware.Result
	receivedAnswers *shared.Processed
	ClientId        string
	finished        bool
	commit          *shared.Commit
}

func NewReducerQuery2(clientId string, m *middleware.Middleware) *ReducerQuery2 {

	return &ReducerQuery2{
		middleware:      m,
		results:         make(chan *middleware.Result),
		receivedAnswers: shared.NewProcessed(fmt.Sprintf("./database/%s/received.bin", clientId)),
		ClientId:        clientId,
		commit:          shared.NewCommit(fmt.Sprintf("./database/%s/commit.csv", clientId)),
	}
}

func (r *ReducerQuery2) QueueResult(result *middleware.Result) {
	if r.finished {
		result.Ack()
		return
	}
	r.results <- result
}

func (r *ReducerQuery2) End() {
	if r.finished {
		return
	}
	r.finished = true
	os.RemoveAll(fmt.Sprintf("./database/%s", r.ClientId))
	close(r.results)
}

func (r *ReducerQuery2) RestoreResult() []middleware.Game {
	file, err := os.OpenFile(fmt.Sprintf("./database/%s/2.csv", r.ClientId), os.O_RDONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Errorf("Failed to open file: %v", err)
		return nil
	}
	defer file.Close()

	result := make([]middleware.Game, 0)

	reader := csv.NewReader(file)
	for {
		line, err := reader.Read()
		if err != nil && err.Error() == "EOF" {
			return result
		}
		// if err != nil && err == csv.ErrFieldCount {
		// 	continue
		// }
		// if err != nil && err.Error() != "EOF" && err != csv.ErrFieldCount {
		// 	log.Errorf("Failed to read line: %v, line: %v", err, line)
		// 	return nil
		// }

		id, _ := strconv.Atoi(line[0])
		year, _ := strconv.Atoi(line[2])
		avgPlaytime, _ := strconv.Atoi(line[7])
		game := middleware.Game{
			AppId:       id,
			Name:        line[1],
			Year:        year,
			Windows:     false, // no nos interesan ni los generos ni las plataformas
			Mac:         false,
			Linux:       false,
			AvgPlaytime: int64(avgPlaytime),
		}
		result = append(result, game)
	}
}

func (r *ReducerQuery2) Run() {
	log.Infof("Reducer Query 2 running")
	r.RestoreResult()

	shared.RestoreCommit(fmt.Sprintf("./database/%s/commit.csv", r.ClientId), func(commit *shared.Commit) {
		log.Infof("Restored commit: %v", commit)

		os.Rename(commit.Data[0][2], commit.Data[0][3])

		id, _ := strconv.ParseInt(commit.Data[0][1], 10, 64)
		r.receivedAnswers.Add(id)

		r.RestoreResult()

		isFinalMessage := r.receivedAnswers.Count() == r.middleware.Config.Sharding.Amount
		if isFinalMessage {
			r.SendResult()
		}
	})

	for msg := range r.results {
		r.processResult(msg)
	}
}

func (r *ReducerQuery2) processResult(result *middleware.Result) {
	query2Result := result.Payload.(middleware.Query2Result)

	if r.receivedAnswers.Contains(int64(result.ShardId)) {
		log.Infof("Result %d already processed", result.Id)
		result.Ack()
		return
	}

	topGames := r.RestoreResult()
	topGames = r.mergeTopGames(topGames, query2Result.TopGames)
	tmpFile := r.storeResults(topGames)
	realFilename := fmt.Sprintf("./database/%s/2.csv", r.ClientId)

	shared.TestTolerance(1, 3, "Exiting after tmp")

	r.commit.Write([][]string{
		{r.ClientId, strconv.Itoa(result.ShardId), tmpFile.Name(), realFilename},
	})

	shared.TestTolerance(1, 3, "Exiting after creating commit")

	r.receivedAnswers.Add(int64(result.ShardId))

	shared.TestTolerance(1, 3, "Exiting after adding processed answer")

	os.Rename(tmpFile.Name(), realFilename)

	shared.TestTolerance(1, 3, "Exiting after renaming")

	isFinalMessage := r.receivedAnswers.Count() == r.middleware.Config.Sharding.Amount
	if isFinalMessage {
		r.SendResult()
	}
	shared.TestTolerance(1, 3, "Exiting after sending result")

	r.commit.End()
	result.Ack()

	if isFinalMessage {
		r.End()
	}

}

func (r *ReducerQuery2) storeResults(topGames []middleware.Game) *os.File {
	file, err := os.CreateTemp(fmt.Sprintf("./database/%s/", r.ClientId), "tmp-reducer-query-2.csv")
	if err != nil {
		log.Errorf("Failed to open file: %v", err)
		return nil
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	for _, game := range topGames {
		record := []string{
			strconv.Itoa(game.AppId),
			game.Name,
			strconv.Itoa(game.Year),
			"",
			strconv.FormatBool(game.Windows),
			strconv.FormatBool(game.Mac),
			strconv.FormatBool(game.Linux),
			strconv.FormatInt(game.AvgPlaytime, 10),
		}
		if err := writer.Write(record); err != nil {
			log.Errorf("Failed to write line: %v", err)
			return nil
		}
	}
	writer.Flush()

	return file
}

func (r *ReducerQuery2) mergeTopGames(topGames1 []middleware.Game, topGames2 []middleware.Game) []middleware.Game {
	merged := make([]middleware.Game, 0)
	i := 0
	j := 0

	for i < len(topGames1) && j < len(topGames2) && len(merged) < topGamesSize {
		if topGames1[i].AvgPlaytime > topGames2[j].AvgPlaytime {
			merged = append(merged, topGames1[i])
			i++
		} else {
			merged = append(merged, topGames2[j])
			j++
		}

	}

	for i < len(topGames1) && len(merged) < topGamesSize {
		merged = append(merged, topGames1[i])
		i++
	}

	for j < len(topGames2) && len(merged) < topGamesSize {
		merged = append(merged, topGames2[j])
		j++
	}

	return merged
}

func (r *ReducerQuery2) SendResult() {
	topGames := r.RestoreResult()
	query2Result := &middleware.Query2Result{
		TopGames: topGames,
	}

	result := &middleware.Result{
		ClientId:       r.ClientId,
		QueryId:        2,
		IsFinalMessage: true,
		Payload:        query2Result,
	}

	log.Infof("Sending result")
	err := r.middleware.SendResponse(result)
	if err != nil {
		log.Errorf("Failed to send response: %v", err)
	}

	for i, game := range topGames {
		log.Infof("Top %d Game: %v", i+1, game)
	}

}
