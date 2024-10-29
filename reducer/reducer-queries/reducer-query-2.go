package reducer

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"tp1-distribuidos/middleware"
)

const topGamesSize = 10

type ReducerQuery2 struct {
	middleware     *middleware.Middleware
	results        chan *middleware.Result
	pendingAnswers int
	ClientId       string
	finished       bool
}

func NewReducerQuery2(clientId string, m *middleware.Middleware) *ReducerQuery2 {

	return &ReducerQuery2{
		middleware:     m,
		results:        make(chan *middleware.Result),
		pendingAnswers: m.Config.Sharding.Amount,
		ClientId:       clientId,
	}
}

func (r *ReducerQuery2) QueueResult(result *middleware.Result) {
	r.results <- result
}

func (r *ReducerQuery2) Close() {
	if r.finished {
		return
	}
	r.finished = true
	log.Infof("Reducer Query 2 closing")
	os.RemoveAll(fmt.Sprintf("./database/%s", r.ClientId))
	close(r.results)
}

func (r *ReducerQuery2) getResultsFromFile() []middleware.Game {
	file, err := os.OpenFile(fmt.Sprintf("./database/%s/2.csv", r.ClientId), os.O_RDONLY|os.O_CREATE, 0644)
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
		if err != nil && err == csv.ErrFieldCount {
			continue
		}
		if err != nil && err.Error() != "EOF" && err != csv.ErrFieldCount {
			log.Errorf("Failed to read line: %v, line: %v", err, line)
			return nil
		}

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

func (r *ReducerQuery2) storeResults(topGames []middleware.Game) {
	file, err := os.CreateTemp("", "tmp-reducer-query-2.csv")
	// file, err := os.OpenFile(fmt.Sprintf("./database/%s/2.csv", r.ClientId), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("Failed to open file: %v", err)
		return
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
			return
		}
	}
	writer.Flush()

	os.Rename(file.Name(), fmt.Sprintf("./database/%s/2.csv", r.ClientId))
	os.Remove(file.Name())
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

func (r *ReducerQuery2) Run() {
	log.Infof("Reducer Query 2 running")

	for msg := range r.results {
		log.Infof("Top games: %v", msg.Payload.(middleware.Query2Result))
		r.processResult(msg)

		msg.Ack()

		if msg.IsFinalMessage {
			r.pendingAnswers--
		}

		if r.pendingAnswers == 0 {
			r.SendResult()
			r.Close()
			break
		}

	}
}

func (r *ReducerQuery2) processResult(result *middleware.Result) {
	switch result.Payload.(type) {
	case middleware.Query2Result:
		topGames := r.getResultsFromFile()
		topGames = r.mergeTopGames(topGames, result.Payload.(middleware.Query2Result).TopGames)
		r.storeResults(topGames)
	}
}

func (r *ReducerQuery2) SendResult() {
	topGames := r.getResultsFromFile()
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
