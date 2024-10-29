package reducer

import (
	"tp1-distribuidos/middleware"
)

const topGamesSize = 10

type ReducerQuery2 struct {
	middleware     *middleware.Middleware
	results        chan *middleware.Result
	pendingAnswers int
	TopGames       []middleware.Game
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
	close(r.results)
	// os.RemoveAll(fmt.Sprintf("./database/%s", r.ClientId))
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

		if msg.IsFinalMessage { // en teoria todos van a ser finales pero por las dudasssss
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
		r.TopGames = r.mergeTopGames(r.TopGames, result.Payload.(middleware.Query2Result).TopGames)
	}
}

func (r *ReducerQuery2) SendResult() {
	query2Result := &middleware.Query2Result{
		TopGames: r.TopGames,
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

	for i, game := range r.TopGames {
		log.Infof("Top %d Game: %v", i+1, game)
	}

}
