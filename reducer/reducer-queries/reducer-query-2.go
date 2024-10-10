package reducer

import (
	"tp1-distribuidos/middleware"
)

const topGamesSize = 10

type ReducerQuery2 struct {
	middleware     *middleware.Middleware
	pendingAnswers int
	TopGames       []middleware.Game
}

func NewReducerQuery2(middleware *middleware.Middleware) *ReducerQuery2 {
	return &ReducerQuery2{
		middleware:     middleware,
		pendingAnswers: 2, // despues cambiar por middleware.ShardingAmount o algo que indique los nodos
	}
}

func (r *ReducerQuery2) Close() {
	r.middleware.Close()
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

	resultsQueue, err := r.middleware.ListenResults("2")
	if err != nil {
		log.Fatalf("action: listen reviews| result: error | message: %s", err)
		return
	}

	resultsQueue.Consume(func(result *middleware.Result, ack func()) error {
		log.Infof("Top games: %v", result.Payload.(middleware.Query2Result))
		r.processResult(result)

		ack()

		if result.IsFinalMessage { // en teoria todos van a ser finales pero por las dudasssss
			r.pendingAnswers--
		}

		if r.pendingAnswers == 0 {
			r.SendResult()
		}

		return nil
	})
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
