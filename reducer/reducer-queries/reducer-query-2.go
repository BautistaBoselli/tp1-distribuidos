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
		pendingAnswers: 5, // despues cambiar por middleware.ShardingAmount o algo que indique los nodos
	}
}

func (r *ReducerQuery2) Close() {
	r.middleware.Close()
}

func (r *ReducerQuery2) mergeTopGames(topGames1 []middleware.Game, topGames2 []middleware.Game) []middleware.Game {
	merged := make([]middleware.Game, 0)
	i := 0
	j := 0

	for i < len(topGames1) && j < len(topGames2) {
		if topGames1[i].AvgPlaytime > topGames2[j].AvgPlaytime {
			merged = append(merged, topGames1[i])
			i++
		} else {
			merged = append(merged, topGames2[j])
			j++
		}

		if len(merged) == topGamesSize {
			break
		}
	}

	for i < len(topGames1) {
		if len(merged) == topGamesSize {
			break
		}
		merged = append(merged, topGames1[i])
		i++
	}

	for j < len(topGames2) {
		if len(merged) == topGamesSize {
			break
		}
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
	case []middleware.Game:
		r.TopGames = r.mergeTopGames(r.TopGames, result.Payload.([]middleware.Game))
	}
}

func (r *ReducerQuery2) SendResult() {
	query2Result := &middleware.Query2Result{
		TopGames: r.TopGames,
	}

	result := &middleware.Result{
		QueryId:             2,
		IsFinalMessage:      true,
		IsFragmentedMessage: false,
		Payload:             query2Result,
	}

	err := r.middleware.SendResult("2", result)
	if err != nil {
		log.Errorf("Failed to send result: %v", err)
	}
}
