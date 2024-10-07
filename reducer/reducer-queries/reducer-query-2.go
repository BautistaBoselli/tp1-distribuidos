package reducer

import (
	"tp1-distribuidos/middleware"
)

const topGamesSize = 10

type ReducerQuery2 struct {
	middleware *middleware.Middleware
	// pendingAnswers  int aca si es necesario este campo para saber cuando mandar la respuesta final
	TopGames []middleware.Game
}

func NewReducerQuery2(middleware *middleware.Middleware) *ReducerQuery2 {
	return &ReducerQuery2{
		middleware: middleware,
		// TopGames:   make([]middleware.Game, 0), esto no se por que no le gusta pero no creo que haga falta porque se recalcula siempre
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

	resultsQueue, err := r.middleware.ListenGames("") // esto despues va a ser listenResults
	if err != nil {
		log.Fatalf("action: listen reviews| result: error | message: %s", err)
		return
	}

	resultsQueue.Consume(func(msg *middleware.GameBatch, ack func()) error {
		// r.processResult(&game)
		log.Infof("Game: %v", msg.Game)

		ack()

		return nil
	})
}

func (r *ReducerQuery2) processResult(game *middleware.Query2Result) { // despues este interface va a ser el result
	r.TopGames = r.mergeTopGames(r.TopGames, game.TopGames)
}

func (r *ReducerQuery2) SendResult() {
	// result := &middleware.Query2Result{
	// 	TopGames: r.TopGames,
	// }

	// err := r.middleware.SendQuery2Result(result)
	// if err != nil {
	// 	log.Errorf("Failed to send result: %v", err)
	// }
}
