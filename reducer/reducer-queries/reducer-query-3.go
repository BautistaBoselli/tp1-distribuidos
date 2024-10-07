package reducer

import (
	"tp1-distribuidos/middleware"
)

const topStatsSize = 10

type ReducerQuery3 struct {
	middleware *middleware.Middleware
	// pendingAnswers  int mismo caso que en reducer query 2
	TopStats []middleware.Stats
}

func NewReducerQuery3(middleware *middleware.Middleware) *ReducerQuery3 {
	return &ReducerQuery3{
		middleware: middleware,
	}
}

func (r *ReducerQuery3) Close() {
	r.middleware.Close()
}

func (r *ReducerQuery3) mergeTopStats(topStats1 []middleware.Stats, topStats2 []middleware.Stats) []middleware.Stats {
	merged := make([]middleware.Stats, 0)
	i := 0
	j := 0

	for i < len(topStats1) && j < len(topStats2) {
		if topStats1[i].Positives > topStats2[j].Positives {
			merged = append(merged, topStats1[i])
			i++
		} else {
			merged = append(merged, topStats2[j])
			j++
		}

		if len(merged) == topStatsSize {
			break
		}
	}

	for i < len(topStats1) {
		if len(merged) == topStatsSize {
			break
		}
		merged = append(merged, topStats1[i])
		i++
	}

	for j < len(topStats2) {
		if len(merged) == topStatsSize {
			break
		}
		merged = append(merged, topStats2[j])
		j++
	}

	return merged
}

func (r *ReducerQuery3) Run() {
	defer r.Close()

	resultsQueue, err := r.middleware.ListenGames("") // cambiar esto despues a listenStats
	if err != nil {
		log.Fatalf("action: listen stats | result: error | message: %s", err)
		return
	}

	resultsQueue.Consume(func(msg *middleware.GameBatch, ack func()) error { // cambiar despues por stats
		// r.processResult(msg)
		log.Infof("Stats: %v", msg)
		ack()
		return nil
	})

	// agregar logica para que despues de contar todos los finished se envie
	// la respuesta al server
}

func (r *ReducerQuery3) processResult(stats *middleware.Stats) {
	r.TopStats = append(r.TopStats, *stats)
}
	

func (r *ReducerQuery3) SendResult() {
	// result := &middleware.Query3Result{
	// 	TopStats: r.TopStats,
	// }

	// err := r.middleware.SendQuery3Result(result)
	// if err != nil {
	// 	log.Errorf("Failed to send result: %v", err)
	// }
}
