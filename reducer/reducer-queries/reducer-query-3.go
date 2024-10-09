package reducer

import (
	"tp1-distribuidos/middleware"
)

const topStatsSize = 10

type ReducerQuery3 struct {
	middleware     *middleware.Middleware
	pendingAnswers int
	TopStats       []middleware.Stats
}

func NewReducerQuery3(middleware *middleware.Middleware) *ReducerQuery3 {
	return &ReducerQuery3{
		middleware:     middleware,
		pendingAnswers: 5, // despues cambiar por middleware.ShardingAmount o algo que indique los nodos
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

	resultsQueue, err := r.middleware.ListenResults("3")
	if err != nil {
		log.Fatalf("action: listen stats | result: error | message: %s", err)
		return
	}

	resultsQueue.Consume(func(result *middleware.Result, ack func()) error { // cambiar despues por stats
		r.processResult(result)

		ack()

		if result.IsFinalMessage { // como no hay resultados parciales esto en teoria pasa siempre pero por las dudas
			r.pendingAnswers--
		}

		if r.pendingAnswers == 0 {
			r.SendResult()
		}

		return nil
	})
}

func (r *ReducerQuery3) processResult(result *middleware.Result) {
	switch result.Payload.(type) {
	case []middleware.Stats:
		r.TopStats = r.mergeTopStats(r.TopStats, result.Payload.([]middleware.Stats))
	}
}

func (r *ReducerQuery3) SendResult() {
	query3Result := &middleware.Query3Result{
		TopStats: r.TopStats,
	}

	result := &middleware.Result{
		QueryId:             3,
		IsFragmentedMessage: false,
		IsFinalMessage:      true,
		Payload:             query3Result,
	}

	err := r.middleware.SendResult("", result)
	if err != nil {
		log.Errorf("Failed to send result: %v", err)
	}
}
