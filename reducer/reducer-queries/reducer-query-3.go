package reducer

import (
	"tp1-distribuidos/middleware"
)

const topStatsSize = 5

type ReducerQuery3 struct {
	middleware     *middleware.Middleware
	results        chan *middleware.Result
	pendingAnswers int
	TopStats       []middleware.Stats
	ClientId       string
	finished       bool
}

func NewReducerQuery3(clientId string, m *middleware.Middleware) *ReducerQuery3 {
	return &ReducerQuery3{
		middleware:     m,
		results:        make(chan *middleware.Result),
		pendingAnswers: m.Config.Sharding.Amount,
		ClientId:       clientId,
	}
}

func (r *ReducerQuery3) QueueResult(result *middleware.Result) {
	r.results <- result
}

func (r *ReducerQuery3) Close() {
	if r.finished {
		return
	}
	r.finished = true
	close(r.results)
	// os.RemoveAll(fmt.Sprintf("./database/%s", r.ClientId))
}

func (r *ReducerQuery3) mergeTopStats(topStats1 []middleware.Stats, topStats2 []middleware.Stats) []middleware.Stats {
	merged := make([]middleware.Stats, 0)
	i := 0
	j := 0

	for i < len(topStats1) && j < len(topStats2) && len(merged) < topStatsSize {
		if topStats1[i].Positives > topStats2[j].Positives {
			merged = append(merged, topStats1[i])
			i++
		} else {
			merged = append(merged, topStats2[j])
			j++
		}
	}

	for i < len(topStats1) && len(merged) < topStatsSize {
		merged = append(merged, topStats1[i])
		i++
	}

	for j < len(topStats2) && len(merged) < topStatsSize {
		merged = append(merged, topStats2[j])
		j++
	}

	return merged
}

func (r *ReducerQuery3) Run() {
	for result := range r.results {
		r.processResult(result)

		if result.IsFinalMessage { // como no hay resultados parciales esto en teoria pasa siempre pero por las dudas
			r.pendingAnswers--
		}

		if r.pendingAnswers == 0 {
			r.SendResult()
		}

		result.Ack()

	}
}

func (r *ReducerQuery3) processResult(result *middleware.Result) {
	switch result.Payload.(type) {
	case middleware.Query3Result:
		r.TopStats = r.mergeTopStats(r.TopStats, result.Payload.(middleware.Query3Result).TopStats)
	}
}

func (r *ReducerQuery3) SendResult() {
	query3Result := &middleware.Query3Result{
		TopStats: r.TopStats,
	}

	result := &middleware.Result{
		ClientId:       r.ClientId,
		QueryId:        3,
		IsFinalMessage: true,
		Payload:        query3Result,
	}

	for i, stat := range r.TopStats {
		log.Infof("Top %d Stat: %v", i+1, stat)
	}

	err := r.middleware.SendResponse(result)
	if err != nil {
		log.Errorf("Failed to send result: %v", err)
	}
}
