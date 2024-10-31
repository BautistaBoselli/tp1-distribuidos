package reducer

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"tp1-distribuidos/middleware"
)

const topStatsSize = 5

type ReducerQuery3 struct {
	middleware     *middleware.Middleware
	results        chan *middleware.Result
	pendingAnswers int
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
	if err := os.RemoveAll(fmt.Sprintf("./database/%s", r.ClientId)); err != nil {
		log.Errorf("Failed to remove directory: %v", err)
	}
	close(r.results)
}

func (r *ReducerQuery3) getResultsFromFile() []middleware.Stats {
	file, err := os.OpenFile(fmt.Sprintf("./database/%s/3.csv", r.ClientId), os.O_RDONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Errorf("Failed to open file: %v", err)
		return nil
	}
	defer file.Close()

	result := make([]middleware.Stats, 0)

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
		positives, _ := strconv.Atoi(line[4])
		negatives, _ := strconv.Atoi(line[5])
		stats := middleware.Stats{
			AppId:     id,
			Name:      line[1],
			Positives: positives,
			Negatives: negatives,
		}
		result = append(result, stats)
	}
}

func (r *ReducerQuery3) storeResults(stats []middleware.Stats) {
	file, err := os.CreateTemp(fmt.Sprintf("./database/%s/", r.ClientId), "tmp-reducer-query-3.csv")
	// file, err := os.OpenFile(fmt.Sprintf("./database/%s/3.csv", r.ClientId), os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Errorf("Failed to open file: %v", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	for _, stat := range stats {
		record := []string{
			strconv.Itoa(stat.AppId),
			stat.Name,
			"text",
			"genre",
			strconv.Itoa(stat.Positives),
			strconv.Itoa(stat.Negatives),
		}

		if err := writer.Write(record); err != nil {
			log.Errorf("Failed to write line: %v", err)
			return
		}
	}
	writer.Flush()

	os.Rename(file.Name(), fmt.Sprintf("./database/%s/3.csv", r.ClientId))
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

		if result.IsFinalMessage {
			r.pendingAnswers--
		}

		if r.pendingAnswers == 0 {
			r.SendResult()
			r.Close()
			result.Ack()
			break
		}

		result.Ack()

	}
}

func (r *ReducerQuery3) processResult(result *middleware.Result) {
	switch result.Payload.(type) {
	case middleware.Query3Result:
		topStats := r.getResultsFromFile()
		topStats = r.mergeTopStats(topStats, result.Payload.(middleware.Query3Result).TopStats)
		log.Infof("Top stats: %v", topStats)
		r.storeResults(topStats)
	}
}

func (r *ReducerQuery3) SendResult() {
	topStats := r.getResultsFromFile()
	query3Result := &middleware.Query3Result{
		TopStats: topStats,
	}

	result := &middleware.Result{
		ClientId:       r.ClientId,
		QueryId:        3,
		IsFinalMessage: true,
		Payload:        query3Result,
	}

	for i, stat := range topStats {
		log.Infof("Top %d Stat: %v", i+1, stat)
	}

	err := r.middleware.SendResponse(result)
	if err != nil {
		log.Errorf("Failed to send result: %v", err)
	}

}
