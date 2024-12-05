package reducer

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"
)

const topStatsSize = 5

type ReducerQuery3 struct {
	middleware      *middleware.Middleware
	results         chan *middleware.Result
	receivedAnswers *shared.Processed
	ClientId        string
	finished        bool
	commit          *shared.Commit
}

func NewReducerQuery3(clientId string, m *middleware.Middleware) *ReducerQuery3 {
	return &ReducerQuery3{
		middleware:      m,
		results:         make(chan *middleware.Result),
		receivedAnswers: shared.NewProcessed(fmt.Sprintf("./database/%s/received.bin", clientId)),
		ClientId:        clientId,
		commit:          shared.NewCommit(fmt.Sprintf("./database/%s/commit.csv", clientId)),
	}
}

func (r *ReducerQuery3) QueueResult(result *middleware.Result) {
	if r.finished {
		result.Ack()
		return
	}
	r.results <- result
}

func (r *ReducerQuery3) End() {
	if r.finished {
		return
	}
	r.finished = true
	if err := os.RemoveAll(fmt.Sprintf("./database/%s", r.ClientId)); err != nil {
		log.Errorf("Failed to remove directory: %v", err)
	}
	close(r.results)
}

func (r *ReducerQuery3) RestoreResult() []middleware.Stats {
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

		stats, _ := shared.ParseStat(line)
		result = append(result, *stats)
	}
}

func (r *ReducerQuery3) Run() {
	log.Infof("Reducer Query 3 running")
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

	for result := range r.results {
		r.processResult(result)
	}
}

func (r *ReducerQuery3) processResult(result *middleware.Result) {
	query3Result := result.Payload.(middleware.Query3Result)

	if r.receivedAnswers.Contains(int64(result.ShardId)) {
		log.Infof("Result %d already processed", result.Id)
		result.Ack()
		return
	}

	topStats := r.RestoreResult()
	topStats = r.mergeTopStats(topStats, query3Result.TopStats)
	tmpFile := r.storeResults(topStats)
	realFilename := fmt.Sprintf("./database/%s/3.csv", r.ClientId)

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

func (r *ReducerQuery3) storeResults(stats []middleware.Stats) *os.File {
	file, err := os.CreateTemp(fmt.Sprintf("./database/%s/", r.ClientId), "tmp-reducer-query-3.csv")
	if err != nil {
		log.Errorf("Failed to open file: %v", err)
		return nil
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	for _, stat := range stats {
		record := []string{
			strconv.Itoa(stat.AppId),
			stat.Name,
			strconv.Itoa(stat.Positives),
			strconv.Itoa(stat.Negatives),
		}

		if err := writer.Write(record); err != nil {
			log.Errorf("Failed to write line: %v", err)
			return nil
		}
	}
	writer.Flush()

	return file
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

func (r *ReducerQuery3) SendResult() {
	topStats := r.RestoreResult()
	query3Result := &middleware.Query3Result{
		TopStats: topStats,
	}

	result := &middleware.Result{
		Id:             r.getNextId(),
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

func (r *ReducerQuery3) getNextId() int64 {
	clientId, _ := strconv.Atoi(r.ClientId)

	clientIdHigh := (clientId >> 8) & 0xFF // Get high byte
	clientIdLow := clientId & 0xFF         // Get low byte

	processed := r.receivedAnswers.Count() + 1 // 0 means last

	return int64(clientIdHigh)<<56 | int64(clientIdLow)<<48 | int64(3)<<40 | int64(processed)
}
