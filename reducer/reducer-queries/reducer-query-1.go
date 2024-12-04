package reducer

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ReducerQuery1 struct {
	middleware       *middleware.Middleware
	results          chan *middleware.Result
	processedAnswers *shared.Processed
	finalAnswers     *shared.Processed
	result           middleware.Query1Result
	commit           *shared.Commit
	ClientId         string
	finished         bool
}

func NewReducerQuery1(clientId string, m *middleware.Middleware) *ReducerQuery1 {

	return &ReducerQuery1{
		middleware:       m,
		results:          make(chan *middleware.Result),
		commit:           shared.NewCommit(fmt.Sprintf("./database/%s/commit.csv", clientId)),
		processedAnswers: shared.NewProcessed(fmt.Sprintf("./database/%s/processed.bin", clientId)),
		finalAnswers:     shared.NewProcessed(fmt.Sprintf("./database/%s/received.bin", clientId)),
		ClientId:         clientId,
	}
}

func (r *ReducerQuery1) QueueResult(result *middleware.Result) {
	if r.finished {
		result.Ack()
		return
	}
	r.results <- result
}

func (r *ReducerQuery1) End() {
	if r.finished {
		return
	}
	r.finished = true
	os.RemoveAll(fmt.Sprintf("./database/%s", r.ClientId))
	close(r.results)
}

func (r *ReducerQuery1) RestoreResult() {
	file, err := os.OpenFile(fmt.Sprintf("./database/%s/query-1.csv", r.ClientId), os.O_RDONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Errorf("Failed to open file: %v", err)
		return
	}
	defer file.Close()

	result := middleware.Query1Result{
		Windows: 0,
		Mac:     0,
		Linux:   0,
	}

	reader := bufio.NewReader(file)
	line, err := reader.ReadString('\n')

	log.Infof("Restoring line: %s", line)

	if err == nil {
		fields := strings.Split(strings.TrimSuffix(line, "\n"), ",")

		if len(fields) == 3 {
			result.Windows, _ = strconv.ParseInt(fields[0], 10, 64)
			result.Linux, _ = strconv.ParseInt(fields[1], 10, 64)
			result.Mac, _ = strconv.ParseInt(fields[2], 10, 64)
		}
	}

	r.result = result
}

func (r *ReducerQuery1) Run() {
	log.Infof("Reducer Query 1 running")
	r.RestoreResult()

	shared.RestoreCommit(fmt.Sprintf("./database/%s/commit.csv", r.ClientId), func(commit *shared.Commit) {
		log.Infof("Restored commit: %v", commit)

		os.Rename(commit.Data[0][2], commit.Data[0][3])

		id, _ := strconv.ParseInt(commit.Data[0][1], 10, 64)
		r.processedAnswers.Add(id)

		if commit.Data[0][4] == "true" {
			shardId, _ := strconv.Atoi(commit.Data[0][5])
			r.finalAnswers.Add(int64(shardId))
		}

		r.RestoreResult()

		isFinalMessage := r.finalAnswers.Count() == r.middleware.Config.Sharding.Amount
		r.SendResult(isFinalMessage)
	})

	for msg := range r.results {
		r.processResult(msg)
	}
}

func (r *ReducerQuery1) processResult(result *middleware.Result) {
	query1Result := result.Payload.(middleware.Query1Result)

	if r.processedAnswers.Contains(result.Id) {
		log.Infof("Result %d already processed", result.Id)
		result.Ack()
		return
	}

	r.result.Windows += query1Result.Windows
	r.result.Mac += query1Result.Mac
	r.result.Linux += query1Result.Linux

	tmpFile, err := os.CreateTemp(fmt.Sprintf("./database/%s", r.ClientId), "query-1.csv")
	if err != nil {
		log.Errorf("failed to create temp file: %v", err)
		return
	}

	tmpFile.WriteString(fmt.Sprintf("%d,%d,%d\n", r.result.Windows, r.result.Linux, r.result.Mac))
	realFilename := fmt.Sprintf("./database/%s/query-1.csv", r.ClientId)

	shared.TestTolerance(1, 10, "Exiting after tmp")

	r.commit.Write([][]string{
		{r.ClientId, strconv.FormatInt(result.Id, 10), tmpFile.Name(), realFilename, strconv.FormatBool(query1Result.Final), strconv.Itoa(result.ShardId)},
	})

	shared.TestTolerance(1, 10, "Exiting after creating commit")

	r.processedAnswers.Add(result.Id)

	shared.TestTolerance(1, 10, "Exiting after adding processed answer")

	os.Rename(tmpFile.Name(), realFilename)

	shared.TestTolerance(1, 10, "Exiting after renaming")

	if query1Result.Final {
		r.finalAnswers.Add(int64(result.ShardId))
	}

	shared.TestTolerance(1, 10, "Exiting after adding final answer")

	isFinalMessage := r.finalAnswers.Count() == r.middleware.Config.Sharding.Amount
	r.SendResult(isFinalMessage)

	shared.TestTolerance(1, 10, "Exiting after sending result")

	r.commit.End()

	result.Ack()

	if isFinalMessage {
		r.End()
	}
}

func (r *ReducerQuery1) SendResult(isFinalMessage bool) {

	result := &middleware.Result{
		ClientId:       r.ClientId,
		QueryId:        1,
		Payload:        r.result,
		IsFinalMessage: isFinalMessage,
	}

	if r.result.Windows == 0 && r.result.Mac == 0 && r.result.Linux == 0 {
		return
	}

	log.Infof("Reducer Query 1: Windows: %d, Mac: %d, Linux: %d, IsFinalMessage: %t", r.result.Windows, r.result.Mac, r.result.Linux, isFinalMessage)

	err := r.middleware.SendResponse(result)
	if err != nil {
		log.Errorf("Failed to send response: %v", err)
	}

}
