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
	middleware *middleware.Middleware
	results    chan *middleware.Result
	// pendingAnswers int
	receivedAnswers *shared.Processed
	ClientId        string
	finished        bool
}

func NewReducerQuery1(clientId string, m *middleware.Middleware) *ReducerQuery1 {
	return &ReducerQuery1{
		middleware: m,
		results:    make(chan *middleware.Result),
		// pendingAnswers: m.Config.Sharding.Amount,
		receivedAnswers: shared.NewProcessed(fmt.Sprintf("./database/%s/received.bin", clientId)),
		ClientId:        clientId,
	}
}

func (r *ReducerQuery1) QueueResult(result *middleware.Result) {
	r.results <- result
}

func (r *ReducerQuery1) Close() {
	if r.finished {
		return
	}
	r.finished = true
	os.RemoveAll(fmt.Sprintf("./database/%s", r.ClientId))
	close(r.results)
}

func (r *ReducerQuery1) getResultsFromFile() []int64 {
	file, err := os.OpenFile(fmt.Sprintf("./database/%s/1.txt", r.ClientId), os.O_RDONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Errorf("Failed to open file: %v", err)
		return nil
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	line, err := reader.ReadString('\n')
	if err != nil && err.Error() != "EOF" {
		log.Errorf("Failed to read line: %v", err)
		return nil
	}
	line = strings.TrimRight(line, "\n")
	values := strings.Split(line, ",")
	if len(values) != 3 {
		return []int64{0, 0, 0}
	}

	windows, _ := strconv.Atoi(values[0])
	mac, _ := strconv.Atoi(values[1])
	linux, _ := strconv.Atoi(values[2])

	return []int64{int64(windows), int64(mac), int64(linux)}
}

func (r *ReducerQuery1) storeResults(windows int64, mac int64, linux int64) {
	file, err := os.OpenFile(fmt.Sprintf("./database/%s/1.txt", r.ClientId), os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Errorf("Failed to open file: %v", err)
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	_, err = writer.WriteString(fmt.Sprintf("%d,%d,%d\n", windows, mac, linux))
	if err != nil {
		log.Errorf("Failed to write line: %v", err)
		return
	}

	writer.Flush()
}

func (r *ReducerQuery1) Run() {
	log.Infof("Reducer Query 1 running")

	for msg := range r.results {
		r.processResult(msg)

		msg.Ack()

		if r.receivedAnswers.Count() == r.middleware.Config.Sharding.Amount {
			r.SendResult(true)
			r.Close()
			break
		} else {
			r.SendResult(false)
		}
	}
}

func (r *ReducerQuery1) processResult(result *middleware.Result) {

	switch result.Payload.(type) {
	case middleware.Query1Result:
		query1Result := result.Payload.(middleware.Query1Result)
		values := r.getResultsFromFile()
		if len(values) != 3 {
			values = []int64{0, 0, 0}
		}
		windows := values[0]
		mac := values[1]
		linux := values[2]

		windows += query1Result.Windows
		mac += query1Result.Mac
		linux += query1Result.Linux

		r.storeResults(windows, mac, linux)

		if query1Result.Final {
			r.receivedAnswers.Add(int64(result.ShardId))
		}
	}
}

func (r *ReducerQuery1) SendResult(isFinalMessage bool) {
	values := r.getResultsFromFile()
	query1Result := &middleware.Query1Result{
		Windows: values[0],
		Mac:     values[1],
		Linux:   values[2],
	}

	result := &middleware.Result{
		ClientId:       r.ClientId,
		QueryId:        1,
		Payload:        query1Result,
		IsFinalMessage: isFinalMessage,
	}

	log.Infof("Reducer Query 1: Windows: %d, Mac: %d, Linux: %d, IsFinalMessage: %t", values[0], values[1], values[2], isFinalMessage)

	err := r.middleware.SendResponse(result)
	if err != nil {
		log.Errorf("Failed to send response: %v", err)
	}
}
