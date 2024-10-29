package reducer

import (
	"tp1-distribuidos/middleware"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ReducerQuery1 struct {
	middleware     *middleware.Middleware
	results        chan *middleware.Result
	Windows        int64
	Mac            int64
	Linux          int64
	pendingAnswers int
	ClientId       string
	finished       bool
}

func NewReducerQuery1(clientId string, m *middleware.Middleware) *ReducerQuery1 {
	return &ReducerQuery1{
		middleware:     m,
		results:        make(chan *middleware.Result),
		pendingAnswers: m.Config.Sharding.Amount,
		ClientId:       clientId,
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
	// os.Remove(fmt.Sprintf("reducer-clients/%s/%d.txt", r.ClientId, 1))
	close(r.results)
}

func (r *ReducerQuery1) getResultsFromFile() []int {
	// file, err := os.OpenFile(fmt.Sprintf("./database/%s/%reducer-d.txt", r.ClientId, 1), os.O_RDONLY, 0644)
	// if err != nil {
	// 	log.Errorf("Failed to open file: %v", err)
	// 	return nil
	// }
	// defer file.Close()

	// reader, err := file.Read()

	// line, err := reader.ReadString('\n')
	// if err != nil {
	// 	log.Errorf("Failed to read line: %v", err)
	// 	return nil
	// }

	// parts := strings.Split(line, ",")
	// 	parts := strings.Split(line, ",")
	// 	if len(parts) != 3 {
	// 		log.Errorf("Invalid line: %s", line)
	// 		continue
	// 	}

	// 	windows, err := strconv.ParseInt(parts[0], 10, 64)
	// 	if err != nil {
	// 		log.Errorf("Failed to parse windows: %v", err)
	// 		continue
	// 	}

	// 	mac, err := strconv.ParseInt(parts[1], 10, 64)
	// 	if err != nil {
	// 		log.Errorf("Failed to parse mac: %v", err)
	// 		continue
	// 	}

	// 	linux, err := strconv.ParseInt(parts[2], 10, 64)
	// 	if err != nil {
	// 		log.Errorf("Failed to parse linux: %v", err)
	// 		continue
	// 	}

	// 	r.Windows += windows
	// 	r.Mac += mac
	// 	r.Linux += linux
	// }
	return nil
}

func (r *ReducerQuery1) Run() {
	log.Infof("Reducer Query 1 running")

	for msg := range r.results {
		r.processResult(msg)

		msg.Ack()

		if r.pendingAnswers == 0 {
			r.SendResult(true)
			// r.Close()
			// break
		} else {
			r.SendResult(false)
		}
	}
}

func (r *ReducerQuery1) processResult(result *middleware.Result) {
	// file, err := os.OpenFile(fmt.Sprintf("reducer-clients/%s/%d.txt", r.ClientId, 1), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	// if err != nil {
	// 	log.Errorf("Failed to open file: %v", err)
	// 	return
	// }
	// defer file.Close()

	// reader := file.Read()

	switch result.Payload.(type) {
	case middleware.Query1Result:
		query1Result := result.Payload.(middleware.Query1Result)
		r.Windows += query1Result.Windows
		r.Mac += query1Result.Mac
		r.Linux += query1Result.Linux

		log.Infof("Reducer Query 1: Windows: %d, Mac: %d, Linux: %d", r.Windows, r.Mac, r.Linux)
		if query1Result.Final {
			r.pendingAnswers--
		}
	}
}

func (r *ReducerQuery1) SendResult(isFinalMessage bool) {
	query1Result := &middleware.Query1Result{
		Windows: r.Windows,
		Mac:     r.Mac,
		Linux:   r.Linux,
	}

	result := &middleware.Result{
		ClientId:       r.ClientId,
		QueryId:        1,
		Payload:        query1Result,
		IsFinalMessage: isFinalMessage,
	}

	log.Infof("Reducer Query 1: Windows: %d, Mac: %d, Linux: %d, IsFinalMessage: %t", r.Windows, r.Mac, r.Linux, isFinalMessage)

	err := r.middleware.SendResponse(result)
	if err != nil {
		log.Errorf("Failed to send response: %v", err)
	}
}
