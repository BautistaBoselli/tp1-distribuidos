package reducer

import (
	"tp1-distribuidos/middleware"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ReducerQuery1 struct {
	middleware     *middleware.Middleware
	Windows        int64
	Mac            int64
	Linux          int64
	pendingAnswers int
}

func NewReducerQuery1(middleware *middleware.Middleware) *ReducerQuery1 {
	return &ReducerQuery1{
		middleware:     middleware,
		pendingAnswers: middleware.Config.Sharding.Amount,
	}
}

func (r *ReducerQuery1) Close() {
	r.middleware.Close()
}

func (r *ReducerQuery1) Run() {
	log.Infof("Reducer Query 1 running")

	resultsQueue, err := r.middleware.ListenResults("1")
	if err != nil {
		log.Fatalf("action: listen reviews| result: error | message: %s", err)
		return
	}

	resultsQueue.Consume(func(msg *middleware.Result, ack func()) error {
		r.processResult(msg)

		ack()

		if r.pendingAnswers == 0 {
			r.SendResult(true)
		} else {
			r.SendResult(false)
		}
		return nil
	})
}

func (r *ReducerQuery1) processResult(result *middleware.Result) {
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
