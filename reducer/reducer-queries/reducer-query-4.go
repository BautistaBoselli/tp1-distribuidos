package reducer

import (
	"fmt"
	"os"
	"tp1-distribuidos/middleware"
)

type ReducerQuery4 struct {
	middleware     *middleware.Middleware
	results        chan *middleware.Result
	pendingAnswers int
	ClientId       string
	finished       bool
}

func NewReducerQuery4(clientId string, m *middleware.Middleware) *ReducerQuery4 {
	return &ReducerQuery4{
		middleware:     m,
		results:        make(chan *middleware.Result),
		pendingAnswers: m.Config.Sharding.Amount,
		ClientId:       clientId,
	}
}

func (r *ReducerQuery4) QueueResult(result *middleware.Result) {
	r.results <- result
}

func (r *ReducerQuery4) Close() {
	if r.finished {
		return
	}
	r.finished = true
	os.RemoveAll(fmt.Sprintf("./database/%s", r.ClientId))
	close(r.results)
}

func (r *ReducerQuery4) Run() {
	for result := range r.results {
		r.processResult(result)

		if result.IsFinalMessage {
			r.pendingAnswers--
		}

		if r.pendingAnswers <= 0 {
			r.sendResult(&middleware.Result{
				ClientId:       result.ClientId,
				QueryId:        4,
				IsFinalMessage: true,
				Payload: middleware.Query4Result{
					Game: "",
				},
			})
			r.Close()
			result.Ack()
			break
		}

		result.Ack()

	}
}

func (r *ReducerQuery4) processResult(result *middleware.Result) {
	switch result.Payload.(type) {
	case middleware.Query4Result:
		r.sendResult(result)
	}
}

func (r *ReducerQuery4) sendResult(result *middleware.Result) {
	err := r.middleware.SendResponse(result)
	if err != nil {
		log.Errorf("Failed to send result: %v", err)
	}

	log.Infof("Reducer Game: %v", result.Payload.(middleware.Query4Result).Game)
}
