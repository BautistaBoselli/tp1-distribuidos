package reducer

import (
	"tp1-distribuidos/middleware"
)

type ReducerQuery4 struct {
	middleware     *middleware.Middleware
	pendingAnswers int
}

func NewReducerQuery4(middleware *middleware.Middleware) *ReducerQuery4 {
	return &ReducerQuery4{
		middleware:     middleware,
		pendingAnswers: middleware.Config.Sharding.Amount,
	}
}

func (r *ReducerQuery4) Close() {
	r.middleware.Close()
}

func (r *ReducerQuery4) Run() {
	defer r.Close()

	resultsQueue, err := r.middleware.ListenResults("4")
	if err != nil {
		log.Fatalf("action: listen reviews| result: error | message: %s", err)
		return
	}

	resultsQueue.Consume(func(result *middleware.Result, ack func()) error {
		r.processResult(result)

		if result.IsFinalMessage {
			r.pendingAnswers--
		}

		if r.pendingAnswers <= 0 {
			r.sendResult(&middleware.Result{
				QueryId:        4,
				IsFinalMessage: true,
				Payload: middleware.Query4Result{
					Game: "",
				},
			})
		}

		ack()

		return nil
	})

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
