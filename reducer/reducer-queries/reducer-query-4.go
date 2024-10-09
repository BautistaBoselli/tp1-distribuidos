package reducer

import (
	"tp1-distribuidos/middleware"
)

type ReducerQuery4 struct {
	middleware *middleware.Middleware
}

func NewReducerQuery4(middleware *middleware.Middleware) *ReducerQuery4 {
	return &ReducerQuery4{
		middleware: middleware,
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

		ack()

		return nil
	})

}

func (r *ReducerQuery4) processResult(result *middleware.Result) {
	switch result.Payload.(type) {
	case string:
		r.sendResult(result.Payload.(string), result.IsFinalMessage) // aca mandamos el juego porque la query 4 es un pasamanos nada mas
	}
}

func (r *ReducerQuery4) sendResult(game string, isFinalMessage bool) {
	query4Result := &middleware.Query4Result{
		Game: game,
	}

	result := &middleware.Result{
		QueryId:             4,
		IsFinalMessage:      isFinalMessage,
		IsFragmentedMessage: false,
		Payload:             query4Result,
	}

	err := r.middleware.SendResult("", result)
	if err != nil {
		log.Errorf("Failed to send result: %v", err)
	}
}
