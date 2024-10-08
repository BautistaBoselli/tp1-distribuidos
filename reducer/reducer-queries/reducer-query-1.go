package reducer

import (
	"tp1-distribuidos/middleware"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type ReducerQuery1 struct {
	middleware *middleware.Middleware
	// pendingAnswers  int no se si es necesario este campo para la query 1
	Windows int64
	Mac     int64
	Linux   int64
}

func NewReducerQuery1(middleware *middleware.Middleware) *ReducerQuery1 {
	return &ReducerQuery1{
		middleware: middleware,
	}
}

func (r *ReducerQuery1) Close() {
	r.middleware.Close()
}

func (r *ReducerQuery1) Run() {
	log.Infof("Reducer Query 1 running")

	resultsQueue, err := r.middleware.ListenGames("") // esto despues va a ser listenResults
	if err != nil {
		log.Fatalf("action: listen reviews| result: error | message: %s", err)
		return
	}

	resultsQueue.Consume(func(msg *middleware.GameMsg, ack func()) error {
		// r.processResult(&game)
		log.Infof("Game: %v", msg.Game)

		ack()

		return nil
	})
}

func (r *ReducerQuery1) processResult(game *middleware.Query1Result) { // despues este interface va a ser el result
	r.Windows += game.Windows
	r.Mac += game.Mac
	r.Linux += game.Linux
}

func (r *ReducerQuery1) SendResult() {
	// result := &middleware.Query1Result{
	// 	Windows: r.Windows,
	// 	Mac:     r.Mac,
	// 	Linux:   r.Linux,
	// }

	// err := r.middleware.SendQuery1Result(result)
	// if err != nil {
	// 	log.Errorf("Failed to send result: %v", err)
	// }
}
