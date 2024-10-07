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

	resultsQueue, err := r.middleware.ListenGames("") // esto despues va a ser listenResults
	if err != nil {
		log.Fatalf("action: listen reviews| result: error | message: %s", err)
		return
	}

	resultsQueue.Consume(func(msg *middleware.GameBatch, ack func()) error {
		// r.processResult(&game)
		log.Infof("Game: %v", msg.Game)

		ack()

		return nil
	})

	// agregar logica despues para manejar el caso del envio final, cuando 
	// hay que cortar de escuchar mensajes
}

func (r *ReducerQuery4) processResult(game *middleware.Query4Result) { // despues este interface va a ser el result
	// r.sendResult(&game) // aca mandamos el juego porque la query 4 es un pasamanos nada mas
}

func (r *ReducerQuery4) sendResult(game *middleware.Query4Result) {
	// result := &middleware.Query4Result{
	// 	Game: game.Game,
	// }

	// r.middleware.SendQuery4Result(result)
	// if err != nil {
	// 	log.Errorf("Failed to send result: %v", err)
	// }
}
