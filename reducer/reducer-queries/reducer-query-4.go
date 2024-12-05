package reducer

import (
	"fmt"
	"os"
	"strconv"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"
)

type ReducerQuery4 struct {
	middleware      *middleware.Middleware
	results         chan *middleware.Result
	receivedAnswers *shared.Processed
	ClientId        string
	finished        bool
}

func NewReducerQuery4(clientId string, m *middleware.Middleware) *ReducerQuery4 {
	return &ReducerQuery4{
		middleware:      m,
		results:         make(chan *middleware.Result),
		receivedAnswers: shared.NewProcessed(fmt.Sprintf("./database/%s/received.bin", clientId)),
		ClientId:        clientId,
	}
}

func (r *ReducerQuery4) QueueResult(result *middleware.Result) {
	r.results <- result
}

func (r *ReducerQuery4) End() {
	if r.finished {
		return
	}
	r.finished = true
	os.RemoveAll(fmt.Sprintf("./database/%s", r.ClientId))
	close(r.results)
}

func (r *ReducerQuery4) Run() {
	for result := range r.results {
		log.Infof("Result received: %v", result.Payload.(middleware.Query4Result))
		shared.TestTolerance(1, 12, "Exiting after sending result")

		if result.IsFinalMessage {
			r.receivedAnswers.Add(int64(result.ShardId))
			shared.TestTolerance(1, 12, "Exiting after adding to received answers")
		} else {
			newResult := middleware.Result{
				Id:             r.getNextId(result.Payload.(middleware.Query4Result).Game),
				ClientId:       result.ClientId,
				QueryId:        4,
				IsFinalMessage: result.IsFinalMessage,
				Payload:        result.Payload,
				ShardId:        result.ShardId,
			}
			r.sendResult(&newResult)
		}

		if r.receivedAnswers.Count() == r.middleware.Config.Sharding.Amount {

			id := r.getNextId(fmt.Sprintf("Final %s", result.ShardId))
			r.sendResult(&middleware.Result{
				Id:             id,
				ClientId:       result.ClientId,
				QueryId:        4,
				IsFinalMessage: true,
				Payload: middleware.Query4Result{
					Game: "",
				},
			})
			shared.TestTolerance(1, 2, "Exiting after sending final")
			result.Ack()
			r.End()
			break
		}

		result.Ack()

	}
}

func (r *ReducerQuery4) sendResult(result *middleware.Result) {
	err := r.middleware.SendResponse(result)
	if err != nil {
		log.Errorf("Failed to send result: %v", err)
	}

	log.Infof("Reducer Game: %v for client %d", result.Payload.(middleware.Query4Result).Game, result.ClientId)
}

func (r *ReducerQuery4) getNextId(result string) int64 {
	clientId, _ := strconv.Atoi(r.ClientId)

	clientIdHigh := (clientId >> 8) & 0xFF // Get high byte
	clientIdLow := clientId & 0xFF         // Get low byte

	gameNameId := uint32(0)
	for i := 0; i < len(result); i++ {
		gameNameId += uint32([]byte(result)[i])
	}

	return int64(clientIdHigh)<<56 | int64(clientIdLow)<<48 | int64(4)<<40 | int64(gameNameId)
}
