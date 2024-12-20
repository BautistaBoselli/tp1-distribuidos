package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"tp1-distribuidos/config"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/reducer/reducer-queries"
	"tp1-distribuidos/shared"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func createReducer(env *config.Config, clientId string, mid *middleware.Middleware) Reducer {
	if err := os.MkdirAll(fmt.Sprintf("database/%s", clientId), 0755); err != nil && !os.IsExist(err) {
		log.Errorf("Failed to create directory for client %s: %v", clientId, err)
		return nil
	}

	var reduc Reducer
	switch env.Query.Id {
	case 1:
		reduc = reducer.NewReducerQuery1(clientId, mid)
	case 2:
		reduc = reducer.NewReducerQuery2(clientId, mid)
	case 3:
		reduc = reducer.NewReducerQuery3(clientId, mid)
	case 4:
		reduc = reducer.NewReducerQuery4(clientId, mid)
	case 5:
		reduc = reducer.NewReducerQuery5(clientId, mid)
	}

	log.Infof("action: running reducer %d | result: success | client_id: %s", env.Query.Id, clientId)
	go reduc.Run()
	return reduc
}

func main() {
	os.Mkdir("./database", 0666)
	env, err := config.InitConfig()
	if err != nil {
		log.Errorf("action: init config | result: fail | error: %s", err)
	}

	if err := shared.InitLogger(env.Log.Level); err != nil {
		log.Errorf("action: init logger | result: fail | error: %s", err)
	}

	mid, err := middleware.NewMiddleware(env)
	if err != nil {
		log.Errorf("action: creating middleware | result: error | message: %s", err)
	}

	finishedClients := shared.NewFinishedClients("finished-reducer-"+strconv.Itoa(env.Query.Id), mid)

	finishedClients.Consume()

	resultsQueue, err := mid.ListenResults(strconv.Itoa(env.Query.Id))
	if err != nil {
		log.Errorf("action: listen results| result: error | message: %s", err)
		return
	}

	reducers := make(map[string]Reducer)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		log.Info("action: reducer finishing | result: in progress")
		mid.Close()
	}()

	go shared.RunUDPListener(8080)

	resultsQueue.Consume(func(msg *middleware.Result) error {
		finishedClients.Lock()
		defer finishedClients.Unlock()

		if finishedClients.Contains(msg.ClientId) {
			msg.Ack()
			return nil
		}

		if _, ok := reducers[msg.ClientId]; !ok {
			reducers[msg.ClientId] = createReducer(env, msg.ClientId, mid)
		}
		reducers[msg.ClientId].QueueResult(msg)
		return nil
	})

	log.Infof("action: reducer finished | result: success")
}

type Reducer interface {
	QueueResult(*middleware.Result)
	Run()
}
