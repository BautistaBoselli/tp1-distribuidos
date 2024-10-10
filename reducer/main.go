package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"tp1-distribuidos/config"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/reducer/reducer-queries"
	"tp1-distribuidos/shared"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func main() {
	env, err := config.InitConfig()
	if err != nil {
		log.Fatalf("action: init config | result: fail | error: %s", err)
	}

	if err := shared.InitLogger(env.Log.Level); err != nil {
		log.Fatalf("action: init logger | result: fail | error: %s", err)
	}

	middleware, err := middleware.NewMiddleware(env)
	if err != nil {
		log.Fatalf("action: creating middleware | result: error | message: %s", err)
	}

	var reduc Reducer
	log.Infof("action: creating reducer | result: success | reducer_id: %d", env.Query.Id)
	switch env.Query.Id {
	case 1:
		reduc = reducer.NewReducerQuery1(middleware)

	case 2:
		reduc = reducer.NewReducerQuery2(middleware)

	case 3:
		reduc = reducer.NewReducerQuery3(middleware)

	case 4:
		reduc = reducer.NewReducerQuery4(middleware)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		reduc.Close()
	}()

	log.Infof("action: running reducer | result: success")
	reduc.Run()

	log.Infof("action: reducer finished | result: success")
}

type Reducer interface {
	Run()
	Close()
}
