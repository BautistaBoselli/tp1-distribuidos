package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"tp1-distribuidos/config"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/query/queries"
	"tp1-distribuidos/shared"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func main() {
	config, err := config.InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
	}

	if err := shared.InitLogger(config.Log.Level); err != nil {
		log.Criticalf("%s", err)
	}

	middleware, err := middleware.NewMiddleware(config)
	if err != nil {
		log.Criticalf("Error creating middleware: %s", err)
	}

	var query Query

	switch config.Query.Id {
	case 1:
		log.Info("Running query 1")
		query = queries.NewQuery1(middleware, config.Query.Shard, config.Query.ResultInterval)
	case 2:
		log.Info("Running query 2")
		query = queries.NewQuery2(middleware, config.Query.Shard)
	case 3:
		log.Info("Running query 3")
		query = queries.NewQuery3(middleware, config.Query.Shard)
	case 4:
		log.Info("Running query 4")
		query = queries.NewQuery4(middleware, config.Query.Shard)
	case 5:
		log.Info("Running query 5")
		query = queries.NewQuery5(middleware, config.Query.Shard)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
	defer stop()

	go func() {
		<-ctx.Done()
		log.Info("action: cancelar_query | result: in_progress")
		middleware.Close()
	}()

	log.Info("Running query")
	query.Run()

	log.Info("action: cerrar_query | result: success")
}

type Query interface {
	Run()
}
