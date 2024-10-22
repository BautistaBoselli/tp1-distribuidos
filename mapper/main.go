package main

import (
	"context"
	"net/http"
	_"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"tp1-distribuidos/config"
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

	mapper, err := NewMapper(config)
	if err != nil {
		log.Criticalf("Error creating mapper: %s", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
	defer stop()

	go func() {
		<-ctx.Done()
		mapper.Close()
	}()

	go func() {
		http.ListenAndServe(":6060", nil)
		// if err != nil {
		// 	panic(err) // Handle error if ListenAndServe fails
		// }
	}()

	mapper.Run()
	log.Info("action: cerrar_mapper | result: success")

}
