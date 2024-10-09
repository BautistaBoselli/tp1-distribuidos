package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"tp1-distribuidos/shared"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func main() {
	shared.InitLogger("DEBUG")

	mapper, err := NewMapper()
	if err != nil {
		log.Criticalf("Error creating mapper: %s", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
	defer stop()

	go func() {
		<-ctx.Done()
		mapper.Close()
	}()

	mapper.Run()
	log.Info("action: cerrar_mapper | result: success")

}
