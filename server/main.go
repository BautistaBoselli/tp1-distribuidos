package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"tp1-distribuidos/config"
	"tp1-distribuidos/shared"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func main() {
	os.Mkdir("./database", 0666)
	config, err := config.InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
	}

	if err := shared.InitLogger(config.Log.Level); err != nil {
		log.Criticalf("%s", err)
	}

	server, err := NewServer(config)

	if err != nil {
		log.Criticalf("Error creating server: %s", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
	defer stop()

	go func() {
		<-ctx.Done()
		log.Info("action: cerrar_servidor | result: in_progress")
		server.Close()
	}()

	go shared.RunUDPListener(8080)

	server.Run()

	log.Info("action: cerrar_servidor | result: success")
}
