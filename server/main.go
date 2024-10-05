package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"tp1-distribuidos/shared"

	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

var log = logging.MustGetLogger("log")

func InitConfig() (*Config, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.BindEnv("id", "CLI_ID")
	v.BindEnv("server.address", "CLI_SERVER_ADDRESS")
	v.BindEnv("log.level", "CLI_LOG_LEVEL")
	v.BindEnv("server.gamesBatchAmount", "CLI_GAMES_BATCH_AMOUNT")
	v.BindEnv("server.reviewsBatchAmount", "CLI_REVIEWS_BATCH_AMOUNT")

	v.SetConfigFile("./config.yml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead")
	}

	config := Config{}
	if err := v.Unmarshal(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

func PrintConfig(config *Config) {
	log.Infof("action: config | result: success | server_address: %s | log_level: %s",
		config.Server.Address,
		config.Log.Level,
	)
}

func main() {
	env, err := InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
	}

	if err := shared.InitLogger(env.Log.Level); err != nil {
		log.Criticalf("%s", err)
	}

	PrintConfig(env)

	server, err := NewServer(&env.Server)

	if err != nil {
		log.Criticalf("Error creating server: %s", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
	defer stop()

	go func() {
		<-ctx.Done()
		server.Close()
	}()

	server.Run()

	log.Info("action: cerrar_servidor | result: success")
}
