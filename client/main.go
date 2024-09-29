package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"tp1-distribuidos/client/common"
	"tp1-distribuidos/shared"

	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

var log = logging.MustGetLogger("log")

func InitConfig() (*common.Config, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.BindEnv("id", "CLI_ID")
	v.BindEnv("server.address", "CLI_SERVER_ADDRESS")
	v.BindEnv("log.level", "CLI_LOG_LEVEL")
	v.BindEnv("batch.amount", "CLI_BATCH_MAX_AMOUNT")

	v.SetConfigFile("./config.yml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead")
	}

	config := common.Config{}
	if err := v.Unmarshal(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

func PrintConfig(config *common.Config) {
	log.Infof("action: config | result: success | client: %v | server: %s | log_level: %s",
		config.ID,
		config.Server.Address,
		config.Log.Level,
	)
}

func main() {
	config, err := InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
	}

	if err := shared.InitLogger(config.Log.Level); err != nil {
		log.Criticalf("%s", err)
	}

	// Print program config with debugging purposes
	PrintConfig(config)

	// gamesFile, err := openFile(config.Datasets.Games)
	// if err != nil {
	// 	log.Criticalf("Error reading agency file: %s", err)
	// 	return
	// }

	// reviewsFile, err := openFile(config.Datasets.Reviews)
	// if err != nil {
	// 	log.Criticalf("Error reading agency file: %s", err)
	// 	return
	// }

	client := common.NewClient(*config)
	defer client.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
	defer stop()

	go func() {
		<-ctx.Done()
		client.Cancel()
	}()

	// err = client.SendBets(betsFile)
	// if err != nil {
	// 	return
	// }

	// client.GetWinners()

}

func openFile(path string) (*os.File, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return file, nil
}
