package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/reducer/reducer-queries"
	"tp1-distribuidos/shared"

	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

var log = logging.MustGetLogger("log")

type LogConfig struct {
	Level string `mapstructure:"level"`
}

type Config struct {
	Log       LogConfig
	ReducerId int
}

func InitConfig() (*Config, error) {
	v := viper.New()

	// Configue viper to read env vars with the CLI_ prefix
	v.BindEnv("query2.nodes", "CLI_QUERY2_NODES")
	v.BindEnv("reducerId", "CLI_REDUCER_ID")

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
	log.Infof("action: config | result: success | log_level: %s", config.Log.Level)
}

func main() {
	env, err := InitConfig()
	if err != nil {
		log.Fatalf("action: init config | result: fail | error: %s", err)
	}

	if err := shared.InitLogger(env.Log.Level); err != nil {
		log.Fatalf("action: init logger | result: fail | error: %s", err)
	}

	PrintConfig(env)

	middleware, err := middleware.NewMiddleware()
	if err != nil {
		log.Fatalf("action: creating middleware | result: error | message: %s", err)
	}

	var reduc Reducer
	log.Infof("action: creating reducer | result: success | reducer_id: %d", env.ReducerId)
	switch env.ReducerId {
	case 1:
		reduc = reducer.NewReducerQuery1(middleware)
	
	case 2:
		reduc = reducer.NewReducerQuery2(middleware)
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
