package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/query/queries"
	"tp1-distribuidos/shared"

	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

var log = logging.MustGetLogger("log")

type Config struct {
	ShardId  int            `mapstructure:"shardId"`
	QueryId  int            `mapstructure:"queryId"`
	Log      LogConfig      `mapstructure:"log"`
	Query1   Query1Config   `mapstructure:"query1"`
	Sharding ShardingConfig `mapstructure:"sharding"`
}

type LogConfig struct {
	Level string `mapstructure:"level"`
}

type Query1Config struct {
	ResultInterval int `mapstructure:"resultInterval"`
}

type ShardingConfig struct {
	Amount int `mapstructure:"amount"`
}

func InitConfig() (*Config, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.BindEnv("shardId", "CLI_SHARD_ID")
	v.BindEnv("queryId", "CLI_QUERY_ID")
	v.BindEnv("server.address", "CLI_SERVER_ADDRESS")
	v.BindEnv("log.level", "CLI_LOG_LEVEL")
	v.BindEnv("sharding.amount", "CLI_SHARDING_AMOUNT")
	v.BindEnv("query1.resultInterval", "CLI_QUERY1_RESULT_INTERVAL")

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
	log.Infof("action: config | result: success | log_level: %s | shard_id: %d | query_id: %d",
		config.Log.Level,
		config.ShardId,
		config.QueryId,
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

	middleware, err := middleware.NewMiddleware(env.Sharding.Amount)
	if err != nil {
		log.Criticalf("Error creating middleware: %s", err)
	}

	var query Query

	switch env.QueryId {
	case 1:
		log.Info("Running query 1")
		query = queries.NewQuery1(middleware, env.ShardId, env.Query1.ResultInterval)
	case 2:
		log.Info("Running query 2")
		query = queries.NewQuery2(middleware, env.ShardId)
	case 3:
		log.Info("Running query 3")
		query = queries.NewQuery3(middleware, env.ShardId)
	case 4:
		log.Info("Running query 4")
		query = queries.NewQuery4(middleware, env.ShardId)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
	defer stop()

	go func() {
		<-ctx.Done()
		query.Close()
	}()

	log.Info("Running query")
	query.Run()

	log.Info("action: cerrar_query | result: success")
}

type Query interface {
	Run()
	Close()
}
