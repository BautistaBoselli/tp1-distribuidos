package config

import (
	"fmt"

	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

var log = logging.MustGetLogger("DEBUG")

type ServerConfig struct {
	Address            string `mapstructure:"address"`
	GamesBatchAmount   int    `mapstructure:"gamesBatchAmount"`
	ReviewsBatchAmount int    `mapstructure:"reviewsBatchAmount"`
}

type LogConfig struct {
	Level string `mapstructure:"level"`
}

type MappersConfig struct {
	Amount int `mapstructure:"amount"`
}

type ShardingConfig struct {
	Amount int `mapstructure:"amount"`
}

type QueryConfig struct {
	Id             int `mapstructure:"id"`
	Shard          int `mapstructure:"shard"`
	ResultInterval int `mapstructure:"query1-result-interval"`
	MinNegatives   int `mapstructure:"query4-min-negatives"`
}

type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	Log      LogConfig      `mapstructure:"log"`
	Mappers  MappersConfig  `mapstructure:"mappers"`
	Sharding ShardingConfig `mapstructure:"sharding"`
	Query    QueryConfig    `mapstructure:"query"`
}

func InitConfig() (*Config, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.BindEnv("id", "CLI_ID")
	v.BindEnv("server.address", "CLI_SERVER_ADDRESS")
	v.BindEnv("log.level", "CLI_LOG_LEVEL")
	v.BindEnv("server.gamesBatchAmount", "CLI_GAMES_BATCH_AMOUNT")
	v.BindEnv("server.reviewsBatchAmount", "CLI_REVIEWS_BATCH_AMOUNT")
	v.BindEnv("mappers.amount", "CLI_MAPPER_AMOUNT")
	v.BindEnv("sharding.amount", "CLI_SHARDING_AMOUNT")
	v.BindEnv("query.query1-result-interval", "CLI_QUERY1_RESULT_INTERVAL")
	v.BindEnv("query.query4-min-negatives", "CLI_QUERY4_MIN_NEGATIVES")
	v.BindEnv("query.id", "CLI_QUERY_ID")
	v.BindEnv("query.shard", "CLI_SHARD_ID")

	v.SetConfigFile("./server.yml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead: %s", err)
	}

	config := Config{}
	if err := v.Unmarshal(&config); err != nil {
		return nil, err
	}

	printConfig(&config)

	return &config, nil
}

func printConfig(config *Config) {
	log.Infof("action: config | result: success | server_address: %s | log_level: %s",
		config.Server.Address,
		config.Log.Level,
	)
}
