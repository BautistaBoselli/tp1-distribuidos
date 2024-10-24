package main

import (
	"fmt"
	"os"
	"tp1-distribuidos/config"

	"github.com/spf13/viper"
)

func generateDockerCompose(sharding int, mappers int, games string, reviews string) {
	// Base structure of the docker-compose file
	composeStr := `
name: tp1
services:
  rabbitmq:
    image: rabbitmq:4-management
    container_name: rabbitmq
    ports:
      - "5672:5672"
      - "15672:15672"
    # command: ["rabbitmq-server"]
    healthcheck:
      test: ["CMD", "rabbitmqctl", "status"]
      interval: 2s
      timeout: 1s
      retries: 5
      start_period: 3s
    environment:
      RABBITMQ_DEFAULT_LOG_LEVEL: error
      RABBITMQ_LOG_LEVELS: "connection=error"`
	composeStr += `
  server:
    container_name: server
    image: server:latest
    entrypoint: /server
    environment:
      - CANT_AGENCIES=5
    volumes:
      - ./server.yml:/server.yml
    depends_on:
      rabbitmq:
        condition: service_healthy`
	for i := 1; i <= mappers; i++ {
		composeStr += fmt.Sprintf(`
      mapper-%d:
        condition: service_started`, i)
	}
	for query := 1; query <= 5; query++ {
		for i := 0; i < sharding; i++ {
			composeStr += fmt.Sprintf(`
      queries-%d-%d:
        condition: service_started`, query, i)
		}
		composeStr += fmt.Sprintf(`
      reducer-%d:
        condition: service_started`, query)
	}

	composeStr += fmt.Sprintf(`
  client1:
    container_name: client1
    image: client:latest
    entrypoint: /client
    environment:
      - CLI_ID=1
    volumes:
      - ./client.yml:/config.yml
      - ./results:/results
      - %s:/games.csv
      - %s:/reviews.csv
    depends_on:
      server:
        condition: service_started
      rabbitmq:
        condition: service_healthy`, games, reviews)

	// Generate client services
	for i := 1; i <= mappers; i++ {
		clientStr := fmt.Sprintf(`
  mapper-%d:
    container_name: mapper-%d
    image: mapper:latest
    entrypoint: /mapper
    depends_on:
      rabbitmq:
        condition: service_healthy
    volumes:
      - ./server.yml:/server.yml`, i, i)
		composeStr += clientStr
	}

	for query := 1; query <= 5; query++ {
		for i := 0; i < sharding; i++ {
			composeStr += fmt.Sprintf(`
  queries-%d-%d:
    container_name: queries-%d-%d
    image: query:latest
    entrypoint: /query
    environment:
      - CLI_QUERY_ID=%d
      - CLI_SHARD_ID=%d
    volumes:
      - ./server.yml:/server.yml
    depends_on:
      rabbitmq:
        condition: service_healthy`, query, i, query, i, query, i)
		}
	}

	for i := 1; i <= 5; i++ {
		composeStr += fmt.Sprintf(`
  reducer-%d:
    container_name: reducer-%d
    image: reducer:latest
    entrypoint: /reducer
    environment:
      - CLI_QUERY_ID=%d
    volumes:
      - ./server.yml:/server.yml
    depends_on:
      rabbitmq:
        condition: service_healthy`, i, i, i)
	}

	// Write the docker-compose file
	err := os.WriteFile("docker-compose.yml", []byte(composeStr), 0644)
	if err != nil {
		fmt.Printf("Error writing file: %v\n", err)
		os.Exit(1)
	}
}

func main() {
	config, err := config.InitConfig()
	if err != nil {
		fmt.Printf("Error initializing config: %v\n", err)
		os.Exit(1)
	}

	viper.SetConfigFile("client.yml")
	viper.ReadInConfig()

	games := viper.Get("datasets.games").(string)
	reviews := viper.Get("datasets.reviews").(string)

	generateDockerCompose(config.Sharding.Amount, config.Mappers.Amount, games, reviews)
}
