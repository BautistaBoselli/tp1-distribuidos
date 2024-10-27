package main

import (
	"fmt"
	"os"
	"tp1-distribuidos/config"
)

func generateDockerCompose(sharding int, mappers int) {
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
    networks:
      - network
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
    networks:
      - network
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
    networks:
      - network
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
    networks:
      - network
    volumes:
      - ./server.yml:/server.yml
    depends_on:
      rabbitmq:
        condition: service_healthy`, query, i, query, i, query, i)
		}
		composeStr += fmt.Sprintf(`
  reducer-%d:
    container_name: reducer-%d
    image: reducer:latest
    entrypoint: /reducer
    environment:
      - CLI_QUERY_ID=%d
    volumes:
      - ./server.yml:/server.yml
    networks:
      - network
    depends_on:
      rabbitmq:
        condition: service_healthy`, query, query, query)
	}

	composeStr += `
networks:
  network:
    driver: bridge
  `

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

	generateDockerCompose(config.Sharding.Amount, config.Mappers.Amount)
}
