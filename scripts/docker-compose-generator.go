package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"tp1-distribuidos/config"
)

const (
	SERVER_IP  = "10.5.1.1"
	MAPPER_IP  = "10.5.2.X"
	QUERY_IP   = "10.5.3.X"
	REDUCER_IP = "10.5.4.X"
	BULLY_IP   = "10.5.5.X"
	REVIVER_IP = "10.5.6.X"
)

const NAME_IP_FILE = "name_ip.csv"

func generateDockerCompose(sharding int, mappers int, reviverNodes int) {
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
      network:
        ipv4_address: 10.5.1.1
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
	for i := 1; i <= reviverNodes; i++ {
		composeStr += fmt.Sprintf(`
      reviver-%d:
        condition: service_started`, i)
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
      network:
        ipv4_address: %s
    volumes:
      - ./server.yml:/server.yml
      - ../database/mapper-%d_database:/database`, i, i, strings.Replace(MAPPER_IP, "X", fmt.Sprintf("%d", i), 1), i)
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
      network:
        ipv4_address: %s
    volumes:
      - ./server.yml:/server.yml
      - ../database/queries-%d-%d_database:/database
    depends_on:
      rabbitmq:
        condition: service_healthy`, query, i, query, i, query, i, strings.Replace(QUERY_IP, "X", fmt.Sprintf("%d%d", query, i), 1), query, i)
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
      - ../database/reducer-%d_database:/database
    networks:
      network:
        ipv4_address: %s
    depends_on:
      rabbitmq:
        condition: service_healthy`, query, query, query, query, strings.Replace(REDUCER_IP, "X", fmt.Sprintf("%d", query), 1))
	}

	for i := 1; i <= reviverNodes; i++ {
		composeStr += fmt.Sprintf(`
  reviver-%d:
    container_name: reviver-%d
    image: reviver:latest
    entrypoint: /reviver
    environment:
      - CLI_TOPOLOGY_NODES=%d
      - CLI_ID=%d
    networks:
      network:
        ipv4_address: %s
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ./server.yml:/server.yml
      - ./name_ip.csv:/name_ip.csv
    depends_on:
      rabbitmq:
        condition: service_healthy`, i, i, reviverNodes, i, strings.Replace(REVIVER_IP, "X", fmt.Sprintf("%d", i), 1))
	}

	composeStr += `
networks:
  network:
    driver: bridge
    ipam:
      config:
        - subnet: 10.5.0.0/16
  `

	// Write the docker-compose file
	err := os.WriteFile("docker-compose.yml", []byte(composeStr), 0755)
	if err != nil {
		fmt.Printf("Error writing file: %v\n", err)
		os.Exit(1)
	}

}

func generateNameIpFile(mappers int, sharding int, reviverNodes int) {
	fmt.Printf("Generating name_ip file with %d mappers, %d sharding, and %d revivers\n", mappers, sharding, reviverNodes)
	file, err := os.Create(NAME_IP_FILE)
	if err != nil {
		fmt.Printf("Error creating file: %v\n", err)
		os.Exit(1)
	}

	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write([]string{"server", SERVER_IP}); err != nil {
		fmt.Printf("Error writing to file: %v\n", err)
		os.Exit(1)
	}
	for i := 1; i <= mappers; i++ {
		if err := writer.Write([]string{fmt.Sprintf("mapper-%d", i), strings.Replace(MAPPER_IP, "X", fmt.Sprintf("%d", i), 1)}); err != nil {
			fmt.Printf("Error writing to file: %v\n", err)
			os.Exit(1)
		}
	}

	for query := 1; query <= 5; query++ {
		for i := 0; i < sharding; i++ {
			if err := writer.Write([]string{fmt.Sprintf("queries-%d-%d", query, i), strings.Replace(QUERY_IP, "X", fmt.Sprintf("%d%d", query, i), 1)}); err != nil {
				fmt.Printf("Error writing to file: %v\n", err)
				os.Exit(1)
			}
		}
		if err := writer.Write([]string{fmt.Sprintf("reducer-%d", query), strings.Replace(REDUCER_IP, "X", fmt.Sprintf("%d", query), 1)}); err != nil {
			fmt.Printf("Error writing to file: %v\n", err)
			os.Exit(1)
		}
	}

	for i := 1; i <= reviverNodes; i++ {
		if err := writer.Write([]string{fmt.Sprintf("reviver-%d", i), strings.Replace(REVIVER_IP, "X", fmt.Sprintf("%d", i), 1)}); err != nil {
			fmt.Printf("Error writing to file: %v\n", err)
			os.Exit(1)
		}
	}
}

func main() {
	config, err := config.InitConfig()
	if err != nil {
		fmt.Printf("Error initializing config: %v\n", err)
		os.Exit(1)
	}

	generateDockerCompose(config.Sharding.Amount, config.Mappers.Amount, config.Reviver.Amount)
	generateNameIpFile(config.Mappers.Amount, config.Sharding.Amount, config.Reviver.Amount)
}
