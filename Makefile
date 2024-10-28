SHELL := /bin/bash
PWD := $(shell pwd)

# GIT_REMOTE = github.com/7574-sistemas-distribuidos/docker-compose-init

default: build

all:

# deps:
# 	go mod tidy
# 	go mod vendor

# build: deps
# 	GOOS=linux go build -o bin/client github.com/7574-sistemas-distribuidos/docker-compose-init/client
# .PHONY: build

docker-image:
	docker build -f ./server/Dockerfile -t "server:latest" .
	docker build -f ./mapper/Dockerfile -t "mapper:latest" .
	docker build -f ./query/Dockerfile -t "query:latest" .
	docker build -f ./reducer/Dockerfile -t "reducer:latest" .
	# Execute this command from time to time to clean up intermediate stages generated 
	# during client build (your hard drive will like this :) ). Don't left uncommented if you 
	# want to avoid rebuilding client image every time the docker-compose-up command 
	# is executed, even when client code has not changed
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
.PHONY: docker-image

docker-compose-up: docker-image
	docker compose up --build
.PHONY: docker-compose-up

docker-compose-down:
	docker compose stop -t 20
	docker compose down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose logs -f
.PHONY: docker-compose-logs

# Entradas para el cliente

client-up:
	docker compose -f docker-compose-clients.yml up --build

client-down:
	docker compose -f docker-compose-clients.yml stop -t 20
	docker compose -f docker-compose-clients.yml down

client-logs:
	docker compose logs -f client

.PHONY: client-up client-down client-logs