SHELL := /bin/bash
.DEFAULT_GOAL := help

GO_SERVICES := ingestion query
GO_PKGS     := ./pkg/... ./services/ingestion/... ./services/query/... ./tests/stress/...
# When invoking `go` with `./...` under a workspace, wildcards must match
# module roots. We pass explicit prefixes instead.

## help: show this help
help:
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} \
		/^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

## tidy: go mod tidy for all modules
tidy:
	cd pkg && go mod tidy
	cd services/ingestion && go mod tidy
	cd services/query && go mod tidy
	cd tests/stress && go mod tidy

## build: build all Go services
build:
	@for svc in $(GO_SERVICES); do \
		echo ">> building $$svc"; \
		CGO_ENABLED=0 go build -C services/$$svc -o ../../bin/$$svc ./cmd/$$svc ; \
	done

## test: run unit tests with race detector + coverage
test:
	go test -race -count=1 -covermode=atomic -coverprofile=coverage.out $(GO_PKGS)
	@go tool cover -func=coverage.out | tail -n 1

## lint: run go vet
lint:
	go vet $(GO_PKGS)

## flink-build: build Flink aggregator uber-jar
flink-build:
	cd services/flink-aggregator && mvn -B -DskipTests package

## docker-build: build all service images
docker-build:
	docker build -t heartbeat/ingestion:dev -f services/ingestion/Dockerfile .
	docker build -t heartbeat/query:dev     -f services/query/Dockerfile     .
	docker build -t heartbeat/stress:dev    -f tests/stress/Dockerfile       .
	docker build -t heartbeat/flink-job:dev -f services/flink-aggregator/Dockerfile services/flink-aggregator

# --env-file ensures the compose file picks up .env from the repo root
# (compose's default search is the compose-file directory).
COMPOSE := docker compose --env-file .env -f deployments/docker-compose.yml

## env: copy .env.example to .env if missing
env:
	@test -f .env || cp .env.example .env
	@echo ".env ready; edit as needed."

## up: start full stack (kafka, clickhouse, flink, services) + submit job
up:
	./run-all.sh

## restart: down + up (keeps volumes)
restart:
	./restart-all.sh

## down: tear down full stack
down:
	$(COMPOSE) down -v

## logs: tail logs for the stack
logs:
	$(COMPOSE) logs -f --tail=100

## submit-flink: submit the Flink job to the running cluster
submit-flink:
	$(COMPOSE) exec flink-jobmanager \
		flink run -d /opt/flink/usrlib/flink-aggregator.jar

## stress: run the stress test against a running ingestion service
stress:
	go run ./tests/stress/cmd/stress --target=http://localhost:8080 --rps=5000 --duration=60s --devices=10000

.PHONY: help env tidy build test lint flink-build docker-build up down restart logs submit-flink stress
