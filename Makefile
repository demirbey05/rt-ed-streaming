COMPOSE      := docker compose -f deploy/docker-compose.yml
DATABASE_URL ?= postgres://app:app@localhost:5432/streaming?sslmode=disable

export DATABASE_URL

.PHONY: up down logs run-order build test tidy

up:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

run-order:
	go run ./cmd/order-service

build:
	go build -o bin/ ./cmd/...

test:
	go test ./...

tidy:
	go mod tidy
