COMPOSE      := docker compose -f deploy/docker-compose.yml
DATABASE_URL ?= postgres://app:app@localhost:5432/streaming?sslmode=disable

export DATABASE_URL

MIGRATIONS   := migrations

.PHONY: up down logs run-order build test tidy migrate-up migrate-down migrate-version migrate-new

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

migrate-up:
	migrate -path $(MIGRATIONS) -database "$(DATABASE_URL)" up

# Rolls back one migration at a time (safer than "down" with no limit).
migrate-down:
	migrate -path $(MIGRATIONS) -database "$(DATABASE_URL)" down 1

migrate-version:
	migrate -path $(MIGRATIONS) -database "$(DATABASE_URL)" version

# Usage: make migrate-new name=add_something
migrate-new:
	@test -n "$(name)" || (echo "usage: make migrate-new name=<name>" && exit 1)
	migrate create -ext sql -dir $(MIGRATIONS) -seq $(name)
