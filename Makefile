COMPOSE      := docker compose -f deploy/docker-compose.yml
DATABASE_URL ?= postgres://app:app@localhost:5432/streaming?sslmode=disable
RABBITMQ_URL ?= amqp://guest:guest@localhost:5672/
SERVICEBUS_CONNECTION_STRING ?= Endpoint=sb://localhost:5673;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;
PUBLISHERS   ?= rabbitmq
MIGRATIONS   := migrations

export DATABASE_URL RABBITMQ_URL SERVICEBUS_CONNECTION_STRING PUBLISHERS

.PHONY: up up-azure down logs build test test-integration tidy sqlc \
        run-order run-relay run-relay-azure run-notification run-gateway run-sb-listener \
        migrate-up migrate-down migrate-version migrate-new

## --- infrastructure ------------------------------------------------------

up:
	$(COMPOSE) up -d --wait

# Also starts the Azure Service Bus emulator (+ its SQL backend).
up-azure:
	$(COMPOSE) --profile azure up -d --wait

down:
	$(COMPOSE) --profile azure down

logs:
	$(COMPOSE) logs -f

## --- services (run each in its own terminal) ------------------------------

run-order:
	go run ./cmd/order-service

run-relay:
	go run ./cmd/outbox-relay

# Publishes every event to RabbitMQ and Azure Service Bus.
run-relay-azure:
	PUBLISHERS=rabbitmq,servicebus go run ./cmd/outbox-relay

run-notification:
	go run ./cmd/notification-service

run-gateway:
	go run ./cmd/stream-gateway

run-sb-listener:
	go run ./cmd/servicebus-listener

## --- build & test ----------------------------------------------------------

build:
	go build -o bin/ ./cmd/...

# Unit tests: fast, no Docker needed.
test:
	go test ./...

# Integration tests: throwaway Postgres/RabbitMQ via testcontainers (needs Docker).
test-integration:
	go test -tags=integration -count=1 ./...

tidy:
	go mod tidy

sqlc:
	sqlc generate

## --- migrations ------------------------------------------------------------

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
