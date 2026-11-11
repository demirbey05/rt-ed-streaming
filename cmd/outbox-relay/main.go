// outbox-relay polls the outbox table and publishes events to the
// configured brokers (RabbitMQ, Azure Service Bus, or both).
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"rt-ed-streaming/internal/broker"
	"rt-ed-streaming/internal/broker/rabbitmq"
	"rt-ed-streaming/internal/broker/servicebus"
	"rt-ed-streaming/internal/config"
	"rt-ed-streaming/internal/db"
	"rt-ed-streaming/internal/httpserver"
	"rt-ed-streaming/internal/logger"
	"rt-ed-streaming/internal/relay"
)

const serviceName = "outbox-relay"

func main() {
	if err := run(); err != nil {
		os.Stderr.WriteString("fatal: " + err.Error() + "\n")
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(serviceName, "8081")
	if err != nil {
		return err
	}
	dbURL, err := config.Required("DATABASE_URL")
	if err != nil {
		return err
	}
	relayCfg, err := loadRelayConfig()
	if err != nil {
		return err
	}
	log := logger.New(cfg.ServiceName, cfg.LogLevel)

	pool, err := db.NewPool(ctx, dbURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	health := map[string]httpserver.Pinger{"postgres": pool}
	var pubs broker.Multi
	for _, name := range strings.Split(config.String("PUBLISHERS", "rabbitmq"), ",") {
		switch strings.TrimSpace(name) {
		case "rabbitmq":
			p := rabbitmq.NewPublisher(config.String("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"), log, rabbitmq.NotificationQueue)
			health["rabbitmq"] = p
			pubs = append(pubs, p)
		case "servicebus":
			conn, err := config.Required("SERVICEBUS_CONNECTION_STRING")
			if err != nil {
				return err
			}
			p, err := servicebus.NewPublisher(conn, config.String("SERVICEBUS_TOPIC", "order-events"))
			if err != nil {
				return err
			}
			pubs = append(pubs, p)
		default:
			return fmt.Errorf("unknown publisher %q (want rabbitmq, servicebus)", name)
		}
	}
	defer pubs.Close()

	var pub broker.Publisher = pubs
	if len(pubs) == 1 {
		pub = pubs[0]
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", httpserver.HealthHandler(log, health))

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return relay.New(pool, pub, log, relayCfg).Run(ctx) })
	g.Go(func() error { return httpserver.Run(ctx, log, ":"+cfg.HTTPPort, mux, cfg.ShutdownTimeout) })
	if err := g.Wait(); err != nil {
		return err
	}
	log.Info("shutdown complete")
	return nil
}

func loadRelayConfig() (relay.Config, error) {
	batch, err := config.Int("RELAY_BATCH_SIZE", 100)
	if err != nil {
		return relay.Config{}, err
	}
	poll, err := config.Duration("RELAY_POLL_INTERVAL", 500*time.Millisecond)
	if err != nil {
		return relay.Config{}, err
	}
	return relay.Config{BatchSize: batch, PollInterval: poll}, nil
}
