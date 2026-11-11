// notification-service consumes order events from RabbitMQ and stores one
// notification per order, idempotently.
package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"

	"rt-ed-streaming/internal/broker/rabbitmq"
	"rt-ed-streaming/internal/config"
	"rt-ed-streaming/internal/db"
	"rt-ed-streaming/internal/httpserver"
	"rt-ed-streaming/internal/logger"
	"rt-ed-streaming/internal/notification"
)

const serviceName = "notification-service"

func main() {
	if err := run(); err != nil {
		os.Stderr.WriteString("fatal: " + err.Error() + "\n")
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(serviceName, "8082")
	if err != nil {
		return err
	}
	dbURL, err := config.Required("DATABASE_URL")
	if err != nil {
		return err
	}
	log := logger.New(cfg.ServiceName, cfg.LogLevel)

	pool, err := db.NewPool(ctx, dbURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	h := notification.NewHandler(pool, log)
	h.FailCustomerID = config.String("NOTIFY_FAIL_CUSTOMER_ID", "") // chaos hook, empty by default

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", httpserver.HealthHandler(log, map[string]httpserver.Pinger{"postgres": pool}))
	h.Register(mux)

	consumerCfg := rabbitmq.ConsumerConfig{
		URL:      config.String("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		Name:     serviceName,
		Prefetch: 10,
	}
	setup := func(ch *amqp.Channel) (string, error) {
		if err := rabbitmq.DeclareExchanges(ch); err != nil {
			return "", err
		}
		return rabbitmq.NotificationQueue.Name, rabbitmq.DeclareQueue(ch, rabbitmq.NotificationQueue)
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return rabbitmq.Consume(ctx, log, consumerCfg, setup, h.Handle) })
	g.Go(func() error { return httpserver.Run(ctx, log, ":"+cfg.HTTPPort, mux, cfg.ShutdownTimeout) })
	if err := g.Wait(); err != nil {
		return err
	}
	log.Info("shutdown complete")
	return nil
}
