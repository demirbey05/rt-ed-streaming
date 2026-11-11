// servicebus-listener stands in for an external system: it reads order
// events from an Azure Service Bus topic subscription and logs them.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"rt-ed-streaming/internal/broker/servicebus"
	"rt-ed-streaming/internal/config"
	"rt-ed-streaming/internal/logger"
	"rt-ed-streaming/internal/outbox"
)

const serviceName = "servicebus-listener"

func main() {
	if err := run(); err != nil {
		os.Stderr.WriteString("fatal: " + err.Error() + "\n")
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(serviceName, "")
	if err != nil {
		return err
	}
	conn, err := config.Required("SERVICEBUS_CONNECTION_STRING")
	if err != nil {
		return err
	}
	log := logger.New(cfg.ServiceName, cfg.LogLevel)

	return servicebus.Listen(ctx, log, conn,
		config.String("SERVICEBUS_TOPIC", "order-events"),
		config.String("SERVICEBUS_SUBSCRIPTION", "integration"),
		func(_ context.Context, ev outbox.Event) error {
			log.Info("integration event received", "event_id", ev.ID, "type", ev.Type, "aggregate_id", ev.AggregateID)
			return nil
		})
}
