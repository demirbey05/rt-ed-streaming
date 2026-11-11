// stream-gateway pushes order events to browsers in real time over SSE.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"

	"rt-ed-streaming/internal/broker"
	"rt-ed-streaming/internal/broker/rabbitmq"
	"rt-ed-streaming/internal/config"
	"rt-ed-streaming/internal/gateway"
	"rt-ed-streaming/internal/httpserver"
	"rt-ed-streaming/internal/logger"
	"rt-ed-streaming/internal/outbox"
)

const serviceName = "stream-gateway"

func main() {
	if err := run(); err != nil {
		os.Stderr.WriteString("fatal: " + err.Error() + "\n")
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(serviceName, "8083")
	if err != nil {
		return err
	}
	heartbeat, err := config.Duration("SSE_HEARTBEAT", 15*time.Second)
	if err != nil {
		return err
	}
	log := logger.New(cfg.ServiceName, cfg.LogLevel)

	hub := gateway.NewHub(log, 32)
	mux := http.NewServeMux()
	hub.Register(mux, heartbeat)

	// Each gateway instance gets its own exclusive, auto-deleted queue, so
	// every instance sees every event (fan-out) and nothing piles up while
	// no gateway is running: a live view does not need a backlog.
	setup := func(ch *amqp.Channel) (string, error) {
		if err := rabbitmq.DeclareExchanges(ch); err != nil {
			return "", err
		}
		q, err := ch.QueueDeclare("", false, true, true, false, nil)
		if err != nil {
			return "", err
		}
		return q.Name, ch.QueueBind(q.Name, "order.*", rabbitmq.ExchangeEvents, false, nil)
	}
	handle := func(_ context.Context, d amqp.Delivery) error {
		var ev outbox.Event
		if err := json.Unmarshal(d.Body, &ev); err != nil {
			return fmt.Errorf("%w: %v", broker.ErrPermanent, err)
		}
		return hub.BroadcastEvent(ev)
	}
	consumerCfg := rabbitmq.ConsumerConfig{
		URL:      config.String("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
		Name:     serviceName,
		Prefetch: 50,
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return rabbitmq.Consume(ctx, log, consumerCfg, setup, handle) })
	g.Go(func() error {
		<-ctx.Done()
		hub.Close() // end open SSE responses so graceful shutdown can finish
		return nil
	})
	g.Go(func() error { return httpserver.Run(ctx, log, ":"+cfg.HTTPPort, mux, cfg.ShutdownTimeout) })
	if err := g.Wait(); err != nil {
		return err
	}
	log.Info("shutdown complete")
	return nil
}
