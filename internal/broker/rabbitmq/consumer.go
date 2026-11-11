package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"rt-ed-streaming/internal/broker"
)

// Handler processes one delivery. The consumer acks on nil, dead-letters on
// an error wrapping broker.ErrPermanent, and requeues on any other error.
type Handler func(ctx context.Context, d amqp.Delivery) error

// SetupFunc declares the topology this consumer needs on a fresh channel
// and returns the queue to consume from. It runs on every (re)connect.
type SetupFunc func(ch *amqp.Channel) (queue string, err error)

type ConsumerConfig struct {
	URL      string
	Name     string // consumer tag, shown in the management UI
	Prefetch int    // max unacked messages in flight
	// RetryDelay is how long to wait before requeueing a failed message,
	// so a failing dependency is not hammered in a tight loop.
	RetryDelay time.Duration
}

// Consume runs until ctx is cancelled, reconnecting with exponential backoff
// whenever the connection or channel drops.
func Consume(ctx context.Context, log *slog.Logger, cfg ConsumerConfig, setup SetupFunc, h Handler) error {
	if cfg.Prefetch <= 0 {
		cfg.Prefetch = 10
	}
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = time.Second
	}

	backoff := 500 * time.Millisecond
	for {
		err := consumeOnce(ctx, log, cfg, setup, h)
		if ctx.Err() != nil {
			return nil
		}
		log.Warn("consumer disconnected, reconnecting", "retry_in", backoff.String(), "error", err)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

func consumeOnce(ctx context.Context, log *slog.Logger, cfg ConsumerConfig, setup SetupFunc, h Handler) error {
	conn, err := amqp.DialConfig(cfg.URL, amqp.Config{Heartbeat: 10 * time.Second, Dial: amqp.DefaultDial(5 * time.Second)})
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("channel: %w", err)
	}
	if err := ch.Qos(cfg.Prefetch, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}
	queue, err := setup(ch)
	if err != nil {
		return fmt.Errorf("setup topology: %w", err)
	}

	deliveries, err := ch.ConsumeWithContext(ctx, queue, cfg.Name, false /* manual ack */, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %s: %w", queue, err)
	}
	log.Info("consuming", "queue", queue, "prefetch", cfg.Prefetch)

	closed := conn.NotifyClose(make(chan *amqp.Error, 1))
	for {
		select {
		case <-ctx.Done():
			return nil
		case amqpErr := <-closed:
			return fmt.Errorf("connection closed: %v", amqpErr)
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("delivery channel closed")
			}
			settle(ctx, log, cfg, d, h(ctx, d))
		}
	}
}

// settle acks, dead-letters or requeues a delivery based on the handler result.
func settle(ctx context.Context, log *slog.Logger, cfg ConsumerConfig, d amqp.Delivery, err error) {
	l := log.With("message_id", d.MessageId, "type", d.Type)
	switch {
	case err == nil:
		_ = d.Ack(false)
	case errors.Is(err, broker.ErrPermanent):
		// requeue=false → the queue's dead-letter exchange takes it.
		l.Error("permanent failure, dead-lettering", "error", err)
		_ = d.Nack(false, false)
	default:
		// requeue=true → redelivered; the quorum queue's x-delivery-limit
		// dead-letters it once it has failed too many times.
		l.Warn("transient failure, requeueing", "error", err, "delivery_count", d.Headers["x-delivery-count"])
		select {
		case <-ctx.Done():
		case <-time.After(cfg.RetryDelay):
		}
		_ = d.Nack(false, true)
	}
}
