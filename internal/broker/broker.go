// Package broker defines the broker-agnostic publishing contract used by the
// outbox relay. Implementations live in subpackages (rabbitmq, servicebus).
package broker

import (
	"context"
	"errors"
	"fmt"

	"rt-ed-streaming/internal/outbox"
)

// Publisher delivers one event to a broker. Publish must return nil only
// once the broker has durably accepted the message (e.g. a publisher
// confirm), because the relay marks the event as published on nil.
type Publisher interface {
	Name() string
	Publish(ctx context.Context, ev outbox.Event) error
	Close() error
}

// ErrPermanent marks a message that can never be processed successfully
// (malformed body, unknown schema). Consumers dead-letter such messages
// immediately instead of retrying them.
var ErrPermanent = errors.New("permanent failure")

// Multi publishes every event to all publishers in order. It succeeds only
// if all of them do; on failure the relay retries the whole event, so some
// brokers may see it twice. That is fine: delivery is at-least-once and
// every consumer is idempotent on the event ID.
type Multi []Publisher

func (m Multi) Name() string { return "multi" }

func (m Multi) Publish(ctx context.Context, ev outbox.Event) error {
	for _, p := range m {
		if err := p.Publish(ctx, ev); err != nil {
			return fmt.Errorf("%s: %w", p.Name(), err)
		}
	}
	return nil
}

func (m Multi) Close() error {
	var errs []error
	for _, p := range m {
		errs = append(errs, p.Close())
	}
	return errors.Join(errs...)
}
