// Package servicebus implements broker.Publisher on Azure Service Bus, plus
// a subscription listener. Locally it runs against the official emulator.
package servicebus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"rt-ed-streaming/internal/outbox"
)

// Publisher sends events to a Service Bus topic. The event ID is used as
// MessageID, so with duplicate detection enabled on the topic a relay retry
// of an already-sent event is discarded by Service Bus itself.
type Publisher struct {
	client *azservicebus.Client
	sender *azservicebus.Sender
	topic  string
}

func NewPublisher(connectionString, topic string) (*Publisher, error) {
	client, err := azservicebus.NewClientFromConnectionString(connectionString, nil)
	if err != nil {
		return nil, fmt.Errorf("servicebus client: %w", err)
	}
	sender, err := client.NewSender(topic, nil)
	if err != nil {
		return nil, fmt.Errorf("servicebus sender for %s: %w", topic, err)
	}
	return &Publisher{client: client, sender: sender, topic: topic}, nil
}

func (p *Publisher) Name() string { return "servicebus" }

// Publish returns once Service Bus has accepted the message. The SDK retries
// transient errors and reconnects on its own.
func (p *Publisher) Publish(ctx context.Context, ev outbox.Event) error {
	body, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	return p.sender.SendMessage(ctx, &azservicebus.Message{
		MessageID:   ptr(ev.ID.String()),
		Subject:     ptr(ev.Type), // subscriptions can filter on it
		ContentType: ptr("application/json"),
		Body:        body,
		ApplicationProperties: map[string]any{
			"event_type":     ev.Type,
			"aggregate_type": ev.AggregateType,
			"aggregate_id":   ev.AggregateID.String(),
		},
	}, nil)
}

func (p *Publisher) Close() error {
	ctx := context.Background()
	return errors.Join(p.sender.Close(ctx), p.client.Close(ctx))
}

// Listen receives from a topic subscription until ctx is cancelled. A
// message is completed when fn returns nil and abandoned otherwise, so
// Service Bus redelivers it and dead-letters it after MaxDeliveryCount.
func Listen(ctx context.Context, log *slog.Logger, connectionString, topic, subscription string, fn func(context.Context, outbox.Event) error) error {
	client, err := azservicebus.NewClientFromConnectionString(connectionString, nil)
	if err != nil {
		return fmt.Errorf("servicebus client: %w", err)
	}
	defer client.Close(context.Background())

	receiver, err := client.NewReceiverForSubscription(topic, subscription, nil)
	if err != nil {
		return fmt.Errorf("receiver %s/%s: %w", topic, subscription, err)
	}
	defer receiver.Close(context.Background())
	log.Info("listening", "topic", topic, "subscription", subscription)

	for {
		msgs, err := receiver.ReceiveMessages(ctx, 10, nil)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return fmt.Errorf("receive: %w", err)
		}
		for _, m := range msgs {
			var ev outbox.Event
			if err := json.Unmarshal(m.Body, &ev); err != nil {
				log.Error("malformed message, dead-lettering", "message_id", m.MessageID, "error", err)
				_ = receiver.DeadLetterMessage(ctx, m, &azservicebus.DeadLetterOptions{Reason: ptr("malformed")})
				continue
			}
			if err := fn(ctx, ev); err != nil {
				log.Warn("handler failed, abandoning", "message_id", m.MessageID, "delivery_count", m.DeliveryCount, "error", err)
				_ = receiver.AbandonMessage(ctx, m, nil)
				continue
			}
			if err := receiver.CompleteMessage(ctx, m, nil); err != nil {
				log.Warn("complete failed; message will be redelivered", "message_id", m.MessageID, "error", err)
			}
		}
	}
}

func ptr[T any](v T) *T { return &v }
