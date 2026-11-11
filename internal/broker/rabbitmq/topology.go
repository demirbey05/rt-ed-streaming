// Package rabbitmq implements the broker.Publisher and a reconnecting
// consumer on top of RabbitMQ (AMQP 0-9-1).
package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// ExchangeEvents receives every domain event; the routing key is the
	// event type (e.g. "order.created"), so consumers bind with patterns
	// like "order.*".
	ExchangeEvents = "orders.events"
	// ExchangeDLX receives messages that were rejected or exceeded their
	// delivery limit. Each queue routes to its own "<queue>.dlq".
	ExchangeDLX = "orders.dlx"
)

// QueueSpec describes a durable consumer queue and its dead-letter queue.
type QueueSpec struct {
	Name        string
	BindingKeys []string
	// DeliveryLimit is how many times a message may be redelivered before
	// RabbitMQ dead-letters it (quorum queue x-delivery-limit).
	DeliveryLimit int
}

func (q QueueSpec) DLQName() string { return q.Name + ".dlq" }

// NotificationQueue is the durable queue of notification-service. The relay
// declares it too, so events published before the consumer ever started
// are routed and kept, not dropped as unroutable.
var NotificationQueue = QueueSpec{
	Name:          "notification.order-events",
	BindingKeys:   []string{"order.*"},
	DeliveryLimit: 5,
}

// DeclareExchanges declares the event exchange and the dead-letter exchange.
// Declarations are idempotent, so every service can call this on connect.
func DeclareExchanges(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(ExchangeEvents, amqp.ExchangeTopic, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange %s: %w", ExchangeEvents, err)
	}
	if err := ch.ExchangeDeclare(ExchangeDLX, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange %s: %w", ExchangeDLX, err)
	}
	return nil
}

// DeclareQueue declares a quorum queue with a delivery limit and a
// dead-letter queue, and binds it to the event exchange.
func DeclareQueue(ch *amqp.Channel, q QueueSpec) error {
	if _, err := ch.QueueDeclare(q.DLQName(), true, false, false, false, amqp.Table{
		amqp.QueueTypeArg: amqp.QueueTypeQuorum,
	}); err != nil {
		return fmt.Errorf("declare queue %s: %w", q.DLQName(), err)
	}
	if err := ch.QueueBind(q.DLQName(), q.DLQName(), ExchangeDLX, false, nil); err != nil {
		return fmt.Errorf("bind %s: %w", q.DLQName(), err)
	}

	if _, err := ch.QueueDeclare(q.Name, true, false, false, false, amqp.Table{
		amqp.QueueTypeArg:           amqp.QueueTypeQuorum,
		"x-delivery-limit":          q.DeliveryLimit,
		"x-dead-letter-exchange":    ExchangeDLX,
		"x-dead-letter-routing-key": q.DLQName(),
	}); err != nil {
		return fmt.Errorf("declare queue %s: %w", q.Name, err)
	}
	for _, key := range q.BindingKeys {
		if err := ch.QueueBind(q.Name, key, ExchangeEvents, false, nil); err != nil {
			return fmt.Errorf("bind %s to %s: %w", q.Name, key, err)
		}
	}
	return nil
}
