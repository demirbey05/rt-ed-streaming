// Package contracts holds the public event schemas shared between the
// producer (order-service) and every consumer. Consumers depend on this
// package, never on the producer's internals.
package contracts

import (
	"time"

	"github.com/google/uuid"
)

const (
	AggregateOrder    = "order"
	EventOrderCreated = "order.created"
)

// OrderCreated is the payload of the order.created event.
// Changes must stay backward compatible: add fields, never rename or remove.
type OrderCreated struct {
	OrderID     uuid.UUID `json:"order_id"`
	CustomerID  string    `json:"customer_id"`
	AmountCents int64     `json:"amount_cents"`
	Currency    string    `json:"currency"`
	CreatedAt   time.Time `json:"created_at"`
}
