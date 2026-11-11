package order

import (
	"time"

	"github.com/google/uuid"

	"rt-ed-streaming/internal/outbox"
)

const (
	AggregateType    = "order"
	EventTypeCreated = "order.created"
)

// OrderCreatedPayload is the public contract of the order.created event.
// It is deliberately separate from both Order and store.Order so that
// changes to the domain model or table schema don't break consumers.
type OrderCreatedPayload struct {
	OrderID     uuid.UUID `json:"order_id"`
	CustomerID  string    `json:"customer_id"`
	AmountCents int64     `json:"amount_cents"`
	Currency    string    `json:"currency"`
	CreatedAt   time.Time `json:"created_at"`
}

// CreatedEvent builds the order.created event for this order.
func (o Order) CreatedEvent() (outbox.Event, error) {
	return outbox.NewEvent(EventTypeCreated, AggregateType, o.ID, OrderCreatedPayload{
		OrderID:     o.ID,
		CustomerID:  o.CustomerID,
		AmountCents: o.AmountCents,
		Currency:    o.Currency,
		CreatedAt:   o.CreatedAt,
	})
}
