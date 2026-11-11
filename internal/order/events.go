package order

import (
	"rt-ed-streaming/internal/contracts"
	"rt-ed-streaming/internal/outbox"
)

const (
	AggregateType    = contracts.AggregateOrder
	EventTypeCreated = contracts.EventOrderCreated
)

// OrderCreatedPayload is the public contract of the order.created event.
// It lives in the contracts package so consumers don't import this one.
type OrderCreatedPayload = contracts.OrderCreated

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
