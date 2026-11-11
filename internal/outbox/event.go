// Package outbox defines the event envelope written to the outbox table
// and later published to the message broker.
package outbox

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Event is the envelope shared by every event type. Its ID doubles as the
// outbox row's primary key and as the idempotency key on the consumer side.
type Event struct {
	ID            uuid.UUID       `json:"id"`
	Type          string          `json:"type"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   uuid.UUID       `json:"aggregate_id"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Payload       json.RawMessage `json:"payload"`
}

// NewEvent marshals payload and wraps it in an envelope with a fresh ID.
func NewEvent(eventType, aggregateType string, aggregateID uuid.UUID, payload any) (Event, error) {
	if eventType == "" || aggregateType == "" {
		return Event{}, errors.New("outbox: event type and aggregate type are required")
	}
	if aggregateID == uuid.Nil {
		return Event{}, errors.New("outbox: aggregate id is required")
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return Event{}, fmt.Errorf("outbox: marshal %s payload: %w", eventType, err)
	}

	id, err := uuid.NewV7()
	if err != nil {
		return Event{}, fmt.Errorf("outbox: generate event id: %w", err)
	}

	return Event{
		ID:            id,
		Type:          eventType,
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		OccurredAt:    time.Now().UTC().Truncate(time.Microsecond), // match Postgres precision
		Payload:       raw,
	}, nil
}
