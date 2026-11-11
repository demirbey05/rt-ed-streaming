// Package notification is an idempotent consumer of order events. For each
// order.created it stores one notification, exactly once, even though the
// broker delivers at least once.
package notification

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	amqp "github.com/rabbitmq/amqp091-go"

	"rt-ed-streaming/internal/broker"
	"rt-ed-streaming/internal/contracts"
	"rt-ed-streaming/internal/notification/store"
	"rt-ed-streaming/internal/outbox"
)

// ConsumerName scopes the inbox: another consumer of the same event keeps
// its own processed_messages rows.
const ConsumerName = "notification-service"

// DB is satisfied by *pgxpool.Pool.
type DB interface {
	store.DBTX
	Begin(ctx context.Context) (pgx.Tx, error)
}

type Handler struct {
	db  DB
	q   *store.Queries
	log *slog.Logger
	// FailCustomerID makes every order of this customer fail with a
	// transient error. A chaos hook to demonstrate retry → DLQ; empty in
	// normal operation.
	FailCustomerID string
}

func NewHandler(db DB, log *slog.Logger) *Handler {
	return &Handler{db: db, q: store.New(db), log: log}
}

// Handle is a rabbitmq.Handler.
func (h *Handler) Handle(ctx context.Context, d amqp.Delivery) error {
	var ev outbox.Event
	if err := json.Unmarshal(d.Body, &ev); err != nil {
		return fmt.Errorf("%w: decode envelope: %v", broker.ErrPermanent, err)
	}
	if ev.ID == uuid.Nil {
		return fmt.Errorf("%w: event has no id", broker.ErrPermanent)
	}
	return h.HandleEvent(ctx, ev)
}

// HandleEvent records the event in the inbox and applies its side effect in
// one transaction. A duplicate delivery finds the inbox row and does nothing.
func (h *Handler) HandleEvent(ctx context.Context, ev outbox.Event) error {
	tx, err := h.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := h.q.WithTx(tx)

	n, err := q.MarkProcessed(ctx, store.MarkProcessedParams{Consumer: ConsumerName, MessageID: ev.ID})
	if err != nil {
		return fmt.Errorf("mark processed: %w", err)
	}
	if n == 0 {
		h.log.Info("duplicate event skipped", "event_id", ev.ID, "type", ev.Type)
		return nil // already processed: ack without side effects
	}

	switch ev.Type {
	case contracts.EventOrderCreated:
		if err := h.orderCreated(ctx, q, ev); err != nil {
			return err
		}
	default:
		// Unknown types are recorded and acked, so a new event type never
		// blocks this consumer.
		h.log.Debug("ignoring event type", "type", ev.Type)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (h *Handler) orderCreated(ctx context.Context, q *store.Queries, ev outbox.Event) error {
	var p contracts.OrderCreated
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		return fmt.Errorf("%w: decode %s payload: %v", broker.ErrPermanent, ev.Type, err)
	}
	if h.FailCustomerID != "" && p.CustomerID == h.FailCustomerID {
		return errors.New("simulated transient failure")
	}

	id, err := uuid.NewV7()
	if err != nil {
		return err
	}
	msg := fmt.Sprintf("Order %s received: %d.%02d %s", p.OrderID, p.AmountCents/100, p.AmountCents%100, p.Currency)
	if err := q.InsertNotification(ctx, store.InsertNotificationParams{
		ID:         id,
		EventID:    ev.ID,
		OrderID:    p.OrderID,
		CustomerID: p.CustomerID,
		Message:    msg,
	}); err != nil {
		return fmt.Errorf("insert notification: %w", err)
	}
	h.log.Info("notification stored", "event_id", ev.ID, "order_id", p.OrderID, "customer_id", p.CustomerID)
	return nil
}

// List returns the most recent notifications of a customer.
func (h *Handler) List(ctx context.Context, customerID string, limit int32) ([]store.Notification, error) {
	return h.q.ListNotifications(ctx, store.ListNotificationsParams{CustomerID: customerID, Limit: limit})
}
