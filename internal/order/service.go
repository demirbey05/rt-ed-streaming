package order

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"rt-ed-streaming/internal/order/store"
)

// DB is the minimal database surface the service needs.
// *pgxpool.Pool satisfies it.
type DB interface {
	store.DBTX
	Begin(ctx context.Context) (pgx.Tx, error)
}

type Service struct {
	db DB
	q  *store.Queries
}

func NewService(db DB) *Service {
	return &Service{db: db, q: store.New(db)}
}

type CreateInput struct {
	CustomerID  string
	AmountCents int64
	Currency    string
}

// Create validates the input, then writes the order and its order.created
// event in a single transaction (Transactional Outbox). Either both rows are
// committed or neither is; the event is published later by the outbox relay.
func (s *Service) Create(ctx context.Context, in CreateInput) (Order, error) {
	// Everything that can fail without the database happens before BEGIN,
	// so the transaction holds nothing but the two inserts.
	o, err := NewOrder(in.CustomerID, in.AmountCents, in.Currency)
	if err != nil {
		return Order{}, err
	}
	ev, err := o.CreatedEvent()
	if err != nil {
		return Order{}, fmt.Errorf("build order.created event: %w", err)
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return Order{}, fmt.Errorf("begin tx: %w", err)
	}
	// No-op after a successful Commit; rolls back on any early return or panic.
	defer func() { _ = tx.Rollback(ctx) }()

	q := s.q.WithTx(tx)

	if _, err := q.CreateOrder(ctx, store.CreateOrderParams{
		ID:          o.ID,
		CustomerID:  o.CustomerID,
		AmountCents: o.AmountCents,
		Currency:    o.Currency,
		Status:      string(o.Status),
		CreatedAt:   o.CreatedAt,
	}); err != nil {
		return Order{}, fmt.Errorf("insert order: %w", err)
	}

	if err := q.InsertOutboxEvent(ctx, store.InsertOutboxEventParams{
		ID:            ev.ID,
		AggregateType: ev.AggregateType,
		AggregateID:   ev.AggregateID,
		EventType:     ev.Type,
		Payload:       ev.Payload,
		CreatedAt:     ev.OccurredAt,
	}); err != nil {
		return Order{}, fmt.Errorf("insert outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return Order{}, fmt.Errorf("commit: %w", err)
	}
	return o, nil
}

// Get returns the order with the given ID, or ErrNotFound.
func (s *Service) Get(ctx context.Context, id uuid.UUID) (Order, error) {
	row, err := s.q.GetOrder(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return Order{}, ErrNotFound
	}
	if err != nil {
		return Order{}, fmt.Errorf("get order: %w", err)
	}
	return toDomain(row), nil
}

func toDomain(r store.Order) Order {
	return Order{
		ID:          r.ID,
		CustomerID:  r.CustomerID,
		AmountCents: r.AmountCents,
		Currency:    r.Currency,
		Status:      Status(r.Status),
		CreatedAt:   r.CreatedAt.UTC(), // pgx returns timestamptz in the local zone
	}
}
