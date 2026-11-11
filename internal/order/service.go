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
	// IdempotencyKey is optional. When set, retries with the same key and
	// body return the original order instead of creating a new one.
	IdempotencyKey string
}

type CreateResult struct {
	Order Order
	// Replayed is true when the order was created by an earlier request
	// with the same Idempotency-Key.
	Replayed bool
}

// Create validates the input, then writes the order and its order.created
// event in a single transaction (Transactional Outbox). Either both rows are
// committed or neither is; the event is published later by the outbox relay.
func (s *Service) Create(ctx context.Context, in CreateInput) (CreateResult, error) {
	// Everything that can fail without the database happens before BEGIN,
	// so the transaction holds nothing but the inserts.
	o, err := NewOrder(in.CustomerID, in.AmountCents, in.Currency)
	if err != nil {
		return CreateResult{}, err
	}
	key := in.IdempotencyKey
	if err := validateIdempotencyKey(key); err != nil {
		return CreateResult{}, err
	}
	hash := requestHash(o)

	// Fast path: a plain retry of a request that already succeeded.
	if key != "" {
		if res, ok, err := s.replay(ctx, key, hash); err != nil || ok {
			return res, err
		}
	}

	ev, err := o.CreatedEvent()
	if err != nil {
		return CreateResult{}, fmt.Errorf("build order.created event: %w", err)
	}

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return CreateResult{}, fmt.Errorf("begin tx: %w", err)
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
		return CreateResult{}, fmt.Errorf("insert order: %w", err)
	}

	if err := q.InsertOutboxEvent(ctx, store.InsertOutboxEventParams{
		ID:            ev.ID,
		AggregateType: ev.AggregateType,
		AggregateID:   ev.AggregateID,
		EventType:     ev.Type,
		Payload:       ev.Payload,
		CreatedAt:     ev.OccurredAt,
	}); err != nil {
		return CreateResult{}, fmt.Errorf("insert outbox event: %w", err)
	}

	if key != "" {
		// Slow path: a concurrent request with the same key. The unique index
		// makes this insert wait for the other transaction; if it commits we
		// get 0 rows, roll back our order + event, and return theirs.
		n, err := q.InsertIdempotencyKey(ctx, store.InsertIdempotencyKeyParams{
			Key:         key,
			RequestHash: hash,
			OrderID:     o.ID,
		})
		if err != nil {
			return CreateResult{}, fmt.Errorf("insert idempotency key: %w", err)
		}
		if n == 0 {
			_ = tx.Rollback(ctx)
			res, ok, err := s.replay(ctx, key, hash)
			if err != nil {
				return CreateResult{}, err
			}
			if !ok {
				return CreateResult{}, errors.New("idempotency key conflict but no stored key found")
			}
			return res, nil
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return CreateResult{}, fmt.Errorf("commit: %w", err)
	}
	return CreateResult{Order: o}, nil
}

// replay looks up a stored idempotency key. ok is true when the key exists
// and matches hash; a key stored with a different hash is an error.
func (s *Service) replay(ctx context.Context, key, hash string) (res CreateResult, ok bool, err error) {
	row, err := s.q.GetIdempotencyKey(ctx, key)
	if errors.Is(err, pgx.ErrNoRows) {
		return CreateResult{}, false, nil
	}
	if err != nil {
		return CreateResult{}, false, fmt.Errorf("get idempotency key: %w", err)
	}
	if row.RequestHash != hash {
		return CreateResult{}, false, ErrIdempotencyKeyReuse
	}

	o, err := s.Get(ctx, row.OrderID)
	if err != nil {
		return CreateResult{}, false, err
	}
	return CreateResult{Order: o, Replayed: true}, true, nil
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
