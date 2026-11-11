// Package relay moves events from the outbox table to the message broker.
//
// Delivery is at-least-once: an event is marked published only after the
// broker confirms it. If the relay crashes between the confirm and the
// commit, the event is published again on restart, so consumers must be
// idempotent on the event ID.
package relay

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"rt-ed-streaming/internal/broker"
	"rt-ed-streaming/internal/outbox"
	"rt-ed-streaming/internal/relay/store"
)

// DB is satisfied by *pgxpool.Pool.
type DB interface {
	store.DBTX
	Begin(ctx context.Context) (pgx.Tx, error)
}

type Config struct {
	BatchSize      int
	PollInterval   time.Duration // wait between polls when the outbox is drained
	MaxBackoff     time.Duration // upper bound for the wait after a failure
	PublishTimeout time.Duration // per-message broker timeout
}

func (c *Config) defaults() {
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 500 * time.Millisecond
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 30 * time.Second
	}
	if c.PublishTimeout <= 0 {
		c.PublishTimeout = 10 * time.Second
	}
}

type Relay struct {
	db  DB
	q   *store.Queries
	pub broker.Publisher
	log *slog.Logger
	cfg Config
}

func New(db DB, pub broker.Publisher, log *slog.Logger, cfg Config) *Relay {
	cfg.defaults()
	return &Relay{db: db, q: store.New(db), pub: pub, log: log, cfg: cfg}
}

// Run polls the outbox until ctx is cancelled. A full batch is followed
// immediately by the next one; a failure backs off exponentially.
func (r *Relay) Run(ctx context.Context) error {
	r.log.Info("relay started", "publisher", r.pub.Name(), "batch_size", r.cfg.BatchSize, "poll_interval", r.cfg.PollInterval.String())
	var backoff time.Duration

	for {
		n, err := r.ProcessBatch(ctx)

		var wait time.Duration
		switch {
		case ctx.Err() != nil:
			return nil
		case err != nil:
			backoff = nextBackoff(backoff, r.cfg.PollInterval, r.cfg.MaxBackoff)
			wait = backoff
			r.log.Warn("relay batch failed, backing off", "published", n, "retry_in", wait.String(), "error", err)
		case n == r.cfg.BatchSize:
			backoff, wait = 0, 0 // more rows are probably waiting
		default:
			backoff, wait = 0, r.cfg.PollInterval
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
	}
}

// ProcessBatch publishes up to BatchSize unpublished events in created_at
// order. It stops at the first failure, so later events are not published
// ahead of an earlier one, and returns how many were published.
func (r *Relay) ProcessBatch(ctx context.Context) (int, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	q := r.q.WithTx(tx)

	rows, err := q.FetchUnpublished(ctx, int32(r.cfg.BatchSize))
	if err != nil {
		return 0, fmt.Errorf("fetch unpublished: %w", err)
	}
	if len(rows) == 0 {
		return 0, nil
	}

	var (
		published []uuid.UUID
		pubErr    error
	)
	for _, row := range rows {
		ev := toEvent(row)
		if err := r.publish(ctx, ev); err != nil {
			pubErr = fmt.Errorf("publish %s %s: %w", ev.Type, ev.ID, err)
			// Record the failure for observability; the row stays unpublished.
			if err := q.MarkFailed(context.WithoutCancel(ctx), store.MarkFailedParams{ID: row.ID, LastError: stringPtr(err.Error())}); err != nil {
				return 0, fmt.Errorf("mark failed: %w", err)
			}
			break
		}
		published = append(published, row.ID)
	}

	// Events the broker already confirmed must be recorded even if we are
	// shutting down; otherwise they would be published a second time.
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if len(published) > 0 {
		if err := q.MarkPublished(writeCtx, published); err != nil {
			return 0, fmt.Errorf("mark published: %w", err)
		}
	}
	if err := tx.Commit(writeCtx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}

	if len(published) > 0 {
		r.log.Debug("published events", "count", len(published))
	}
	return len(published), pubErr
}

func (r *Relay) publish(ctx context.Context, ev outbox.Event) error {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.PublishTimeout)
	defer cancel()
	return r.pub.Publish(ctx, ev)
}

func toEvent(r store.FetchUnpublishedRow) outbox.Event {
	return outbox.Event{
		ID:            r.ID,
		Type:          r.EventType,
		AggregateType: r.AggregateType,
		AggregateID:   r.AggregateID,
		OccurredAt:    r.CreatedAt.UTC(),
		Payload:       r.Payload,
	}
}

func nextBackoff(cur, base, max time.Duration) time.Duration {
	if cur == 0 {
		return base
	}
	return min(cur*2, max)
}

func stringPtr(s string) *string { return &s }
