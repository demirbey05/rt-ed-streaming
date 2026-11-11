// Package order contains the order domain model, its persistence and HTTP API.
package order

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Status string

const StatusCreated Status = "created"

// ErrValidation wraps every input validation error so callers can map
// them to a client error with a single errors.Is check.
var (
	ErrValidation      = errors.New("validation failed")
	ErrInvalidCustomer = fmt.Errorf("%w: customer_id is required", ErrValidation)
	ErrInvalidAmount   = fmt.Errorf("%w: amount_cents must be greater than zero", ErrValidation)
	ErrInvalidCurrency = fmt.Errorf("%w: currency must be a 3-letter ISO code", ErrValidation)
)

// ErrNotFound is returned when an order does not exist.
var ErrNotFound = errors.New("order not found")

const maxIdempotencyKeyLen = 255

var (
	ErrInvalidIdempotencyKey = fmt.Errorf("%w: Idempotency-Key must be at most %d characters", ErrValidation, maxIdempotencyKeyLen)
	// ErrIdempotencyKeyReuse means the key was already used with a different request body.
	ErrIdempotencyKeyReuse = errors.New("idempotency key was already used with a different request")
)

func validateIdempotencyKey(key string) error {
	if len(key) > maxIdempotencyKeyLen {
		return ErrInvalidIdempotencyKey
	}
	return nil
}

// requestHash fingerprints the normalized input of an order, so that a
// retried request ("eur" vs "EUR") matches but a different one does not.
func requestHash(o Order) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s\x00%d\x00%s", o.CustomerID, o.AmountCents, o.Currency))
	return hex.EncodeToString(sum[:])
}

type Order struct {
	ID          uuid.UUID `json:"id"`
	CustomerID  string    `json:"customer_id"`
	AmountCents int64     `json:"amount_cents"`
	Currency    string    `json:"currency"`
	Status      Status    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
}

// NewOrder is the only way to build a valid order. The ID is generated here,
// not by the database, so events can reference it before the commit.
func NewOrder(customerID string, amountCents int64, currency string) (Order, error) {
	customerID = strings.TrimSpace(customerID)
	if customerID == "" {
		return Order{}, ErrInvalidCustomer
	}
	if amountCents <= 0 {
		return Order{}, ErrInvalidAmount
	}

	currency = strings.ToUpper(strings.TrimSpace(currency))
	if !isCurrencyCode(currency) {
		return Order{}, ErrInvalidCurrency
	}

	// UUIDv7 is time-ordered, so inserts append to the primary key index
	// instead of landing on random pages (as UUIDv4 would).
	id, err := uuid.NewV7()
	if err != nil {
		return Order{}, fmt.Errorf("generate order id: %w", err)
	}

	return Order{
		ID:          id,
		CustomerID:  customerID,
		AmountCents: amountCents,
		Currency:    currency,
		Status:      StatusCreated,
		// Postgres stores microseconds; truncate so the value we return
		// equals what a later read from the database returns.
		CreatedAt: time.Now().UTC().Truncate(time.Microsecond),
	}, nil
}

func isCurrencyCode(s string) bool {
	if len(s) != 3 {
		return false
	}
	for _, r := range s {
		if r < 'A' || r > 'Z' {
			return false
		}
	}
	return true
}
