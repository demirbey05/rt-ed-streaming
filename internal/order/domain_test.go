package order

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestNewOrder(t *testing.T) {
	tests := []struct {
		name         string
		customerID   string
		amountCents  int64
		currency     string
		wantErr      error
		wantCurrency string
	}{
		{name: "valid", customerID: "c-1", amountCents: 1999, currency: "EUR", wantCurrency: "EUR"},
		{name: "currency is normalized", customerID: "c-1", amountCents: 1, currency: " try ", wantCurrency: "TRY"},
		{name: "empty customer", customerID: "", amountCents: 100, currency: "EUR", wantErr: ErrInvalidCustomer},
		{name: "whitespace customer", customerID: "   ", amountCents: 100, currency: "EUR", wantErr: ErrInvalidCustomer},
		{name: "zero amount", customerID: "c-1", amountCents: 0, currency: "EUR", wantErr: ErrInvalidAmount},
		{name: "negative amount", customerID: "c-1", amountCents: -5, currency: "EUR", wantErr: ErrInvalidAmount},
		{name: "short currency", customerID: "c-1", amountCents: 100, currency: "EU", wantErr: ErrInvalidCurrency},
		{name: "long currency", customerID: "c-1", amountCents: 100, currency: "EURO", wantErr: ErrInvalidCurrency},
		{name: "non-letter currency", customerID: "c-1", amountCents: 100, currency: "E1R", wantErr: ErrInvalidCurrency},
		{name: "non-ascii currency", customerID: "c-1", amountCents: 100, currency: "ÜSD", wantErr: ErrInvalidCurrency},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o, err := NewOrder(tt.customerID, tt.amountCents, tt.currency)

			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				if !errors.Is(err, ErrValidation) {
					t.Fatalf("err %v should wrap ErrValidation", err)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if o.ID == uuid.Nil {
				t.Error("ID was not generated")
			}
			if o.Status != StatusCreated {
				t.Errorf("Status = %q, want %q", o.Status, StatusCreated)
			}
			if o.Currency != tt.wantCurrency {
				t.Errorf("Currency = %q, want %q", o.Currency, tt.wantCurrency)
			}
			if o.CreatedAt.IsZero() {
				t.Error("CreatedAt was not set")
			}
		})
	}
}

func TestOrderCreatedEvent(t *testing.T) {
	o, err := NewOrder("c-1", 2500, "EUR")
	if err != nil {
		t.Fatal(err)
	}

	ev, err := o.CreatedEvent()
	if err != nil {
		t.Fatal(err)
	}

	if ev.ID == uuid.Nil || ev.ID == o.ID {
		t.Errorf("event ID must be a new UUID distinct from the order ID, got %s", ev.ID)
	}
	if ev.Type != EventTypeCreated || ev.AggregateType != AggregateType {
		t.Errorf("type = %q/%q, want %q/%q", ev.Type, ev.AggregateType, EventTypeCreated, AggregateType)
	}
	if ev.AggregateID != o.ID {
		t.Errorf("AggregateID = %s, want %s", ev.AggregateID, o.ID)
	}

	var p OrderCreatedPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		t.Fatalf("payload is not valid JSON: %v", err)
	}
	if p.OrderID != o.ID || p.AmountCents != 2500 || p.Currency != "EUR" || p.CustomerID != "c-1" {
		t.Errorf("payload mismatch: %+v", p)
	}
}
