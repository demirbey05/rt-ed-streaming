package order

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// fakeService runs the real domain validation and keeps orders in memory.
type fakeService struct {
	orders    map[uuid.UUID]Order
	keys      map[string]uuid.UUID // idempotency key → order ID
	createErr error                // forced error for Create, if set
}

func newFakeService() *fakeService {
	return &fakeService{orders: map[uuid.UUID]Order{}, keys: map[string]uuid.UUID{}}
}

func (f *fakeService) Create(_ context.Context, in CreateInput) (CreateResult, error) {
	if f.createErr != nil {
		return CreateResult{}, f.createErr
	}
	o, err := NewOrder(in.CustomerID, in.AmountCents, in.Currency)
	if err != nil {
		return CreateResult{}, err
	}
	if err := validateIdempotencyKey(in.IdempotencyKey); err != nil {
		return CreateResult{}, err
	}
	if id, ok := f.keys[in.IdempotencyKey]; ok && in.IdempotencyKey != "" {
		prev := f.orders[id]
		if requestHash(prev) != requestHash(o) {
			return CreateResult{}, ErrIdempotencyKeyReuse
		}
		return CreateResult{Order: prev, Replayed: true}, nil
	}
	f.orders[o.ID] = o
	if in.IdempotencyKey != "" {
		f.keys[in.IdempotencyKey] = o.ID
	}
	return CreateResult{Order: o}, nil
}

func (f *fakeService) Get(_ context.Context, id uuid.UUID) (Order, error) {
	o, ok := f.orders[id]
	if !ok {
		return Order{}, ErrNotFound
	}
	return o, nil
}

func newTestServer(svc *fakeService) *http.ServeMux {
	mux := http.NewServeMux()
	NewHandler(svc, slog.New(slog.NewTextHandler(io.Discard, nil))).Register(mux)
	return mux
}

func do(mux *http.ServeMux, method, path, body string, headers ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func TestCreateOrder(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		createErr  error
		wantStatus int
	}{
		{name: "valid", body: `{"customer_id":"c-1","amount_cents":1999,"currency":"EUR"}`, wantStatus: http.StatusCreated},
		{name: "negative amount", body: `{"customer_id":"c-1","amount_cents":-1,"currency":"EUR"}`, wantStatus: http.StatusBadRequest},
		{name: "bad currency", body: `{"customer_id":"c-1","amount_cents":100,"currency":"EURO"}`, wantStatus: http.StatusBadRequest},
		{name: "missing customer", body: `{"amount_cents":100,"currency":"EUR"}`, wantStatus: http.StatusBadRequest},
		{name: "malformed json", body: `{"customer_id":`, wantStatus: http.StatusBadRequest},
		{name: "unknown field", body: `{"customer_id":"c-1","amount_cents":100,"currency":"EUR","admin":true}`, wantStatus: http.StatusBadRequest},
		{name: "wrong type", body: `{"customer_id":"c-1","amount_cents":"100","currency":"EUR"}`, wantStatus: http.StatusBadRequest},
		{name: "service failure", body: `{"customer_id":"c-1","amount_cents":100,"currency":"EUR"}`, createErr: errors.New("db down: secret dsn"), wantStatus: http.StatusInternalServerError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := newFakeService()
			svc.createErr = tt.createErr
			rec := do(newTestServer(svc), http.MethodPost, "/orders", tt.body)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tt.wantStatus, rec.Body)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("Content-Type = %q", ct)
			}

			switch rec.Code {
			case http.StatusCreated:
				var o Order
				if err := json.NewDecoder(rec.Body).Decode(&o); err != nil {
					t.Fatal(err)
				}
				if loc := rec.Header().Get("Location"); loc != "/orders/"+o.ID.String() {
					t.Errorf("Location = %q", loc)
				}
				if len(svc.orders) != 1 {
					t.Errorf("stored %d orders, want 1", len(svc.orders))
				}
			case http.StatusInternalServerError:
				if strings.Contains(rec.Body.String(), "secret") {
					t.Errorf("internal error leaked to client: %s", rec.Body)
				}
			default:
				if len(svc.orders) != 0 {
					t.Errorf("invalid request stored %d orders", len(svc.orders))
				}
			}
		})
	}
}

func TestGetOrder(t *testing.T) {
	existing, _ := NewOrder("c-1", 500, "TRY")
	svc := newFakeService()
	svc.orders[existing.ID] = existing
	mux := newTestServer(svc)

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{name: "found", path: "/orders/" + existing.ID.String(), wantStatus: http.StatusOK},
		{name: "not found", path: "/orders/" + uuid.NewString(), wantStatus: http.StatusNotFound},
		{name: "malformed id", path: "/orders/not-a-uuid", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(mux, http.MethodGet, tt.path, "")
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tt.wantStatus, rec.Body)
			}
			if rec.Code == http.StatusOK {
				var o Order
				if err := json.NewDecoder(rec.Body).Decode(&o); err != nil {
					t.Fatal(err)
				}
				if o.ID != existing.ID {
					t.Errorf("ID = %s, want %s", o.ID, existing.ID)
				}
			}
		})
	}
}

func TestCreateOrderIdempotency(t *testing.T) {
	const body = `{"customer_id":"c-1","amount_cents":1999,"currency":"EUR"}`
	svc := newFakeService()
	mux := newTestServer(svc)

	first := do(mux, http.MethodPost, "/orders", body, "Idempotency-Key", "k-1")
	if first.Code != http.StatusCreated || first.Header().Get("Idempotent-Replayed") != "" {
		t.Fatalf("first: status=%d replayed=%q", first.Code, first.Header().Get("Idempotent-Replayed"))
	}

	// Same key, same body (currency case differs but normalizes the same) → replay.
	second := do(mux, http.MethodPost, "/orders", strings.Replace(body, "EUR", "eur", 1), "Idempotency-Key", "k-1")
	if second.Code != http.StatusCreated || second.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay: status=%d replayed=%q", second.Code, second.Header().Get("Idempotent-Replayed"))
	}
	if first.Header().Get("Location") != second.Header().Get("Location") {
		t.Errorf("replay returned a different order: %s vs %s", first.Header().Get("Location"), second.Header().Get("Location"))
	}

	// Same key, different body → 422.
	conflict := do(mux, http.MethodPost, "/orders", `{"customer_id":"c-1","amount_cents":1,"currency":"EUR"}`, "Idempotency-Key", "k-1")
	if conflict.Code != http.StatusUnprocessableEntity {
		t.Fatalf("reuse: status=%d, want 422", conflict.Code)
	}

	// Oversized key → 400.
	long := do(mux, http.MethodPost, "/orders", body, "Idempotency-Key", strings.Repeat("x", 256))
	if long.Code != http.StatusBadRequest {
		t.Fatalf("long key: status=%d, want 400", long.Code)
	}

	if len(svc.orders) != 1 {
		t.Errorf("stored %d orders, want 1", len(svc.orders))
	}
}
