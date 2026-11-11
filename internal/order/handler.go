package order

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"rt-ed-streaming/internal/httpserver"
)

const maxBodyBytes = 1 << 20 // 1 MB

// OrderService is what the HTTP layer needs from the service.
// *Service satisfies it; tests use a fake.
type OrderService interface {
	Create(ctx context.Context, in CreateInput) (Order, error)
	Get(ctx context.Context, id uuid.UUID) (Order, error)
}

type Handler struct {
	svc OrderService
	log *slog.Logger
}

func NewHandler(svc OrderService, log *slog.Logger) *Handler {
	return &Handler{svc: svc, log: log}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /orders", h.create)
	mux.HandleFunc("GET /orders/{id}", h.get)
}

type createRequest struct {
	CustomerID  string `json:"customer_id"`
	AmountCents int64  `json:"amount_cents"`
	Currency    string `json:"currency"`
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	var req createRequest
	if err := dec.Decode(&req); err != nil {
		httpserver.WriteError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	o, err := h.svc.Create(r.Context(), CreateInput{
		CustomerID:  req.CustomerID,
		AmountCents: req.AmountCents,
		Currency:    req.Currency,
	})
	if err != nil {
		h.writeServiceError(w, r, err)
		return
	}

	h.log.Info("order created", "order_id", o.ID, "customer_id", o.CustomerID)
	w.Header().Set("Location", "/orders/"+o.ID.String())
	httpserver.WriteJSON(w, http.StatusCreated, o)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		httpserver.WriteError(w, http.StatusBadRequest, "invalid order id")
		return
	}

	o, err := h.svc.Get(r.Context(), id)
	if err != nil {
		h.writeServiceError(w, r, err)
		return
	}
	httpserver.WriteJSON(w, http.StatusOK, o)
}

// writeServiceError maps domain errors to HTTP status codes. Unknown errors
// become a generic 500; the details are logged, never sent to the client.
func (h *Handler) writeServiceError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ErrValidation):
		httpserver.WriteError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrNotFound):
		httpserver.WriteError(w, http.StatusNotFound, err.Error())
	default:
		h.log.Error("request failed", "method", r.Method, "path", r.URL.Path, "error", err)
		httpserver.WriteError(w, http.StatusInternalServerError, "internal server error")
	}
}
