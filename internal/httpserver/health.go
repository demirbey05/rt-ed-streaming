// Package httpserver contains shared HTTP handlers.
package httpserver

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// Pinger is satisfied by *pgxpool.Pool (and easy to fake in tests).
type Pinger interface {
	Ping(ctx context.Context) error
}

// HealthHandler returns 200 when all dependencies respond, 503 otherwise.
func HealthHandler(log *slog.Logger, db Pinger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		status, code := "ok", http.StatusOK
		checks := map[string]string{"postgres": "ok"}

		if err := db.Ping(ctx); err != nil {
			log.Warn("health check failed", "dependency", "postgres", "error", err)
			checks["postgres"] = "unavailable"
			status, code = "degraded", http.StatusServiceUnavailable
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": status, "checks": checks})
	}
}
