// Package httpserver contains shared HTTP plumbing.
package httpserver

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

// Pinger is satisfied by *pgxpool.Pool (and easy to fake in tests).
type Pinger interface {
	Ping(ctx context.Context) error
}

// PingFunc adapts a function to Pinger.
type PingFunc func(ctx context.Context) error

func (f PingFunc) Ping(ctx context.Context) error { return f(ctx) }

// HealthHandler returns 200 when every dependency responds, 503 otherwise.
func HealthHandler(log *slog.Logger, deps map[string]Pinger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		status, code := "ok", http.StatusOK
		checks := make(map[string]string, len(deps))

		for name, dep := range deps {
			if err := dep.Ping(ctx); err != nil {
				log.Warn("health check failed", "dependency", name, "error", err)
				checks[name] = "unavailable"
				status, code = "degraded", http.StatusServiceUnavailable
				continue
			}
			checks[name] = "ok"
		}

		WriteJSON(w, code, map[string]any{"status": status, "checks": checks})
	}
}
