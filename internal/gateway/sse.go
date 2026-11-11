package gateway

import (
	_ "embed"
	"fmt"
	"net/http"
	"time"

	"rt-ed-streaming/internal/httpserver"
)

//go:embed static/index.html
var indexHTML []byte

// Register exposes:
//
//	GET /             demo page that renders the live stream
//	GET /events       SSE stream; ?customer_id=... filters to one customer
//	GET /stats        connected clients and dropped messages
func (h *Hub) Register(mux *http.ServeMux, heartbeat time.Duration) {
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
	})
	mux.HandleFunc("GET /events", h.serveSSE(heartbeat))
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		httpserver.WriteJSON(w, http.StatusOK, map[string]any{"clients": h.Clients(), "dropped": h.Dropped()})
	})
}

func (h *Hub) serveSSE(heartbeat time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rc := http.NewResponseController(w)

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no") // disable proxy buffering (nginx)

		msgs, unsubscribe := h.Subscribe(r.URL.Query().Get("customer_id"))
		defer unsubscribe()

		// Comment lines (": ...") are ignored by EventSource; they flush
		// headers now and keep idle proxies from closing the connection.
		if _, err := fmt.Fprint(w, ": connected\n\n"); err != nil {
			return
		}
		if err := rc.Flush(); err != nil {
			httpserver.WriteError(w, http.StatusInternalServerError, "streaming unsupported")
			return
		}

		ticker := time.NewTicker(heartbeat)
		defer ticker.Stop()

		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
					return
				}
			case m, ok := <-msgs:
				if !ok {
					return // hub closed
				}
				if _, err := fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", m.ID, m.Type, m.Data); err != nil {
					return
				}
			}
			if err := rc.Flush(); err != nil {
				return
			}
		}
	}
}
