// Package gateway fans broker events out to browsers over Server-Sent Events.
//
// Delivery to browsers is best effort (at-most-once): the live stream is a
// view, the database stays the source of truth. A slow client never slows
// the others down; its messages are dropped instead.
package gateway

import (
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"

	"rt-ed-streaming/internal/outbox"
)

// Message is what a client receives.
type Message struct {
	ID         string
	Type       string
	CustomerID string
	Data       []byte // the full event envelope as JSON
}

type client struct {
	ch         chan Message
	customerID string // empty = all customers
}

// Hub keeps the set of connected clients and broadcasts to them.
type Hub struct {
	log        *slog.Logger
	bufferSize int

	mu      sync.RWMutex
	clients map[*client]struct{}
	closed  bool

	dropped atomic.Int64
}

func NewHub(log *slog.Logger, bufferSize int) *Hub {
	if bufferSize <= 0 {
		bufferSize = 32
	}
	return &Hub{log: log, bufferSize: bufferSize, clients: map[*client]struct{}{}}
}

// Subscribe registers a client. The returned channel is closed when the
// hub shuts down; call the returned func to unsubscribe.
func (h *Hub) Subscribe(customerID string) (<-chan Message, func()) {
	c := &client{ch: make(chan Message, h.bufferSize), customerID: customerID}

	h.mu.Lock()
	if h.closed {
		close(c.ch)
	} else {
		h.clients[c] = struct{}{}
	}
	h.mu.Unlock()

	return c.ch, func() {
		h.mu.Lock()
		if _, ok := h.clients[c]; ok {
			delete(h.clients, c)
			close(c.ch)
		}
		h.mu.Unlock()
	}
}

// Broadcast sends m to every matching client without blocking.
func (h *Hub) Broadcast(m Message) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for c := range h.clients {
		if c.customerID != "" && c.customerID != m.CustomerID {
			continue
		}
		select {
		case c.ch <- m:
		default:
			h.dropped.Add(1)
			h.log.Warn("client too slow, message dropped", "event_id", m.ID)
		}
	}
}

// BroadcastEvent converts an outbox event into a Message and broadcasts it.
func (h *Hub) BroadcastEvent(ev outbox.Event) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	var p struct {
		CustomerID string `json:"customer_id"`
	}
	_ = json.Unmarshal(ev.Payload, &p) // events without a customer go to unfiltered clients only

	h.Broadcast(Message{ID: ev.ID.String(), Type: ev.Type, CustomerID: p.CustomerID, Data: data})
	return nil
}

func (h *Hub) Clients() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

func (h *Hub) Dropped() int64 { return h.dropped.Load() }

// Close disconnects every client. Open SSE responses end, so the HTTP
// server's graceful shutdown does not wait for them forever.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	for c := range h.clients {
		close(c.ch)
		delete(h.clients, c)
	}
}
