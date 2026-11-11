package gateway

import (
	"bufio"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"rt-ed-streaming/internal/outbox"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestHub_FiltersByCustomer(t *testing.T) {
	h := NewHub(discardLogger(), 4)
	all, unsubAll := h.Subscribe("")
	alice, unsubAlice := h.Subscribe("alice")
	defer unsubAll()
	defer unsubAlice()

	h.Broadcast(Message{ID: "1", CustomerID: "bob"})
	h.Broadcast(Message{ID: "2", CustomerID: "alice"})

	if got := drain(all); got != "1,2" {
		t.Errorf("unfiltered client got %q, want 1,2", got)
	}
	if got := drain(alice); got != "2" {
		t.Errorf("alice got %q, want 2", got)
	}
}

func TestHub_SlowClientDropsInsteadOfBlocking(t *testing.T) {
	h := NewHub(discardLogger(), 2)
	_, unsub := h.Subscribe("") // never reads
	defer unsub()

	done := make(chan struct{})
	go func() {
		for i := range 10 {
			h.Broadcast(Message{ID: string(rune('a' + i))})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Broadcast blocked on a slow client")
	}
	if h.Dropped() != 8 {
		t.Errorf("dropped = %d, want 8 (buffer of 2)", h.Dropped())
	}
}

func TestHub_CloseEndsSubscriptions(t *testing.T) {
	h := NewHub(discardLogger(), 1)
	ch, unsub := h.Subscribe("")
	h.Close()
	unsub() // must not panic after Close

	if _, ok := <-ch; ok {
		t.Error("channel still open after Close")
	}
	if late, _ := h.Subscribe(""); late != nil {
		if _, ok := <-late; ok {
			t.Error("subscription after Close should be closed")
		}
	}
}

func TestSSE_StreamsEventsToClient(t *testing.T) {
	h := NewHub(discardLogger(), 8)
	mux := http.NewServeMux()
	h.Register(mux, time.Hour)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	defer h.Close() // ends the open stream before srv.Close waits for it

	resp, err := http.Get(srv.URL + "/events?customer_id=alice")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q", ct)
	}

	r := bufio.NewReader(resp.Body)
	readLine(t, r) // ": connected"

	ev, _ := outbox.NewEvent("order.created", "order", uuid.New(), map[string]string{"customer_id": "alice"})
	if err := h.BroadcastEvent(ev); err != nil {
		t.Fatal(err)
	}

	var got []string
	for len(got) < 3 {
		if line := readLine(t, r); line != "" {
			got = append(got, line)
		}
	}
	if got[0] != "id: "+ev.ID.String() || got[1] != "event: order.created" || !strings.HasPrefix(got[2], "data: {") {
		t.Errorf("unexpected SSE frame: %q", got)
	}
}

func readLine(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return strings.TrimRight(line, "\n")
}

func drain(ch <-chan Message) string {
	var ids []string
	for {
		select {
		case m := <-ch:
			ids = append(ids, m.ID)
		default:
			return strings.Join(ids, ",")
		}
	}
}
