//go:build integration

// Package integration tests the event pipeline across services against real
// Postgres and RabbitMQ containers:
//
//	order.Service → outbox → relay → RabbitMQ → notification consumer / stream gateway
//
// Run with: make test-integration (requires Docker).
package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcrabbit "github.com/testcontainers/testcontainers-go/modules/rabbitmq"

	"rt-ed-streaming/internal/broker"
	"rt-ed-streaming/internal/broker/rabbitmq"
	"rt-ed-streaming/internal/db"
	"rt-ed-streaming/internal/gateway"
	"rt-ed-streaming/internal/notification"
	"rt-ed-streaming/internal/order"
	"rt-ed-streaming/internal/outbox"
	"rt-ed-streaming/internal/relay"
)

var (
	pool    *pgxpool.Pool
	amqpURL string
	quiet   = slog.New(slog.NewTextHandler(io.Discard, nil))
)

func TestMain(m *testing.M) { os.Exit(run(m)) }

func run(m *testing.M) int {
	ctx := context.Background()

	pg, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("streaming"), tcpostgres.WithUsername("app"), tcpostgres.WithPassword("app"),
		tcpostgres.BasicWaitStrategies())
	if err != nil {
		log.Printf("postgres: %v", err)
		return 1
	}
	defer func() { _ = testcontainers.TerminateContainer(pg) }()

	rmq, err := tcrabbit.Run(ctx, "rabbitmq:4.1-management-alpine")
	if err != nil {
		log.Printf("rabbitmq: %v", err)
		return 1
	}
	defer func() { _ = testcontainers.TerminateContainer(rmq) }()

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Print(err)
		return 1
	}
	if err := db.MigrateUp(dsn); err != nil {
		log.Print(err)
		return 1
	}
	if pool, err = pgxpool.New(ctx, dsn); err != nil {
		log.Print(err)
		return 1
	}
	defer pool.Close()

	if amqpURL, err = rmq.AmqpURL(ctx); err != nil {
		log.Print(err)
		return 1
	}
	return m.Run()
}

// --- helpers ---------------------------------------------------------------

func reset(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `TRUNCATE orders, outbox, idempotency_keys, processed_messages, notifications`); err != nil {
		t.Fatal(err)
	}
	withChannel(t, func(ch *amqp.Channel) {
		if err := rabbitmq.DeclareExchanges(ch); err != nil {
			t.Fatal(err)
		}
		if err := rabbitmq.DeclareQueue(ch, rabbitmq.NotificationQueue); err != nil {
			t.Fatal(err)
		}
		for _, q := range []string{rabbitmq.NotificationQueue.Name, rabbitmq.NotificationQueue.DLQName()} {
			if _, err := ch.QueuePurge(q, false); err != nil {
				t.Fatal(err)
			}
		}
	})
}

func withChannel(t *testing.T, fn func(*amqp.Channel)) {
	t.Helper()
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	fn(ch)
}

func createOrders(t *testing.T, n int, customer string) []order.Order {
	t.Helper()
	svc := order.NewService(pool)
	out := make([]order.Order, 0, n)
	for i := range n {
		res, err := svc.Create(context.Background(), order.CreateInput{CustomerID: customer, AmountCents: int64(100 + i), Currency: "EUR"})
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, res.Order)
	}
	return out
}

func scalar(t *testing.T, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func eventually(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out: %s", msg)
}

func queueDepth(t *testing.T, name string) int {
	t.Helper()
	var n int
	withChannel(t, func(ch *amqp.Channel) {
		q, err := ch.QueueDeclarePassive(name, true, false, false, false, nil)
		if err != nil {
			t.Fatal(err)
		}
		n = q.Messages
	})
	return n
}

func notificationSetup(ch *amqp.Channel) (string, error) {
	if err := rabbitmq.DeclareExchanges(ch); err != nil {
		return "", err
	}
	return rabbitmq.NotificationQueue.Name, rabbitmq.DeclareQueue(ch, rabbitmq.NotificationQueue)
}

// startConsumer runs a consumer until the test ends.
func startConsumer(t *testing.T, setup rabbitmq.SetupFunc, h rabbitmq.Handler) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = rabbitmq.Consume(ctx, quiet, rabbitmq.ConsumerConfig{URL: amqpURL, Name: t.Name(), RetryDelay: 50 * time.Millisecond}, setup, h)
	}()
	t.Cleanup(func() { cancel(); <-done })
}

// countingPublisher records how many times each event was published.
type countingPublisher struct {
	mu     sync.Mutex
	counts map[uuid.UUID]int
}

func (p *countingPublisher) Name() string { return "counting" }
func (p *countingPublisher) Close() error { return nil }
func (p *countingPublisher) Publish(_ context.Context, ev outbox.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.counts[ev.ID]++
	return nil
}

// --- Step 2: relay ----------------------------------------------------------

func TestRelay_PublishesToRabbitMQAndMarksEvents(t *testing.T) {
	reset(t)
	orders := createOrders(t, 3, "alice")

	pub := rabbitmq.NewPublisher(amqpURL, quiet, rabbitmq.NotificationQueue)
	defer pub.Close()

	n, err := relay.New(pool, pub, quiet, relay.Config{}).ProcessBatch(context.Background())
	if err != nil || n != 3 {
		t.Fatalf("ProcessBatch = %d, %v; want 3, nil", n, err)
	}
	if got := scalar(t, `SELECT count(*) FROM outbox WHERE published_at IS NULL`); got != 0 {
		t.Errorf("%d events still unpublished", got)
	}

	// The messages are in the durable queue, in order, with the event ID as MessageId.
	withChannel(t, func(ch *amqp.Channel) {
		for i, o := range orders {
			d, ok, err := ch.Get(rabbitmq.NotificationQueue.Name, true)
			if err != nil || !ok {
				t.Fatalf("message %d: ok=%v err=%v", i, ok, err)
			}
			var ev outbox.Event
			if err := json.Unmarshal(d.Body, &ev); err != nil {
				t.Fatal(err)
			}
			if ev.AggregateID != o.ID || d.MessageId != ev.ID.String() || d.Type != "order.created" {
				t.Errorf("message %d: aggregate=%s msgid=%s type=%s; want order %s", i, ev.AggregateID, d.MessageId, d.Type, o.ID)
			}
			if d.DeliveryMode != amqp.Persistent {
				t.Errorf("message %d is not persistent", i)
			}
		}
	})
}

func TestRelay_BrokerDownKeepsEventsInOutbox(t *testing.T) {
	reset(t)
	createOrders(t, 2, "bob")

	down := rabbitmq.NewPublisher("amqp://guest:guest@127.0.0.1:1/", quiet) // nothing listens on port 1
	_, err := relay.New(pool, down, quiet, relay.Config{}).ProcessBatch(context.Background())
	if err == nil {
		t.Fatal("expected a publish error while the broker is down")
	}
	// Nothing lost: both events still pending, the failure recorded on the first.
	if got := scalar(t, `SELECT count(*) FROM outbox WHERE published_at IS NULL`); got != 2 {
		t.Fatalf("unpublished = %d, want 2", got)
	}
	if got := scalar(t, `SELECT count(*) FROM outbox WHERE attempts = 1 AND last_error IS NOT NULL`); got != 1 {
		t.Errorf("failed attempts recorded on %d rows, want 1 (relay stops at first failure)", got)
	}

	// Broker "comes back": everything is published.
	up := rabbitmq.NewPublisher(amqpURL, quiet, rabbitmq.NotificationQueue)
	defer up.Close()
	if n, err := relay.New(pool, up, quiet, relay.Config{}).ProcessBatch(context.Background()); err != nil || n != 2 {
		t.Fatalf("after recovery: %d, %v", n, err)
	}
	if got := queueDepth(t, rabbitmq.NotificationQueue.Name); got != 2 {
		t.Errorf("queue depth = %d, want 2", got)
	}
}

// Several relay instances drain the same outbox concurrently. FOR UPDATE
// SKIP LOCKED must hand every event to exactly one of them.
func TestRelay_ConcurrentInstancesPublishEachEventOnce(t *testing.T) {
	reset(t)
	createOrders(t, 60, "carol")

	pub := &countingPublisher{counts: map[uuid.UUID]int{}}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := relay.New(pool, pub, quiet, relay.Config{BatchSize: 7})
			for {
				n, err := r.ProcessBatch(context.Background())
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()

	if len(pub.counts) != 60 {
		t.Fatalf("published %d distinct events, want 60", len(pub.counts))
	}
	for id, c := range pub.counts {
		if c != 1 {
			t.Errorf("event %s published %d times", id, c)
		}
	}
}

// --- Step 3: idempotent consumer + DLQ -------------------------------------

func TestNotification_DuplicateDeliveryHasOneEffect(t *testing.T) {
	reset(t)
	o := createOrders(t, 1, "dave")[0]
	ev, err := o.CreatedEvent()
	if err != nil {
		t.Fatal(err)
	}

	h := notification.NewHandler(pool, quiet)
	for range 3 { // at-least-once: the same event arrives three times
		if err := h.HandleEvent(context.Background(), ev); err != nil {
			t.Fatal(err)
		}
	}
	if got := scalar(t, `SELECT count(*) FROM notifications WHERE event_id = $1`, ev.ID); got != 1 {
		t.Errorf("notifications = %d, want 1", got)
	}
}

func TestNotification_FailedHandlingLeavesNoInboxRow(t *testing.T) {
	reset(t)
	o := createOrders(t, 1, "erin")[0]
	ev, _ := o.CreatedEvent()

	h := notification.NewHandler(pool, quiet)
	h.FailCustomerID = "erin"
	if err := h.HandleEvent(context.Background(), ev); err == nil {
		t.Fatal("expected simulated failure")
	}
	// The inbox insert was rolled back with the failed side effect, so a
	// retry is not mistaken for a duplicate.
	if got := scalar(t, `SELECT count(*) FROM processed_messages`); got != 0 {
		t.Fatalf("inbox rows = %d after failure, want 0", got)
	}

	h.FailCustomerID = ""
	if err := h.HandleEvent(context.Background(), ev); err != nil {
		t.Fatal(err)
	}
	if got := scalar(t, `SELECT count(*) FROM notifications`); got != 1 {
		t.Errorf("notifications = %d after retry, want 1", got)
	}
}

func TestNotification_PoisonMessageIsDeadLettered(t *testing.T) {
	reset(t)
	startConsumer(t, notificationSetup, notification.NewHandler(pool, quiet).Handle)

	withChannel(t, func(ch *amqp.Channel) {
		if err := ch.Publish(rabbitmq.ExchangeEvents, "order.created", false, false,
			amqp.Publishing{MessageId: "poison", Body: []byte("not json")}); err != nil {
			t.Fatal(err)
		}
	})
	eventually(t, 10*time.Second, func() bool { return queueDepth(t, rabbitmq.NotificationQueue.DLQName()) == 1 }, "poison message in DLQ")
}

func TestNotification_TransientFailureIsRetriedThenDeadLettered(t *testing.T) {
	reset(t)
	h := notification.NewHandler(pool, quiet)
	h.FailCustomerID = "frank"
	startConsumer(t, notificationSetup, h.Handle)

	createOrders(t, 1, "frank")
	pub := rabbitmq.NewPublisher(amqpURL, quiet, rabbitmq.NotificationQueue)
	defer pub.Close()
	if _, err := relay.New(pool, pub, quiet, relay.Config{}).ProcessBatch(context.Background()); err != nil {
		t.Fatal(err)
	}

	eventually(t, 20*time.Second, func() bool { return queueDepth(t, rabbitmq.NotificationQueue.DLQName()) == 1 },
		"message dead-lettered after exceeding x-delivery-limit")
	if got := scalar(t, `SELECT count(*) FROM notifications`); got != 0 {
		t.Errorf("notifications = %d, want 0", got)
	}
}

// --- Steps 2–4 end to end ---------------------------------------------------

func TestPipeline_OrderReachesNotificationAndBrowser(t *testing.T) {
	reset(t)

	// stream-gateway: exclusive queue → hub → SSE.
	hub := gateway.NewHub(quiet, 16)
	mux := http.NewServeMux()
	hub.Register(mux, time.Hour)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	t.Cleanup(hub.Close)

	gatewayReady := make(chan struct{})
	var once sync.Once
	startConsumer(t, func(ch *amqp.Channel) (string, error) {
		if err := rabbitmq.DeclareExchanges(ch); err != nil {
			return "", err
		}
		q, err := ch.QueueDeclare("", false, true, true, false, nil)
		if err != nil {
			return "", err
		}
		defer once.Do(func() { close(gatewayReady) })
		return q.Name, ch.QueueBind(q.Name, "order.*", rabbitmq.ExchangeEvents, false, nil)
	}, func(_ context.Context, d amqp.Delivery) error {
		var ev outbox.Event
		if err := json.Unmarshal(d.Body, &ev); err != nil {
			return errors.Join(broker.ErrPermanent, err)
		}
		return hub.BroadcastEvent(ev)
	})
	<-gatewayReady

	resp, err := http.Get(srv.URL + "/events?customer_id=grace")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	sse := bufio.NewReader(resp.Body)
	if _, err := sse.ReadString('\n'); err != nil { // ": connected"
		t.Fatal(err)
	}

	// notification-service
	startConsumer(t, notificationSetup, notification.NewHandler(pool, quiet).Handle)

	// order-service + outbox-relay
	o := createOrders(t, 1, "grace")[0]
	pub := rabbitmq.NewPublisher(amqpURL, quiet, rabbitmq.NotificationQueue)
	defer pub.Close()
	if _, err := relay.New(pool, pub, quiet, relay.Config{}).ProcessBatch(context.Background()); err != nil {
		t.Fatal(err)
	}

	eventually(t, 10*time.Second, func() bool {
		return scalar(t, `SELECT count(*) FROM notifications WHERE order_id = $1`, o.ID) == 1
	}, "notification stored")

	gotEvent := make(chan string, 1)
	go func() {
		for {
			line, err := sse.ReadString('\n')
			if err != nil {
				return
			}
			if strings.HasPrefix(line, "data: ") {
				gotEvent <- line
				return
			}
		}
	}()
	select {
	case line := <-gotEvent:
		if !strings.Contains(line, o.ID.String()) {
			t.Errorf("SSE data does not reference order %s: %s", o.ID, line)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("browser never received the event over SSE")
	}
}
