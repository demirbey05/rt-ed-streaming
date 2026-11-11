//go:build integration

// Integration tests run against a real Postgres started by testcontainers.
// Run with: make test-integration (requires Docker).
package order

import (
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
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"rt-ed-streaming/internal/db"
)

var (
	testPool *pgxpool.Pool
	testDSN  string
)

func TestMain(m *testing.M) {
	os.Exit(runWithPostgres(m))
}

func runWithPostgres(m *testing.M) int {
	ctx := context.Background()

	ctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("streaming"),
		tcpostgres.WithUsername("app"),
		tcpostgres.WithPassword("app"),
		tcpostgres.BasicWaitStrategies(),
	)
	if err != nil {
		log.Printf("start postgres container: %v", err)
		return 1
	}
	defer func() { _ = testcontainers.TerminateContainer(ctr) }()

	testDSN, err = ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Printf("connection string: %v", err)
		return 1
	}
	if err := db.MigrateUp(testDSN); err != nil {
		log.Printf("migrate: %v", err)
		return 1
	}

	testPool, err = pgxpool.New(ctx, testDSN)
	if err != nil {
		log.Printf("pool: %v", err)
		return 1
	}
	defer testPool.Close()

	return m.Run()
}

// --- helpers ---------------------------------------------------------------

func resetDB(t *testing.T) {
	t.Helper()
	if _, err := testPool.Exec(context.Background(), "TRUNCATE orders, outbox, idempotency_keys"); err != nil {
		t.Fatal(err)
	}
}

func countRows(t *testing.T, table string) int {
	t.Helper()
	var n int
	if err := testPool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func mustExec(t *testing.T, sql string) {
	t.Helper()
	if _, err := testPool.Exec(context.Background(), sql); err != nil {
		t.Fatal(err)
	}
}

var validInput = CreateInput{CustomerID: "c-1", AmountCents: 1999, Currency: "EUR"}

// --- Step 1.6: transactional outbox ---------------------------------------

func TestCreate_WritesOrderAndOutboxInOneTransaction(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	svc := NewService(testPool)

	res, err := svc.Create(ctx, validInput)
	if err != nil {
		t.Fatal(err)
	}
	o := res.Order

	if got := countRows(t, "orders"); got != 1 {
		t.Fatalf("orders = %d, want 1", got)
	}

	var (
		eventType   string
		aggregateID uuid.UUID
		payload     []byte
		createdAt   time.Time
		publishedAt *time.Time
	)
	err = testPool.QueryRow(ctx,
		`SELECT event_type, aggregate_id, payload, created_at, published_at FROM outbox`,
	).Scan(&eventType, &aggregateID, &payload, &createdAt, &publishedAt)
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}

	if eventType != EventTypeCreated || aggregateID != o.ID {
		t.Errorf("outbox row = %s/%s, want %s/%s", eventType, aggregateID, EventTypeCreated, o.ID)
	}
	if publishedAt != nil {
		t.Errorf("published_at = %v, want NULL", publishedAt)
	}
	if createdAt.IsZero() {
		t.Error("outbox created_at is zero")
	}

	var p OrderCreatedPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if p.OrderID != o.ID || p.AmountCents != o.AmountCents || p.Currency != o.Currency {
		t.Errorf("payload = %+v, want order %+v", p, o)
	}

	// Round trip: what Create returned is exactly what Get reads back.
	got, err := svc.Get(ctx, o.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got != o {
		t.Errorf("Get = %+v\nwant  %+v", got, o)
	}
}

func TestCreate_RollsBackOrderWhenOutboxInsertFails(t *testing.T) {
	resetDB(t)

	// Make every outbox insert fail, after the order insert has succeeded.
	mustExec(t, `CREATE FUNCTION fail_outbox() RETURNS trigger AS $$
		BEGIN RAISE EXCEPTION 'simulated outbox failure'; END $$ LANGUAGE plpgsql`)
	mustExec(t, `CREATE TRIGGER fail_outbox BEFORE INSERT ON outbox
		FOR EACH ROW EXECUTE FUNCTION fail_outbox()`)
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DROP TRIGGER IF EXISTS fail_outbox ON outbox; DROP FUNCTION IF EXISTS fail_outbox()`)
	})

	_, err := NewService(testPool).Create(context.Background(), validInput)
	if err == nil || !strings.Contains(err.Error(), "simulated outbox failure") {
		t.Fatalf("err = %v, want simulated outbox failure", err)
	}
	if got := countRows(t, "orders"); got != 0 {
		t.Errorf("orders = %d after failed outbox insert, want 0 (rollback)", got)
	}
	if got := countRows(t, "outbox"); got != 0 {
		t.Errorf("outbox = %d, want 0", got)
	}
}

func TestCreate_InvalidInputWritesNothing(t *testing.T) {
	resetDB(t)

	_, err := NewService(testPool).Create(context.Background(), CreateInput{CustomerID: "c-1", AmountCents: -1, Currency: "EUR"})
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("err = %v, want ErrValidation", err)
	}
	if countRows(t, "orders")+countRows(t, "outbox") != 0 {
		t.Error("invalid input wrote rows")
	}
}

func TestGet_UnknownIDReturnsNotFound(t *testing.T) {
	_, err := NewService(testPool).Get(context.Background(), uuid.New())
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// --- Step 1.8: idempotency ------------------------------------------------

func TestCreate_IdempotencyKeyReplaysOriginalOrder(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	svc := NewService(testPool)

	in := validInput
	in.IdempotencyKey = "key-1"

	first, err := svc.Create(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	in.Currency = "eur" // normalizes to the same request
	second, err := svc.Create(ctx, in)
	if err != nil {
		t.Fatal(err)
	}

	if first.Replayed || !second.Replayed {
		t.Errorf("Replayed = %v/%v, want false/true", first.Replayed, second.Replayed)
	}
	if first.Order != second.Order {
		t.Errorf("replay returned %+v, want %+v", second.Order, first.Order)
	}
	if o, e := countRows(t, "orders"), countRows(t, "outbox"); o != 1 || e != 1 {
		t.Errorf("orders=%d outbox=%d, want 1/1", o, e)
	}
}

func TestCreate_IdempotencyKeyReuseWithDifferentBodyFails(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	svc := NewService(testPool)

	in := validInput
	in.IdempotencyKey = "key-1"
	if _, err := svc.Create(ctx, in); err != nil {
		t.Fatal(err)
	}

	in.AmountCents = 1
	_, err := svc.Create(ctx, in)
	if !errors.Is(err, ErrIdempotencyKeyReuse) {
		t.Fatalf("err = %v, want ErrIdempotencyKeyReuse", err)
	}
	if got := countRows(t, "orders"); got != 1 {
		t.Errorf("orders = %d, want 1", got)
	}
}

// Many clients retrying the same request at the same moment must still
// produce exactly one order and one event. This exercises the slow path
// (unique index + ON CONFLICT DO NOTHING), not just the pre-check.
func TestCreate_ConcurrentRequestsWithSameKeyCreateOneOrder(t *testing.T) {
	resetDB(t)
	ctx := context.Background()
	svc := NewService(testPool)

	in := validInput
	in.IdempotencyKey = "key-concurrent"

	const n = 20
	var (
		wg      sync.WaitGroup
		start   = make(chan struct{})
		results = make([]CreateResult, n)
		errs    = make([]error, n)
	)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i], errs[i] = svc.Create(ctx, in)
		}()
	}
	close(start)
	wg.Wait()

	replayed := 0
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("request %d: %v", i, errs[i])
		}
		if results[i].Order.ID != results[0].Order.ID {
			t.Fatalf("request %d got order %s, request 0 got %s", i, results[i].Order.ID, results[0].Order.ID)
		}
		if results[i].Replayed {
			replayed++
		}
	}
	if replayed != n-1 {
		t.Errorf("replayed = %d, want %d", replayed, n-1)
	}
	if o, e, k := countRows(t, "orders"), countRows(t, "outbox"), countRows(t, "idempotency_keys"); o != 1 || e != 1 || k != 1 {
		t.Errorf("orders=%d outbox=%d keys=%d, want 1/1/1", o, e, k)
	}
}

// --- Step 1.7 + 1.8 over real HTTP ----------------------------------------

func TestHTTP_CreateWithIdempotencyKeyEndToEnd(t *testing.T) {
	resetDB(t)

	mux := http.NewServeMux()
	NewHandler(NewService(testPool), slog.New(slog.NewTextHandler(io.Discard, nil))).Register(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	post := func() *http.Response {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/orders",
			strings.NewReader(`{"customer_id":"c-1","amount_cents":1999,"currency":"EUR"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "http-key")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp
	}

	first, second := post(), post()
	if first.StatusCode != http.StatusCreated || second.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d/%d, want 201/201", first.StatusCode, second.StatusCode)
	}
	if second.Header.Get("Idempotent-Replayed") != "true" {
		t.Error("second response is not marked as replayed")
	}
	if first.Header.Get("Location") != second.Header.Get("Location") {
		t.Error("replay returned a different Location")
	}

	resp, err := http.Get(srv.URL + first.Header.Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET created order: status = %d", resp.StatusCode)
	}
	if got := countRows(t, "orders"); got != 1 {
		t.Errorf("orders = %d, want 1", got)
	}
}

// --- Step 1.1: migrations -------------------------------------------------

// Every up migration must have a working down. Runs last-ish but leaves the
// schema fully migrated again, so test order does not matter.
func TestMigrations_DownThenUp(t *testing.T) {
	if err := db.MigrateDown(testDSN); err != nil {
		t.Fatalf("down: %v", err)
	}
	var tables int
	_ = testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM information_schema.tables
		 WHERE table_schema = 'public' AND table_name IN ('orders','outbox','idempotency_keys')`).Scan(&tables)
	if tables != 0 {
		t.Errorf("%d tables left after down", tables)
	}
	if err := db.MigrateUp(testDSN); err != nil {
		t.Fatalf("up again: %v", err)
	}
}
