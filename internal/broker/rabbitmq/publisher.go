package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"rt-ed-streaming/internal/outbox"
)

// Publisher publishes events to ExchangeEvents with publisher confirms.
// It connects lazily and reconnects on the next Publish after any failure,
// so a broker outage only fails publishes; it never crashes the relay.
type Publisher struct {
	url    string
	log    *slog.Logger
	queues []QueueSpec // durable queues to declare so events are never unroutable

	mu      sync.Mutex
	conn    *amqp.Connection
	ch      *amqp.Channel
	returns chan amqp.Return
}

func NewPublisher(url string, log *slog.Logger, queues ...QueueSpec) *Publisher {
	return &Publisher{url: url, log: log, queues: queues}
}

func (p *Publisher) Name() string { return "rabbitmq" }

// Publish returns nil only after the broker confirms (acks) the message and
// did not return it as unroutable.
func (p *Publisher) Publish(ctx context.Context, ev outbox.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.connect(); err != nil {
		return err
	}

	body, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	confirm, err := p.ch.PublishWithDeferredConfirmWithContext(ctx,
		ExchangeEvents,
		ev.Type, // routing key
		true,    // mandatory: return the message if no queue is bound
		false,
		amqp.Publishing{
			MessageId:    ev.ID.String(),
			Type:         ev.Type,
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Timestamp:    ev.OccurredAt,
			AppId:        "outbox-relay",
			Headers: amqp.Table{
				"aggregate_type": ev.AggregateType,
				"aggregate_id":   ev.AggregateID.String(),
			},
			Body: body,
		})
	if err != nil {
		p.reset()
		return fmt.Errorf("publish: %w", err)
	}

	acked, err := confirm.WaitContext(ctx)
	if err != nil {
		p.reset()
		return fmt.Errorf("wait for confirm: %w", err)
	}
	if !acked {
		return errors.New("broker nacked the message")
	}

	// A mandatory message that could not be routed is returned *before*
	// its ack, so it is already waiting here if it happened.
	select {
	case r := <-p.returns:
		return fmt.Errorf("message returned as unroutable: %d %s", r.ReplyCode, r.ReplyText)
	default:
	}
	return nil
}

// Ping reports whether the broker is reachable (used by /healthz).
func (p *Publisher) Ping(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.connect()
}

func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reset()
	return nil
}

// connect (re)opens the connection and a confirm-mode channel if needed.
// Caller must hold p.mu.
func (p *Publisher) connect() error {
	if p.ch != nil && !p.ch.IsClosed() && p.conn != nil && !p.conn.IsClosed() {
		return nil
	}
	p.reset()

	conn, err := amqp.DialConfig(p.url, amqp.Config{Heartbeat: 10 * time.Second, Dial: amqp.DefaultDial(5 * time.Second)})
	if err != nil {
		return fmt.Errorf("dial rabbitmq: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("open channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		conn.Close()
		return fmt.Errorf("enable confirms: %w", err)
	}
	if err := DeclareExchanges(ch); err != nil {
		conn.Close()
		return err
	}
	for _, q := range p.queues {
		if err := DeclareQueue(ch, q); err != nil {
			conn.Close()
			return err
		}
	}

	p.conn, p.ch = conn, ch
	p.returns = ch.NotifyReturn(make(chan amqp.Return, 1))
	p.log.Info("connected to rabbitmq")
	return nil
}

// reset drops the current connection; the next Publish reconnects.
// Caller must hold p.mu.
func (p *Publisher) reset() {
	if p.conn != nil {
		_ = p.conn.Close()
	}
	p.conn, p.ch, p.returns = nil, nil, nil
}
