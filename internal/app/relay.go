package app

import (
	"context"
	"fmt"
	"time"

	"github.com/patrickzets/Testejungle/internal/domain"
)

type EventPublisher interface {
	Publish(ctx context.Context, e domain.Event) error
}

type RelayConfig struct {
	Owner     string
	BatchSize int
	Lease     time.Duration
	BaseDelay time.Duration
	MaxDelay  time.Duration
	// OnPublish is called once per publication attempt with its outcome.
	OnPublish func(success bool)
}

// OutboxRelay publishes claimed outbox rows. Several relays can run concurrently: rows are leased with SKIP LOCKED
// and an abandoned lease expires, so another instance republishes with the same event id.
type OutboxRelay struct {
	outbox    OutboxRepository
	publisher EventPublisher
	clock     Clock
	cfg       RelayConfig
}

func NewOutboxRelay(outbox OutboxRepository, publisher EventPublisher, clock Clock, cfg RelayConfig) *OutboxRelay {
	if clock == nil {
		clock = SystemClock{}
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 20
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = time.Second
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = time.Minute
	}
	return &OutboxRelay{outbox: outbox, publisher: publisher, clock: clock, cfg: cfg}
}

// RunOnce publishes one batch and reports whether any row was handled.
func (r *OutboxRelay) RunOnce(ctx context.Context) (bool, error) {
	records, err := r.outbox.Claim(ctx, r.cfg.Owner, r.clock.Now(), r.cfg.Lease, r.cfg.BatchSize)
	if err != nil {
		return false, fmt.Errorf("claim outbox: %w", err)
	}
	for _, rec := range records {
		if ctx.Err() != nil {
			return true, ctx.Err()
		}
		if err := r.publisher.Publish(ctx, rec.Event); err != nil {
			r.notify(false)
			next := r.clock.Now().Add(r.backoff(rec.Attempts + 1))
			if rerr := r.outbox.Retry(ctx, rec.Event.EventID, next); rerr != nil {
				return true, fmt.Errorf("schedule retry: %w", rerr)
			}
			continue
		}
		r.notify(true)
		if err := r.outbox.MarkPublished(ctx, rec.Event.EventID, r.clock.Now()); err != nil {
			return true, fmt.Errorf("mark published: %w", err)
		}
	}
	return len(records) > 0, nil
}

func (r *OutboxRelay) backoff(attempts int) time.Duration {
	d := r.cfg.BaseDelay
	for i := 1; i < attempts; i++ {
		d *= 2
		if d >= r.cfg.MaxDelay {
			return r.cfg.MaxDelay
		}
	}
	return d
}

func (r *OutboxRelay) notify(success bool) {
	if r.cfg.OnPublish != nil {
		r.cfg.OnPublish(success)
	}
}
