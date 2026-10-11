package platform

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Worker runs fn repeatedly until stopped. fn reports whether it made progress; when it did not, the worker sleeps.
type Worker struct {
	name     string
	interval time.Duration
	fn       func(ctx context.Context) (bool, error)
	log      *slog.Logger

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func NewWorker(name string, interval time.Duration, log *slog.Logger, fn func(ctx context.Context) (bool, error)) *Worker {
	return &Worker{name: name, interval: interval, fn: fn, log: log.With("worker", name)}
}

func (w *Worker) Start() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.done = make(chan struct{})
	go w.loop(ctx, w.done)
}

// Stop cancels the worker and waits for it to exit or for ctx to expire.
func (w *Worker) Stop(ctx context.Context) error {
	w.mu.Lock()
	cancel, done := w.cancel, w.done
	w.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		w.log.Info("worker stopped")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Worker) loop(ctx context.Context, done chan struct{}) {
	defer close(done)
	w.log.Info("worker started")
	for ctx.Err() == nil {
		progressed, err := w.fn(ctx)
		if err != nil && ctx.Err() == nil {
			w.log.Error("worker iteration failed", "error", err)
		}
		if progressed && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
		case <-time.After(w.interval):
		}
	}
}
