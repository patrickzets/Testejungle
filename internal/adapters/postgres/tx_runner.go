package postgres

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/patrickzets/Testejungle/internal/app"
)

const (
	defaultMaxRetries     = 5
	defaultInitialBackoff = 10 * time.Millisecond
	defaultMaxBackoff     = 200 * time.Millisecond

	pgErrSerializationFailure = "40001"
	pgErrDeadlockDetected     = "40P01"
)

// TxRunnerOption configures the TxRunner instance.
type TxRunnerOption func(*TxRunner)

// WithObserver attaches an app.Observer for concurrency metrics.
func WithObserver(obs app.Observer) TxRunnerOption {
	return func(r *TxRunner) {
		r.obs = obs
	}
}

// WithMaxRetries sets the maximum number of attempts for retryable errors.
func WithMaxRetries(retries int) TxRunnerOption {
	return func(r *TxRunner) {
		if retries > 0 {
			r.maxRetries = retries
		}
	}
}

// WithBackoff sets the initial and maximum backoff for retries.
func WithBackoff(initial, max time.Duration) TxRunnerOption {
	return func(r *TxRunner) {
		if initial > 0 {
			r.initialBackoff = initial
		}
		if max > 0 {
			r.maxBackoff = max
		}
	}
}

// WithTxOptions configures pgx transaction options (e.g. IsolationLevel).
func WithTxOptions(opts pgx.TxOptions) TxRunnerOption {
	return func(r *TxRunner) {
		r.txOptions = opts
	}
}

// TxRunner executes units of work within a PostgreSQL transaction and retries on serialization/deadlock errors.
type TxRunner struct {
	pool           *pgxpool.Pool
	obs            app.Observer
	maxRetries     int
	initialBackoff time.Duration
	maxBackoff     time.Duration
	txOptions      pgx.TxOptions
}

// NewTxRunner creates a new TxRunner attached to the given pool.
func NewTxRunner(pool *pgxpool.Pool, opts ...TxRunnerOption) *TxRunner {
	runner := &TxRunner{
		pool:           pool,
		maxRetries:     defaultMaxRetries,
		initialBackoff: defaultInitialBackoff,
		maxBackoff:     defaultMaxBackoff,
		txOptions:      pgx.TxOptions{},
	}
	for _, opt := range opts {
		opt(runner)
	}
	return runner
}

// WithinTx runs the supplied function inside a database transaction.
// If a transaction is already present in the context, the function is executed within that transaction.
// Otherwise, a new transaction is started and placed in the context.
// If a serialization failure (40001) or deadlock (40P01) occurs, the transaction is retried up to maxRetries.
func (r *TxRunner) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if _, ok := TxFromContext(ctx); ok {
		return fn(ctx)
	}

	var lastErr error
	for attempt := 0; attempt < r.maxRetries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err := r.execTx(ctx, fn)
		if err == nil {
			return nil
		}

		lastErr = err
		if !IsRetryable(err) || attempt == r.maxRetries-1 {
			return err
		}

		if r.obs != nil {
			r.obs.ConcurrencyRetry()
		}

		backoff := r.backoffDuration(attempt)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}

	return lastErr
}

func (r *TxRunner) execTx(ctx context.Context, fn func(ctx context.Context) error) (err error) {
	tx, err := r.pool.BeginTx(ctx, r.txOptions)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(ctx)
			panic(p)
		} else if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	txCtx := ContextWithTx(ctx, tx)
	if err = fn(txCtx); err != nil {
		return err
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	return nil
}

// IsRetryable determines whether a PostgreSQL error represents a transient serialization conflict or deadlock.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == pgErrSerializationFailure || pgErr.Code == pgErrDeadlockDetected
	}
	return false
}

func (r *TxRunner) backoffDuration(attempt int) time.Duration {
	backoff := r.initialBackoff * time.Duration(1<<attempt)
	if backoff > r.maxBackoff {
		backoff = r.maxBackoff
	}
	jitter := time.Duration(rand.Int63n(int64(backoff)/2 + 1))
	return backoff/2 + jitter
}
