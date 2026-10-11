package app

import (
	"context"
	"errors"
	"time"

	"github.com/patrickkaua/wagering/internal/domain"
)

var (
	ErrNotFound            = errors.New("not found")
	ErrWalletNotFound      = errors.New("wallet not found")
	ErrWalletExists        = errors.New("wallet already exists for player and currency")
	ErrVersionConflict     = errors.New("wallet version conflict")
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different payload")
	ErrDuplicateExternal   = errors.New("external transaction already submitted under another idempotency key")
	ErrPoisonMessage       = errors.New("message id reused with a different payload")
)

// TxRunner runs fn inside one SQL transaction carried by the context. It must be reentrant:
// when ctx already carries a transaction, fn joins it.
type TxRunner interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

type WalletRepository interface {
	// Insert returns ErrWalletExists on a (player, currency) conflict.
	Insert(ctx context.Context, w *domain.Wallet) error
	// Get returns ErrNotFound.
	Get(ctx context.Context, id domain.ID) (*domain.Wallet, error)
	// GetForUpdate takes a row lock held until the transaction ends.
	GetForUpdate(ctx context.Context, id domain.ID) (*domain.Wallet, error)
	// Update persists balance and version guarded by w.LoadedVersion(); ErrVersionConflict when it no longer matches.
	Update(ctx context.Context, w *domain.Wallet) error
}

type TransactionRepository interface {
	// InsertExternal stores a PENDING external transaction. inserted is false when the idempotency key or
	// (provider, external id) already exists. ErrWalletNotFound when the wallet does not exist.
	InsertExternal(ctx context.Context, t *domain.WagerTransaction) (inserted bool, err error)
	InsertInternal(ctx context.Context, t *domain.WagerTransaction) error
	GetByID(ctx context.Context, id domain.ID) (*domain.WagerTransaction, error)
	// GetByIdempotencyKey is scoped by provider: two providers may use the same key string independently.
	GetByIdempotencyKey(ctx context.Context, providerID, key string) (*domain.WagerTransaction, error)
	GetByExternalID(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error)
	// FindProcessedReversal returns the PROCESSED REFUND/ROLLBACK that references refTxID, or ErrNotFound.
	FindProcessedReversal(ctx context.Context, refTxID domain.ID) (*domain.WagerTransaction, error)
	// Update persists status, failure code, result balance, attempts, schedule and reference id.
	Update(ctx context.Context, t *domain.WagerTransaction) error
	// ClaimNextDue locks one PENDING or PENDING_REFERENCE row whose next_attempt_at <= now using
	// FOR UPDATE SKIP LOCKED. ErrNotFound when none is available.
	ClaimNextDue(ctx context.Context, now time.Time) (*domain.WagerTransaction, error)
}

type LedgerPage struct {
	Entries    []domain.LedgerEntry
	NextCursor string
}

type Reconciliation struct {
	Stored     domain.Money
	Calculated domain.Money
	Entries    int
}

type LedgerRepository interface {
	Append(ctx context.Context, e domain.LedgerEntry) error
	// List orders by the stable sequence ascending. cursor is opaque and empty for the first page.
	List(ctx context.Context, walletID domain.ID, cursor string, limit int) (LedgerPage, error)
	// Reconcile reads the stored balance and the ledger sum in a single statement (one snapshot).
	Reconcile(ctx context.Context, walletID domain.ID) (Reconciliation, error)
}

type InboxStatus int

const (
	InboxNew InboxStatus = iota
	InboxDuplicate
	InboxConflict
)

type InboxRepository interface {
	// Begin registers (consumer, messageID). InboxDuplicate for the same hash already seen,
	// InboxConflict when the stored hash differs.
	Begin(ctx context.Context, consumer, messageID, payloadHash string, now time.Time) (InboxStatus, error)
	Complete(ctx context.Context, consumer, messageID string, now time.Time) error
}

type OutboxRecord struct {
	Event    domain.Event
	Attempts int
}

type OutboxRepository interface {
	Add(ctx context.Context, events ...domain.Event) error
	// Claim leases up to limit unpublished rows (SKIP LOCKED) for the given duration and commits the lease.
	Claim(ctx context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]OutboxRecord, error)
	MarkPublished(ctx context.Context, eventID domain.ID, now time.Time) error
	// Retry records a failed publication and schedules the next attempt, releasing the lease.
	Retry(ctx context.Context, eventID domain.ID, next time.Time) error
	// Lag returns the age of the oldest unpublished event, zero when empty.
	Lag(ctx context.Context, now time.Time) (time.Duration, error)
}

type Clock interface{ Now() time.Time }

type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

// Observer receives domain-level signals for metrics. Implementations must be safe for concurrent use.
type Observer interface {
	TransactionOutcome(kind domain.Kind, status domain.Status, replay bool)
	IdempotencyConflict()
	ConcurrencyRetry()
	ReconciliationDivergence()
}

type NopObserver struct{}

func (NopObserver) TransactionOutcome(domain.Kind, domain.Status, bool) {}
func (NopObserver) IdempotencyConflict()                                {}
func (NopObserver) ConcurrencyRetry()                                   {}
func (NopObserver) ReconciliationDivergence()                           {}
