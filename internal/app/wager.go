package app

import (
	"context"
	"errors"
	"time"

	"github.com/patrickkaua/wagering/internal/domain"
)

type ReferencePolicy struct {
	MaxAttempts int
	TTL         time.Duration
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

func DefaultReferencePolicy() ReferencePolicy {
	return ReferencePolicy{MaxAttempts: 8, TTL: 10 * time.Minute, BaseDelay: 2 * time.Second, MaxDelay: 5 * time.Minute}
}

func (p ReferencePolicy) delay(attempts int) time.Duration {
	d := p.BaseDelay
	for i := 1; i < attempts; i++ {
		d *= 2
		if d >= p.MaxDelay {
			return p.MaxDelay
		}
	}
	return d
}

type Deps struct {
	Tx           TxRunner
	Wallets      WalletRepository
	Transactions TransactionRepository
	Ledger       LedgerRepository
	Inbox        InboxRepository
	Outbox       OutboxRepository
	Clock        Clock
	Observer     Observer
	Reference    ReferencePolicy
}

type Service struct {
	Deps
}

func NewService(d Deps) *Service {
	if d.Clock == nil {
		d.Clock = SystemClock{}
	}
	if d.Observer == nil {
		d.Observer = NopObserver{}
	}
	if d.Reference == (ReferencePolicy{}) {
		d.Reference = DefaultReferencePolicy()
	}
	return &Service{Deps: d}
}

type SubmitInput struct {
	Operation      domain.ExternalOperation
	IdempotencyKey string
	CorrelationID  string
}

type SubmitResult struct {
	Transaction *domain.WagerTransaction
	Replay      bool
}

// Submit applies an external operation exactly once. It joins an existing transaction when the context carries one.
func (s *Service) Submit(ctx context.Context, in SubmitInput) (SubmitResult, error) {
	var out SubmitResult
	err := s.Tx.WithinTx(ctx, func(ctx context.Context) error {
		res, err := s.submit(ctx, in)
		if err != nil {
			return err
		}
		out = res
		return nil
	})
	if err != nil {
		return SubmitResult{}, err
	}
	s.Observer.TransactionOutcome(out.Transaction.Kind(), out.Transaction.Status(), out.Replay)
	return out, nil
}

// SubmitFromMessage is Submit guarded by the inbox, all in one SQL transaction.
func (s *Service) SubmitFromMessage(ctx context.Context, consumer, messageID, payloadHash string, in SubmitInput) (SubmitResult, error) {
	var out SubmitResult
	err := s.Tx.WithinTx(ctx, func(ctx context.Context) error {
		now := s.Clock.Now()
		status, err := s.Inbox.Begin(ctx, consumer, messageID, payloadHash, now)
		if err != nil {
			return err
		}
		switch status {
		case InboxConflict:
			return ErrPoisonMessage
		case InboxDuplicate:
			res, err := s.replayByKey(ctx, in)
			if err != nil {
				return err
			}
			out = res
			return nil
		}
		res, err := s.submit(ctx, in)
		if err != nil {
			return err
		}
		out = res
		return s.Inbox.Complete(ctx, consumer, messageID, now)
	})
	if err != nil {
		return SubmitResult{}, err
	}
	s.Observer.TransactionOutcome(out.Transaction.Kind(), out.Transaction.Status(), out.Replay)
	return out, nil
}

func (s *Service) replayByKey(ctx context.Context, in SubmitInput) (SubmitResult, error) {
	existing, err := s.Transactions.GetByIdempotencyKey(ctx, in.Operation.ProviderID, in.IdempotencyKey)
	if err != nil {
		return SubmitResult{}, err
	}
	return SubmitResult{Transaction: existing, Replay: true}, nil
}

func (s *Service) submit(ctx context.Context, in SubmitInput) (SubmitResult, error) {
	now := s.Clock.Now()
	t, err := domain.NewExternalTransaction(domain.NewID(), in.Operation, in.IdempotencyKey, now)
	if err != nil {
		return SubmitResult{}, err
	}
	// Replays are answered from committed data without touching the wallet lock.
	found, err := s.Transactions.GetByIdempotencyKey(ctx, t.ProviderID(), t.IdempotencyKey())
	if err == nil {
		return s.matchExisting(found, t)
	}
	if !errors.Is(err, ErrNotFound) {
		return SubmitResult{}, err
	}
	// The wallet row is locked before the transaction row is inserted: the insert takes FOR KEY SHARE on the
	// wallet through the foreign key, and upgrading that to FOR UPDATE afterwards would deadlock concurrent writers.
	wallet, err := s.Wallets.GetForUpdate(ctx, t.WalletID())
	if errors.Is(err, ErrNotFound) {
		return SubmitResult{}, ErrWalletNotFound
	}
	if err != nil {
		return SubmitResult{}, err
	}
	inserted, err := s.Transactions.InsertExternal(ctx, t)
	if err != nil {
		return SubmitResult{}, err
	}
	if !inserted {
		return s.existing(ctx, t)
	}
	if err := s.apply(ctx, wallet, t, in.CorrelationID); err != nil {
		return SubmitResult{}, err
	}
	return SubmitResult{Transaction: t}, nil
}

func (s *Service) existing(ctx context.Context, t *domain.WagerTransaction) (SubmitResult, error) {
	found, err := s.Transactions.GetByIdempotencyKey(ctx, t.ProviderID(), t.IdempotencyKey())
	if errors.Is(err, ErrNotFound) {
		return SubmitResult{}, ErrDuplicateExternal
	}
	if err != nil {
		return SubmitResult{}, err
	}
	return s.matchExisting(found, t)
}

func (s *Service) matchExisting(found, t *domain.WagerTransaction) (SubmitResult, error) {
	if found.PayloadHash() != t.PayloadHash() {
		s.Observer.IdempotencyConflict()
		return SubmitResult{}, ErrIdempotencyConflict
	}
	return SubmitResult{Transaction: found, Replay: true}, nil
}

// apply decides the outcome of t under the wallet lock and persists it. t must be PENDING or PENDING_REFERENCE.
func (s *Service) apply(ctx context.Context, w *domain.Wallet, t *domain.WagerTransaction, correlationID string) error {
	now := s.Clock.Now()
	if w.PlayerID() != t.PlayerID() {
		return s.reject(ctx, t, domain.CodePlayerMismatch, correlationID, now)
	}
	if w.Currency() != t.Money().Currency() {
		return s.reject(ctx, t, domain.CodeCurrencyMismatch, correlationID, now)
	}
	switch t.Kind() {
	case domain.KindLoss:
		return s.settle(ctx, w, t, nil, correlationID, now)
	case domain.KindBet:
		return s.move(ctx, w, t, domain.Debit, domain.CodeInsufficientFunds, correlationID, now)
	case domain.KindWin:
		if t.ReferenceExternalID() == "" {
			return s.move(ctx, w, t, domain.Credit, "", correlationID, now)
		}
	}
	return s.applyReferenced(ctx, w, t, correlationID, now)
}

func (s *Service) applyReferenced(ctx context.Context, w *domain.Wallet, t *domain.WagerTransaction, correlationID string, now time.Time) error {
	ref, err := s.Transactions.GetByExternalID(ctx, t.ProviderID(), t.ReferenceExternalID())
	if errors.Is(err, ErrNotFound) {
		return s.awaitReference(ctx, t, domain.CodeReferenceNotFound, correlationID, now)
	}
	if err != nil {
		return err
	}
	switch ref.Status() {
	case domain.StatusPending, domain.StatusPendingReference:
		return s.awaitReference(ctx, t, domain.CodeReferenceNotProcessed, correlationID, now)
	case domain.StatusRejected, domain.StatusFailed:
		return s.reject(ctx, t, domain.CodeReferenceNotProcessed, correlationID, now)
	}
	direction, rejection := t.CheckReference(ref)
	if rejection != nil {
		return s.reject(ctx, t, rejection.Code, correlationID, now)
	}
	if t.Kind().IsReversal() {
		_, err := s.Transactions.FindProcessedReversal(ctx, ref.ID())
		if err == nil {
			return s.reject(ctx, t, domain.CodeAlreadyReversed, correlationID, now)
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
	}
	t.ResolveReference(ref.ID())
	code := domain.CodeReversalInsufficientFunds
	return s.move(ctx, w, t, direction, code, correlationID, now)
}

func (s *Service) move(ctx context.Context, w *domain.Wallet, t *domain.WagerTransaction, direction domain.Direction, insufficient domain.FailureCode, correlationID string, now time.Time) error {
	entry, err := w.Apply(direction, t.ID(), t.Money(), now)
	if errors.Is(err, domain.ErrInsufficientFunds) {
		return s.reject(ctx, t, insufficient, correlationID, now)
	}
	if errors.Is(err, domain.ErrMoneyOverflow) {
		return s.reject(ctx, t, domain.CodeBalanceLimitExceeded, correlationID, now)
	}
	if err != nil {
		return err
	}
	return s.settle(ctx, w, t, &entry, correlationID, now)
}

func (s *Service) settle(ctx context.Context, w *domain.Wallet, t *domain.WagerTransaction, entry *domain.LedgerEntry, correlationID string, now time.Time) error {
	if err := t.MarkProcessed(w.Balance(), now); err != nil {
		return err
	}
	events := make([]domain.Event, 0, 2)
	if entry != nil {
		if err := s.Wallets.Update(ctx, w); err != nil {
			return err
		}
		if err := s.Ledger.Append(ctx, *entry); err != nil {
			return err
		}
	}
	processed, err := domain.NewWagerTransactionProcessedEvent(t, correlationID, now)
	if err != nil {
		return err
	}
	events = append(events, processed)
	if entry != nil {
		changed, err := domain.NewWalletBalanceChangedEvent(*entry, w.Version(), correlationID, now)
		if err != nil {
			return err
		}
		events = append(events, changed)
	}
	if err := s.Transactions.Update(ctx, t); err != nil {
		return err
	}
	return s.Outbox.Add(ctx, events...)
}

func (s *Service) reject(ctx context.Context, t *domain.WagerTransaction, code domain.FailureCode, correlationID string, now time.Time) error {
	if err := t.Reject(code, now); err != nil {
		return err
	}
	if err := s.Transactions.Update(ctx, t); err != nil {
		return err
	}
	ev, err := domain.NewWagerTransactionRejectedEvent(t, correlationID, now)
	if err != nil {
		return err
	}
	return s.Outbox.Add(ctx, ev)
}

// awaitReference parks t, or rejects it once the retry budget is spent. exhausted is the code used on expiry.
func (s *Service) awaitReference(ctx context.Context, t *domain.WagerTransaction, exhausted domain.FailureCode, correlationID string, now time.Time) error {
	p := s.Reference
	expired := t.Attempts() >= p.MaxAttempts
	if first := t.FirstPendingAt(); first != nil && now.Sub(*first) >= p.TTL {
		expired = true
	}
	if expired {
		return s.reject(ctx, t, exhausted, correlationID, now)
	}
	firstTime := t.Attempts() == 0
	next := now.Add(p.delay(t.Attempts() + 1))
	if err := t.AwaitReference(next, now); err != nil {
		return err
	}
	if err := s.Transactions.Update(ctx, t); err != nil {
		return err
	}
	if !firstTime {
		return nil
	}
	ev, err := domain.NewWagerTransactionPendingReferenceEvent(t, correlationID, now)
	if err != nil {
		return err
	}
	return s.Outbox.Add(ctx, ev)
}

// ResolveNextPending processes one due PENDING_REFERENCE transaction. It returns false when none is due.
// An invariant violation that retrying cannot fix marks the transaction FAILED for audit instead of looping forever.
func (s *Service) ResolveNextPending(ctx context.Context) (bool, error) {
	found := false
	var claimed domain.ID
	err := s.Tx.WithinTx(ctx, func(ctx context.Context) error {
		found, claimed = false, ""
		now := s.Clock.Now()
		t, err := s.Transactions.ClaimNextDue(ctx, now)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		found, claimed = true, t.ID()
		w, err := s.Wallets.GetForUpdate(ctx, t.WalletID())
		if err != nil {
			return err
		}
		return s.apply(ctx, w, t, string(t.ID()))
	})
	if err != nil && claimed != "" && isPermanent(err) {
		if ferr := s.failPermanently(ctx, claimed); ferr != nil {
			return found, ferr
		}
		return found, nil
	}
	return found, err
}

func isPermanent(err error) bool {
	return errors.Is(err, domain.ErrInvalidInput) || errors.Is(err, domain.ErrInvalidMoney) ||
		errors.Is(err, domain.ErrInvalidTransition) || errors.Is(err, domain.ErrCurrencyMismatch)
}

func (s *Service) failPermanently(ctx context.Context, id domain.ID) error {
	return s.Tx.WithinTx(ctx, func(ctx context.Context) error {
		t, err := s.Transactions.GetByID(ctx, id)
		if err != nil {
			return err
		}
		if t.Status().IsTerminal() {
			return nil
		}
		if err := t.Fail(domain.CodeInternalFailure, s.Clock.Now()); err != nil {
			return err
		}
		return s.Transactions.Update(ctx, t)
	})
}
