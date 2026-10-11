package app_test

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/patrickkaua/wagering/internal/app"
	"github.com/patrickkaua/wagering/internal/domain"
)

type memStore struct {
	mu      sync.Mutex
	wallets map[domain.ID]*domain.Wallet
	txs     map[domain.ID]*domain.WagerTransaction
	ledger  []domain.LedgerEntry
	outbox  []domain.Event
	inbox   map[string]string
	now     time.Time
}

func newMemStore() *memStore {
	return &memStore{
		wallets: map[domain.ID]*domain.Wallet{}, txs: map[domain.ID]*domain.WagerTransaction{},
		inbox: map[string]string{}, now: time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC),
	}
}

func (m *memStore) Now() time.Time { return m.now }

func snapshotTx(t *domain.WagerTransaction) *domain.WagerTransaction {
	return domain.RehydrateTransaction(domain.PersistedTransaction{
		ID: t.ID(), Origin: t.Origin(), Operation: t.Operation(), IdempotencyKey: t.IdempotencyKey(),
		PayloadHash: t.PayloadHash(), WalletID: t.WalletID(), PlayerID: t.PlayerID(), Kind: t.Kind(), Money: t.Money(),
		ReferenceTxID: t.ReferenceTxID(), Status: t.Status(), FailureCode: t.FailureCode(), ResultBalance: t.ResultBalance(),
		Attempts: t.Attempts(), NextAttemptAt: t.NextAttemptAt(), FirstPendingAt: t.FirstPendingAt(),
		CreatedAt: t.CreatedAt(), UpdatedAt: t.UpdatedAt(),
	})
}

// WithinTx emulates a SQL transaction: any error restores the state captured on entry.
func (m *memStore) WithinTx(ctx context.Context, fn func(context.Context) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	txs := make(map[domain.ID]*domain.WagerTransaction, len(m.txs))
	for id, t := range m.txs {
		txs[id] = snapshotTx(t)
	}
	wallets := make(map[domain.ID]*domain.Wallet, len(m.wallets))
	for id, w := range m.wallets {
		wallets[id] = clone(w)
	}
	ledgerLen, outboxLen := len(m.ledger), len(m.outbox)
	if err := fn(ctx); err != nil {
		m.txs, m.wallets = txs, wallets
		m.ledger, m.outbox = m.ledger[:ledgerLen], m.outbox[:outboxLen]
		return err
	}
	return nil
}

func clone(w *domain.Wallet) *domain.Wallet {
	c, _ := domain.RehydrateWallet(w.ID(), w.PlayerID(), w.Balance(), w.Version(), w.CreatedAt(), w.UpdatedAt())
	return c
}

func (m *memStore) Insert(_ context.Context, w *domain.Wallet) error {
	for _, e := range m.wallets {
		if e.PlayerID() == w.PlayerID() && e.Currency() == w.Currency() {
			return app.ErrWalletExists
		}
	}
	m.wallets[w.ID()] = clone(w)
	return nil
}

func (m *memStore) Get(_ context.Context, id domain.ID) (*domain.Wallet, error) {
	w, ok := m.wallets[id]
	if !ok {
		return nil, app.ErrNotFound
	}
	return clone(w), nil
}

func (m *memStore) GetForUpdate(ctx context.Context, id domain.ID) (*domain.Wallet, error) {
	return m.Get(ctx, id)
}

func (m *memStore) Update(_ context.Context, w *domain.Wallet) error {
	stored := m.wallets[w.ID()]
	if stored.Version() != w.LoadedVersion() {
		return app.ErrVersionConflict
	}
	m.wallets[w.ID()] = clone(w)
	return nil
}

func (m *memStore) InsertExternal(_ context.Context, t *domain.WagerTransaction) (bool, error) {
	if _, ok := m.wallets[t.WalletID()]; !ok {
		return false, app.ErrWalletNotFound
	}
	for _, e := range m.txs {
		if e.ProviderID() == t.ProviderID() && e.IdempotencyKey() == t.IdempotencyKey() && e.Origin() == domain.OriginExternal ||
			e.ProviderID() == t.ProviderID() && e.ExternalID() == t.ExternalID() && e.Origin() == domain.OriginExternal {
			return false, nil
		}
	}
	m.txs[t.ID()] = t
	return true, nil
}

func (m *memStore) InsertInternal(_ context.Context, t *domain.WagerTransaction) error {
	m.txs[t.ID()] = t
	return nil
}

func (m *memStore) GetByID(_ context.Context, id domain.ID) (*domain.WagerTransaction, error) {
	if t, ok := m.txs[id]; ok {
		return t, nil
	}
	return nil, app.ErrNotFound
}

func (m *memStore) GetByIdempotencyKey(_ context.Context, providerID, key string) (*domain.WagerTransaction, error) {
	for _, t := range m.txs {
		if t.ProviderID() == providerID && t.IdempotencyKey() == key && t.Origin() == domain.OriginExternal {
			return t, nil
		}
	}
	return nil, app.ErrNotFound
}

func (m *memStore) GetByExternalID(_ context.Context, provider, ext string) (*domain.WagerTransaction, error) {
	for _, t := range m.txs {
		if t.ProviderID() == provider && t.ExternalID() == ext && t.Origin() == domain.OriginExternal {
			return t, nil
		}
	}
	return nil, app.ErrNotFound
}

func (m *memStore) FindProcessedReversal(_ context.Context, ref domain.ID) (*domain.WagerTransaction, error) {
	for _, t := range m.txs {
		if t.Kind().IsReversal() && t.Status() == domain.StatusProcessed && t.ReferenceTxID() == ref {
			return t, nil
		}
	}
	return nil, app.ErrNotFound
}

func (m *memStore) Update2(t *domain.WagerTransaction) { m.txs[t.ID()] = t }

func (m *memStore) ClaimNextDue(_ context.Context, now time.Time) (*domain.WagerTransaction, error) {
	for _, t := range m.txs {
		if t.Status() == domain.StatusPendingReference && t.NextAttemptAt() != nil && !t.NextAttemptAt().After(now) {
			return t, nil
		}
	}
	return nil, app.ErrNotFound
}

type txRepo struct{ *memStore }

func (r txRepo) Update(_ context.Context, t *domain.WagerTransaction) error {
	r.txs[t.ID()] = t
	return nil
}

type ledgerRepo struct{ *memStore }

func (r ledgerRepo) Append(_ context.Context, e domain.LedgerEntry) error {
	r.ledger = append(r.ledger, e)
	r.memStore.ledger = r.ledger
	return nil
}

func (r ledgerRepo) List(context.Context, domain.ID, string, int) (app.LedgerPage, error) {
	return app.LedgerPage{}, nil
}

func (r ledgerRepo) Reconcile(_ context.Context, id domain.ID) (app.Reconciliation, error) {
	w := r.wallets[id]
	calc, _ := domain.Zero(w.Currency())
	for _, e := range r.ledger {
		if e.WalletID() != id {
			continue
		}
		if e.Direction() == domain.Credit {
			calc, _ = calc.Add(e.Money())
		} else {
			calc, _ = calc.Sub(e.Money())
		}
	}
	return app.Reconciliation{Stored: w.Balance(), Calculated: calc, Entries: len(r.ledger)}, nil
}

type inboxRepo struct{ *memStore }

func (r inboxRepo) Begin(_ context.Context, consumer, id, hash string, _ time.Time) (app.InboxStatus, error) {
	key := consumer + "/" + id
	if prev, ok := r.inbox[key]; ok {
		if prev == hash {
			return app.InboxDuplicate, nil
		}
		return app.InboxConflict, nil
	}
	r.inbox[key] = hash
	return app.InboxNew, nil
}

func (r inboxRepo) Complete(context.Context, string, string, time.Time) error { return nil }

type outboxRepo struct{ *memStore }

func (r outboxRepo) Add(_ context.Context, events ...domain.Event) error {
	r.memStore.outbox = append(r.memStore.outbox, events...)
	return nil
}

func (r outboxRepo) Claim(context.Context, string, time.Time, time.Duration, int) ([]app.OutboxRecord, error) {
	return nil, nil
}
func (r outboxRepo) MarkPublished(context.Context, domain.ID, time.Time) error { return nil }
func (r outboxRepo) Retry(context.Context, domain.ID, time.Time) error         { return nil }
func (r outboxRepo) Lag(context.Context, time.Time) (time.Duration, error)     { return 0, nil }

type fixture struct {
	store    *memStore
	svc      *app.Service
	walletID domain.ID
	playerID domain.ID
}

func newFixture(t *testing.T, initial int64) *fixture {
	t.Helper()
	st := newMemStore()
	svc := app.NewService(app.Deps{
		Tx: st, Wallets: st, Transactions: txRepo{st}, Ledger: ledgerRepo{st}, Inbox: inboxRepo{st},
		Outbox: outboxRepo{st}, Clock: st,
	})
	player := domain.NewID()
	money, _ := domain.NewMoney(initial, "BRL")
	w, err := svc.OpenWallet(context.Background(), player, money, "corr")
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{store: st, svc: svc, walletID: w.ID(), playerID: player}
}

func (f *fixture) submit(t *testing.T, kind domain.Kind, ext string, minor int64, ref string) app.SubmitResult {
	t.Helper()
	res, err := f.submitErr(kind, ext, minor, ref)
	if err != nil {
		t.Fatalf("%s %s: %v", kind, ext, err)
	}
	return res
}

func (f *fixture) submitErr(kind domain.Kind, ext string, minor int64, ref string) (app.SubmitResult, error) {
	money, _ := domain.NewMoney(minor, "BRL")
	return f.svc.Submit(context.Background(), app.SubmitInput{
		Operation: domain.ExternalOperation{
			ProviderID: "provider-a", ExternalTransactionID: ext, PlayerID: f.playerID, WalletID: f.walletID,
			RoundID: "round-1", GameID: "game-1", Kind: kind, Money: money, ReferenceExternalTransactionID: ref,
		},
		IdempotencyKey: "provider-a:" + ext,
	})
}

func (f *fixture) balance(t *testing.T) int64 {
	t.Helper()
	w, _ := f.store.Get(context.Background(), f.walletID)
	return w.Balance().Minor()
}

func TestOpenWalletCreatesOpeningArtifacts(t *testing.T) {
	f := newFixture(t, 100000)
	if len(f.store.ledger) != 1 || len(f.store.outbox) != 2 {
		t.Fatalf("ledger=%d outbox=%d", len(f.store.ledger), len(f.store.outbox))
	}
	if f.store.outbox[0].EventType != domain.EventWagerTransactionProcessed || f.store.outbox[1].EventType != domain.EventWalletBalanceChanged {
		t.Fatalf("events: %s %s", f.store.outbox[0].EventType, f.store.outbox[1].EventType)
	}
	zero := newFixture(t, 0)
	if len(zero.store.ledger) != 0 || len(zero.store.outbox) != 0 || len(zero.store.txs) != 0 {
		t.Fatal("zero opening must not create transaction, ledger or events")
	}
}

func TestBetDebitsAndReplayReturnsOriginalBalance(t *testing.T) {
	f := newFixture(t, 100000)
	first := f.submit(t, domain.KindBet, "bet-1", 2500, "")
	if first.Transaction.Status() != domain.StatusProcessed || first.Replay || f.balance(t) != 97500 {
		t.Fatalf("first: %s replay=%v balance=%d", first.Transaction.Status(), first.Replay, f.balance(t))
	}
	f.submit(t, domain.KindBet, "bet-2", 1000, "")
	replay := f.submit(t, domain.KindBet, "bet-1", 2500, "")
	if !replay.Replay || replay.Transaction.ResultBalance().Minor() != 97500 || f.balance(t) != 96500 {
		t.Fatalf("replay: replay=%v result=%v balance=%d", replay.Replay, replay.Transaction.ResultBalance(), f.balance(t))
	}
	if len(f.store.ledger) != 3 {
		t.Fatalf("ledger entries = %d", len(f.store.ledger))
	}
}

func TestIdempotencyConflicts(t *testing.T) {
	f := newFixture(t, 100000)
	f.submit(t, domain.KindBet, "bet-1", 2500, "")
	if _, err := f.submitErr(domain.KindBet, "bet-1", 3000, ""); !errors.Is(err, app.ErrIdempotencyConflict) {
		t.Errorf("same key different payload: %v", err)
	}
	money, _ := domain.NewMoney(2500, "BRL")
	_, err := f.svc.Submit(context.Background(), app.SubmitInput{
		Operation: domain.ExternalOperation{
			ProviderID: "provider-a", ExternalTransactionID: "bet-1", PlayerID: f.playerID, WalletID: f.walletID,
			RoundID: "round-1", GameID: "game-1", Kind: domain.KindBet, Money: money,
		},
		IdempotencyKey: "another-key",
	})
	if !errors.Is(err, app.ErrDuplicateExternal) {
		t.Errorf("same external id other key: %v", err)
	}
	if f.balance(t) != 97500 {
		t.Errorf("balance changed: %d", f.balance(t))
	}
}

func TestInsufficientFundsIsRejectedWithoutLedger(t *testing.T) {
	f := newFixture(t, 10000)
	first := f.submit(t, domain.KindBet, "bet-1", 8000, "")
	second := f.submit(t, domain.KindBet, "bet-2", 8000, "")
	if first.Transaction.Status() != domain.StatusProcessed || second.Transaction.Status() != domain.StatusRejected ||
		second.Transaction.FailureCode() != domain.CodeInsufficientFunds {
		t.Fatalf("statuses: %s %s/%s", first.Transaction.Status(), second.Transaction.Status(), second.Transaction.FailureCode())
	}
	if f.balance(t) != 2000 || len(f.store.ledger) != 2 {
		t.Fatalf("balance=%d ledger=%d", f.balance(t), len(f.store.ledger))
	}
	if f.store.outbox[len(f.store.outbox)-1].EventType != domain.EventWagerTransactionRejected {
		t.Fatal("expected rejection event")
	}
}

func TestLossHasNoLedgerAndKeepsVersion(t *testing.T) {
	f := newFixture(t, 10000)
	before := len(f.store.ledger)
	res := f.submit(t, domain.KindLoss, "loss-1", 0, "")
	w, _ := f.store.Get(context.Background(), f.walletID)
	if res.Transaction.Status() != domain.StatusProcessed || len(f.store.ledger) != before || w.Version() != 1 {
		t.Fatalf("loss: %s ledger=%d version=%d", res.Transaction.Status(), len(f.store.ledger), w.Version())
	}
}

func TestRefundAndRollbackAreMutuallyExclusive(t *testing.T) {
	f := newFixture(t, 100000)
	f.submit(t, domain.KindBet, "bet-1", 2500, "")
	refund := f.submit(t, domain.KindRefund, "refund-1", 2500, "bet-1")
	if refund.Transaction.Status() != domain.StatusProcessed || f.balance(t) != 100000 {
		t.Fatalf("refund: %s balance=%d", refund.Transaction.Status(), f.balance(t))
	}
	rollback := f.submit(t, domain.KindRollback, "rollback-1", 2500, "bet-1")
	if rollback.Transaction.Status() != domain.StatusRejected || rollback.Transaction.FailureCode() != domain.CodeAlreadyReversed {
		t.Fatalf("rollback of refunded bet: %s/%s", rollback.Transaction.Status(), rollback.Transaction.FailureCode())
	}
	again := f.submit(t, domain.KindRefund, "refund-2", 2500, "bet-1")
	if again.Transaction.FailureCode() != domain.CodeAlreadyReversed || f.balance(t) != 100000 {
		t.Fatalf("second refund: %s balance=%d", again.Transaction.FailureCode(), f.balance(t))
	}
}

func TestRollbackOfWinNeedsFunds(t *testing.T) {
	f := newFixture(t, 10000)
	f.submit(t, domain.KindBet, "bet-1", 10000, "")
	f.submit(t, domain.KindWin, "win-1", 5000, "bet-1")
	f.submit(t, domain.KindBet, "bet-2", 5000, "")
	rollback := f.submit(t, domain.KindRollback, "rollback-1", 5000, "win-1")
	if rollback.Transaction.FailureCode() != domain.CodeReversalInsufficientFunds {
		t.Fatalf("expected reversal insufficient funds, got %s/%s", rollback.Transaction.Status(), rollback.Transaction.FailureCode())
	}
}

func TestReversalBeforeReferenceResolvesLater(t *testing.T) {
	f := newFixture(t, 100000)
	early := f.submit(t, domain.KindRefund, "refund-1", 2500, "bet-1")
	if early.Transaction.Status() != domain.StatusPendingReference || early.Transaction.Attempts() != 1 {
		t.Fatalf("early refund: %s attempts=%d", early.Transaction.Status(), early.Transaction.Attempts())
	}
	f.store.now = f.store.now.Add(time.Second)
	if progressed, err := f.svc.ResolveNextPending(context.Background()); err != nil || progressed {
		t.Fatalf("not due yet: progressed=%v err=%v", progressed, err)
	}

	f.submit(t, domain.KindBet, "bet-1", 2500, "")
	f.store.now = f.store.now.Add(time.Minute)
	progressed, err := f.svc.ResolveNextPending(context.Background())
	if err != nil || !progressed {
		t.Fatalf("resolve: progressed=%v err=%v", progressed, err)
	}
	got, _ := f.store.GetByID(context.Background(), early.Transaction.ID())
	if got.Status() != domain.StatusProcessed || f.balance(t) != 100000 {
		t.Fatalf("after resolve: %s balance=%d", got.Status(), f.balance(t))
	}
}

func TestPendingReferenceExpires(t *testing.T) {
	f := newFixture(t, 100000)
	early := f.submit(t, domain.KindRollback, "rollback-1", 2500, "ghost")
	for i := 0; i < 20; i++ {
		f.store.now = f.store.now.Add(6 * time.Minute)
		if _, err := f.svc.ResolveNextPending(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := f.store.GetByID(context.Background(), early.Transaction.ID())
	if got.Status() != domain.StatusRejected || got.FailureCode() != domain.CodeReferenceNotFound {
		t.Fatalf("expired: %s/%s", got.Status(), got.FailureCode())
	}
	last := f.store.outbox[len(f.store.outbox)-1]
	if last.EventType != domain.EventWagerTransactionRejected {
		t.Fatalf("last event: %s", last.EventType)
	}
}

func TestMessageInboxDeduplicatesAndDetectsPoison(t *testing.T) {
	f := newFixture(t, 100000)
	money, _ := domain.NewMoney(2500, "BRL")
	in := app.SubmitInput{
		Operation: domain.ExternalOperation{
			ProviderID: "provider-a", ExternalTransactionID: "bet-1", PlayerID: f.playerID, WalletID: f.walletID,
			RoundID: "round-1", GameID: "game-1", Kind: domain.KindBet, Money: money,
		},
		IdempotencyKey: "provider-a:bet-1",
	}
	ctx := context.Background()
	if _, err := f.svc.SubmitFromMessage(ctx, "c", "msg-1", "h1", in); err != nil {
		t.Fatal(err)
	}
	res, err := f.svc.SubmitFromMessage(ctx, "c", "msg-1", "h1", in)
	if err != nil || !res.Replay || f.balance(t) != 97500 {
		t.Fatalf("redelivery: replay=%v err=%v balance=%d", res.Replay, err, f.balance(t))
	}
	if _, err := f.svc.SubmitFromMessage(ctx, "c", "msg-1", "other", in); !errors.Is(err, app.ErrPoisonMessage) {
		t.Fatalf("poison: %v", err)
	}
	http, err := f.svc.Submit(ctx, in)
	if err != nil || !http.Replay || f.balance(t) != 97500 {
		t.Fatalf("http after sqs: replay=%v err=%v", http.Replay, err)
	}
}

func TestPlayerAndCurrencyMismatchAreRejected(t *testing.T) {
	f := newFixture(t, 100000)
	usd, _ := domain.NewMoney(100, "USD")
	res, err := f.svc.Submit(context.Background(), app.SubmitInput{
		Operation: domain.ExternalOperation{
			ProviderID: "provider-a", ExternalTransactionID: "bet-usd", PlayerID: f.playerID, WalletID: f.walletID,
			RoundID: "r", GameID: "g", Kind: domain.KindBet, Money: usd,
		},
		IdempotencyKey: "k-usd",
	})
	if err != nil || res.Transaction.FailureCode() != domain.CodeCurrencyMismatch {
		t.Fatalf("currency: %v %v", res.Transaction, err)
	}
	f.playerID = domain.NewID()
	res = f.submit(t, domain.KindBet, "bet-other", 100, "")
	if res.Transaction.FailureCode() != domain.CodePlayerMismatch {
		t.Fatalf("player: %s", res.Transaction.FailureCode())
	}
}

func TestReconcileDetectsDivergence(t *testing.T) {
	f := newFixture(t, 100000)
	f.submit(t, domain.KindBet, "bet-1", 2500, "")
	report, err := f.svc.Reconcile(context.Background(), f.walletID)
	if err != nil || !report.Consistent || report.Entries != 2 || report.Stored.Minor() != 97500 {
		t.Fatalf("report: %+v %v", report, err)
	}
	f.store.wallets[f.walletID], _ = domain.RehydrateWallet(f.walletID, f.playerID, mustMoney(97400), 3, f.store.now, f.store.now)
	report, _ = f.svc.Reconcile(context.Background(), f.walletID)
	if report.Consistent || report.Difference.Minor() != -100 {
		t.Fatalf("divergence: %+v", report)
	}
}

func mustMoney(minor int64) domain.Money {
	m, _ := domain.NewMoney(minor, "BRL")
	return m
}

func TestReversalAcrossWalletsIsRejected(t *testing.T) {
	f := newFixture(t, 100000)
	f.submit(t, domain.KindBet, "bet-1", 2500, "")

	otherPlayer := domain.NewID()
	other, err := f.svc.OpenWallet(context.Background(), otherPlayer, mustMoney(100000), "corr")
	if err != nil {
		t.Fatal(err)
	}
	res, err := f.svc.Submit(context.Background(), app.SubmitInput{
		Operation: domain.ExternalOperation{
			ProviderID: "provider-a", ExternalTransactionID: "refund-x", PlayerID: otherPlayer, WalletID: other.ID(),
			RoundID: "round-1", GameID: "game-1", Kind: domain.KindRefund, Money: mustMoney(2500),
			ReferenceExternalTransactionID: "bet-1",
		},
		IdempotencyKey: "provider-a:refund-x",
	})
	if err != nil || res.Transaction.FailureCode() != domain.CodeReferenceMismatch {
		t.Fatalf("cross-wallet refund: %v %v", res.Transaction.FailureCode(), err)
	}
	if f.balance(t) != 97500 {
		t.Fatalf("original wallet changed: %d", f.balance(t))
	}
}

type failingLedger struct {
	app.LedgerRepository
	err error
}

func (f failingLedger) Append(context.Context, domain.LedgerEntry) error { return f.err }

func TestPermanentInvariantFailureMarksPendingTransactionFailed(t *testing.T) {
	f := newFixture(t, 100000)
	early := f.submit(t, domain.KindRefund, "refund-1", 2500, "bet-1")
	f.submit(t, domain.KindBet, "bet-1", 2500, "")

	f.svc.Ledger = failingLedger{LedgerRepository: ledgerRepo{f.store}, err: domain.ErrInvalidMoney}
	f.store.now = f.store.now.Add(time.Minute)
	if _, err := f.svc.ResolveNextPending(context.Background()); err != nil {
		t.Fatalf("resolver should absorb a permanent failure: %v", err)
	}
	got, _ := f.store.GetByID(context.Background(), early.Transaction.ID())
	if got.Status() != domain.StatusFailed || got.FailureCode() != domain.CodeInternalFailure {
		t.Fatalf("expected FAILED/INTERNAL_FAILURE, got %s/%s", got.Status(), got.FailureCode())
	}
	if progressed, _ := f.svc.ResolveNextPending(context.Background()); progressed {
		t.Fatal("a FAILED transaction must not be claimed again")
	}
}

func TestBalanceOverflowIsRejectedNotRetried(t *testing.T) {
	f := newFixture(t, math.MaxInt64-100)
	res := f.submit(t, domain.KindWin, "win-1", 500, "")
	if res.Transaction.Status() != domain.StatusRejected || res.Transaction.FailureCode() != domain.CodeBalanceLimitExceeded {
		t.Fatalf("overflow: %s/%s", res.Transaction.Status(), res.Transaction.FailureCode())
	}
}
