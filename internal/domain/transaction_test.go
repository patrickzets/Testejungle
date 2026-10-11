package domain_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/patrickkaua/wagering/internal/domain"
)

func operation(kind domain.Kind, minor int64) domain.ExternalOperation {
	return domain.ExternalOperation{
		ProviderID: "provider-a", ExternalTransactionID: "tx-1",
		PlayerID: domain.NewID(), WalletID: domain.NewID(),
		RoundID: "round-1", GameID: "game-1", Kind: kind, Money: brl(minor),
	}
}

func TestOperationValidation(t *testing.T) {
	cases := []struct {
		name string
		op   func() domain.ExternalOperation
		want error
	}{
		{"valid bet", func() domain.ExternalOperation { return operation(domain.KindBet, 100) }, nil},
		{"valid loss", func() domain.ExternalOperation { return operation(domain.KindLoss, 0) }, nil},
		{"loss with amount", func() domain.ExternalOperation { return operation(domain.KindLoss, 1) }, domain.ErrInvalidInput},
		{"bet zero", func() domain.ExternalOperation { return operation(domain.KindBet, 0) }, domain.ErrInvalidInput},
		{"win zero", func() domain.ExternalOperation { return operation(domain.KindWin, 0) }, domain.ErrInvalidInput},
		{"opening rejected", func() domain.ExternalOperation { return operation(domain.KindOpening, 100) }, domain.ErrOpeningNotAllowed},
		{"unknown kind", func() domain.ExternalOperation { return operation("FOO", 100) }, domain.ErrInvalidInput},
		{"refund without reference", func() domain.ExternalOperation { return operation(domain.KindRefund, 100) }, domain.ErrInvalidInput},
		{"bet with reference", func() domain.ExternalOperation {
			o := operation(domain.KindBet, 100)
			o.ReferenceExternalTransactionID = "x"
			return o
		}, domain.ErrInvalidInput},
		{"missing round", func() domain.ExternalOperation {
			o := operation(domain.KindBet, 100)
			o.RoundID = ""
			return o
		}, domain.ErrInvalidInput},
		{"uninitialized money", func() domain.ExternalOperation {
			o := operation(domain.KindBet, 100)
			o.Money = domain.Money{}
			return o
		}, domain.ErrInvalidInput},
	}
	for _, tc := range cases {
		err := tc.op().Validate()
		if tc.want == nil && err != nil || tc.want != nil && !errors.Is(err, tc.want) {
			t.Errorf("%s: got %v, want %v", tc.name, err, tc.want)
		}
	}
	if _, err := domain.ParseExternalKind("OPENING"); !errors.Is(err, domain.ErrOpeningNotAllowed) {
		t.Errorf("OPENING from transport: %v", err)
	}
}

func TestHashIsDeterministicAndSensitive(t *testing.T) {
	a := operation(domain.KindBet, 2500)
	b := a
	if a.Hash() != b.Hash() {
		t.Fatal("equal operations must hash equally")
	}
	b.Money = brl(2501)
	if a.Hash() == b.Hash() {
		t.Fatal("different amounts must hash differently")
	}
	b = a
	b.GameID = "other"
	if a.Hash() == b.Hash() {
		t.Fatal("different game must hash differently")
	}
	if len(a.Hash()) != 64 {
		t.Fatalf("expected sha-256 hex, got %d chars", len(a.Hash()))
	}
}

func TestTransactionStateMachine(t *testing.T) {
	op := operation(domain.KindRefund, 100)
	op.ReferenceExternalTransactionID = "bet-1"
	tx, err := domain.NewExternalTransaction(domain.NewID(), op, "provider-a:tx-1", now)
	if err != nil || tx.Status() != domain.StatusPending {
		t.Fatalf("new: %v %v", tx, err)
	}
	if err := tx.AwaitReference(now.Add(2), now); err != nil || tx.Status() != domain.StatusPendingReference || tx.Attempts() != 1 {
		t.Fatalf("await: %v status=%s", err, tx.Status())
	}
	if err := tx.AwaitReference(now.Add(4), now); err != nil || tx.Attempts() != 2 {
		t.Fatalf("await again: %v", err)
	}
	if err := tx.MarkProcessed(brl(500), now); err != nil || tx.Status() != domain.StatusProcessed {
		t.Fatalf("processed: %v", err)
	}
	for name, err := range map[string]error{
		"reject":  tx.Reject(domain.CodeAlreadyReversed, now),
		"fail":    tx.Fail(domain.CodeInternalFailure, now),
		"process": tx.MarkProcessed(brl(1), now),
		"await":   tx.AwaitReference(now, now),
	} {
		if !errors.Is(err, domain.ErrTerminalTransaction) {
			t.Errorf("%s after terminal: %v", name, err)
		}
	}
}

func TestBetCannotAwaitReference(t *testing.T) {
	tx, _ := domain.NewExternalTransaction(domain.NewID(), operation(domain.KindBet, 100), "k", now)
	if err := tx.AwaitReference(now, now); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Errorf("bet await: %v", err)
	}
}

func TestOpeningTransaction(t *testing.T) {
	tx, err := domain.NewOpeningTransaction(domain.NewID(), domain.NewID(), domain.NewID(), brl(1000), now)
	if err != nil {
		t.Fatal(err)
	}
	if tx.Origin() != domain.OriginInternal || tx.Status() != domain.StatusProcessed || tx.ProviderID() != "" || tx.IdempotencyKey() != "" {
		t.Fatalf("opening: %+v", tx)
	}
	if _, err := domain.NewOpeningTransaction(domain.NewID(), domain.NewID(), domain.NewID(), brl(0), now); err == nil {
		t.Error("zero opening must be rejected")
	}
}

func referenced(kind domain.Kind, minor int64, ref *domain.WagerTransaction) *domain.WagerTransaction {
	op := ref.Operation()
	op.ExternalTransactionID = "tx-2"
	op.Kind = kind
	op.Money = brl(minor)
	op.ReferenceExternalTransactionID = ref.ExternalID()
	tx, err := domain.NewExternalTransaction(domain.NewID(), op, "provider-a:tx-2", now)
	if err != nil {
		panic(err)
	}
	return tx
}

func TestCheckReference(t *testing.T) {
	bet, _ := domain.NewExternalTransaction(domain.NewID(), operation(domain.KindBet, 2500), "k1", now)

	if dir, rej := referenced(domain.KindRefund, 2500, bet).CheckReference(bet); rej != nil || dir != domain.Credit {
		t.Errorf("refund of bet: %v %v", dir, rej)
	}
	if dir, rej := referenced(domain.KindRollback, 2500, bet).CheckReference(bet); rej != nil || dir != domain.Credit {
		t.Errorf("rollback of bet: %v %v", dir, rej)
	}
	if _, rej := referenced(domain.KindRefund, 2000, bet).CheckReference(bet); rej == nil || rej.Code != domain.CodeReferenceMismatch {
		t.Errorf("partial refund: %v", rej)
	}

	winOp := operation(domain.KindWin, 5000)
	winOp.ExternalTransactionID = "win-1"
	win, _ := domain.NewExternalTransaction(domain.NewID(), winOp, "k2", now)
	if dir, rej := referenced(domain.KindRollback, 5000, win).CheckReference(win); rej != nil || dir != domain.Debit {
		t.Errorf("rollback of win: %v %v", dir, rej)
	}
	if _, rej := referenced(domain.KindRefund, 5000, win).CheckReference(win); rej == nil || rej.Code != domain.CodeInvalidReferenceKind {
		t.Errorf("refund of win: %v", rej)
	}

	other := operation(domain.KindBet, 2500)
	other.ExternalTransactionID = "tx-9"
	otherBet, _ := domain.NewExternalTransaction(domain.NewID(), other, "k3", now)
	mismatch := referenced(domain.KindRefund, 2500, bet)
	if _, rej := mismatch.CheckReference(otherBet); rej == nil || rej.Code != domain.CodeReferenceMismatch {
		t.Errorf("different wallet: %v", rej)
	}
}

func TestEventsAreImmutableSnapshots(t *testing.T) {
	w := openWallet(t, 1000)
	entry, _ := w.Debit(domain.NewID(), brl(250), now)
	ev, err := domain.NewWalletBalanceChangedEvent(entry, w.Version(), "corr-1", now)
	if err != nil {
		t.Fatal(err)
	}
	if ev.EventType != domain.EventWalletBalanceChanged || ev.Version != 1 || ev.CorrelationID != "corr-1" || ev.EventID.IsZero() {
		t.Fatalf("envelope: %+v", ev)
	}
	want := `"balanceBefore":{"amount":"10.00","currency":"BRL"}`
	if !strings.Contains(string(ev.Data), want) || !strings.Contains(string(ev.Data), `"walletVersion":2`) || !strings.Contains(string(ev.Data), `"direction":"DEBIT"`) {
		t.Fatalf("payload: %s", ev.Data)
	}
}
