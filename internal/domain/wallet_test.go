package domain_test

import (
	"errors"
	"testing"
	"time"

	"github.com/patrickzets/Testejungle/internal/domain"
)

func brl(minor int64) domain.Money {
	m, err := domain.NewMoney(minor, "BRL")
	if err != nil {
		panic(err)
	}
	return m
}

var now = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func openWallet(t *testing.T, minor int64) *domain.Wallet {
	t.Helper()
	w, _, err := domain.OpenWallet(domain.NewID(), domain.NewID(), brl(minor), domain.NewID(), now)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestOpenWallet(t *testing.T) {
	w, entry, err := domain.OpenWallet(domain.NewID(), domain.NewID(), brl(100000), domain.NewID(), now)
	if err != nil {
		t.Fatal(err)
	}
	if w.Version() != 1 || w.Balance().Minor() != 100000 {
		t.Fatalf("wallet: v=%d balance=%v", w.Version(), w.Balance())
	}
	if entry == nil || entry.Direction() != domain.Credit || entry.BalanceBefore().Minor() != 0 || entry.BalanceAfter().Minor() != 100000 {
		t.Fatalf("opening entry: %+v", entry)
	}

	zero, none, err := domain.OpenWallet(domain.NewID(), domain.NewID(), brl(0), "", now)
	if err != nil || none != nil || zero.Version() != 1 {
		t.Fatalf("zero opening: %v %v %v", zero, none, err)
	}

	if _, _, err := domain.OpenWallet(domain.NewID(), domain.NewID(), brl(-1), domain.NewID(), now); !errors.Is(err, domain.ErrInvalidInput) {
		t.Errorf("negative opening: %v", err)
	}
}

func TestWalletDebitCredit(t *testing.T) {
	w := openWallet(t, 10000)
	entry, err := w.Debit(domain.NewID(), brl(8000), now)
	if err != nil {
		t.Fatal(err)
	}
	if w.Balance().Minor() != 2000 || w.Version() != 2 || entry.BalanceBefore().Minor() != 10000 || entry.BalanceAfter().Minor() != 2000 {
		t.Fatalf("after debit: balance=%v version=%d entry=%+v", w.Balance(), w.Version(), entry)
	}
	if _, err := w.Credit(domain.NewID(), brl(500), now); err != nil || w.Balance().Minor() != 2500 || w.Version() != 3 {
		t.Fatalf("credit: %v %v", w.Balance(), err)
	}
}

func TestWalletRejectsInsufficientFundsWithoutMutation(t *testing.T) {
	w := openWallet(t, 10000)
	if _, err := w.Debit(domain.NewID(), brl(10001), now); !errors.Is(err, domain.ErrInsufficientFunds) {
		t.Fatalf("expected insufficient funds, got %v", err)
	}
	if w.Balance().Minor() != 10000 || w.Version() != 1 {
		t.Fatalf("wallet mutated: %v v%d", w.Balance(), w.Version())
	}
	if _, err := w.Debit(domain.NewID(), brl(10000), now); err != nil || w.Balance().Minor() != 0 {
		t.Fatalf("exact debit: %v %v", w.Balance(), err)
	}
}

func TestWalletRejectsInvalidMovements(t *testing.T) {
	w := openWallet(t, 10000)
	usd, _ := domain.NewMoney(100, "USD")
	if _, err := w.Credit(domain.NewID(), usd, now); !errors.Is(err, domain.ErrCurrencyMismatch) {
		t.Errorf("currency: %v", err)
	}
	if _, err := w.Credit(domain.NewID(), brl(0), now); !errors.Is(err, domain.ErrInvalidInput) {
		t.Errorf("zero movement: %v", err)
	}
	if w.Version() != 1 {
		t.Errorf("version changed on rejected movements: %d", w.Version())
	}
}

func TestRehydrateDoesNotApplyMovements(t *testing.T) {
	w, err := domain.RehydrateWallet(domain.NewID(), domain.NewID(), brl(500), 7, now, now)
	if err != nil || w.Version() != 7 || w.LoadedVersion() != 7 || w.Balance().Minor() != 500 {
		t.Fatalf("rehydrate: %v %v", w, err)
	}
	if _, err := domain.RehydrateWallet(domain.NewID(), domain.NewID(), brl(-1), 1, now, now); err == nil {
		t.Error("negative persisted balance must be rejected")
	}
}

func TestLedgerEntryInvariant(t *testing.T) {
	wid, tid := domain.NewID(), domain.NewID()
	e, err := domain.NewLedgerEntry(wid, tid, domain.Debit, brl(300), brl(1000), now)
	if err != nil || e.BalanceAfter().Minor() != 700 {
		t.Fatalf("debit entry: %+v %v", e, err)
	}
	if _, err := domain.NewLedgerEntry(wid, tid, domain.Debit, brl(1001), brl(1000), now); !errors.Is(err, domain.ErrInsufficientFunds) {
		t.Errorf("overdraft entry: %v", err)
	}
	if _, err := domain.NewLedgerEntry(wid, tid, "SIDEWAYS", brl(1), brl(1), now); err == nil {
		t.Error("invalid direction accepted")
	}
}
