package domain_test

import (
	"encoding/json"
	"errors"
	"math"
	"testing"

	"github.com/patrickkaua/wagering/internal/domain"
)

func TestParseMoney(t *testing.T) {
	valid := map[string]int64{"0.00": 0, "0.01": 1, "25.00": 2500, "1000.50": 100050, "92233720368547758.07": math.MaxInt64}
	for in, want := range valid {
		m, err := domain.ParseMoney(in, "BRL")
		if err != nil {
			t.Fatalf("%q: unexpected error %v", in, err)
		}
		if m.Minor() != want || m.Amount() != in {
			t.Errorf("%q: got minor=%d amount=%s", in, m.Minor(), m.Amount())
		}
	}
	invalid := []string{"", "1", "1.0", "1.000", ".50", "-1.00", "+1.00", "1e3", "1.00e1", "NaN", "Infinity", "01.00", " 1.00", "1,00", "1.0a"}
	for _, in := range invalid {
		if _, err := domain.ParseMoney(in, "BRL"); !errors.Is(err, domain.ErrInvalidInput) {
			t.Errorf("%q: expected ErrInvalidInput, got %v", in, err)
		}
	}
	if _, err := domain.ParseMoney("92233720368547758.08", "BRL"); !errors.Is(err, domain.ErrMoneyOverflow) {
		t.Errorf("expected overflow, got %v", err)
	}
	if _, err := domain.ParseMoney("1.00", "XXX"); !errors.Is(err, domain.ErrInvalidInput) {
		t.Errorf("expected unsupported currency, got %v", err)
	}
}

func TestMoneyArithmetic(t *testing.T) {
	a, _ := domain.NewMoney(1000, "BRL")
	b, _ := domain.NewMoney(250, "BRL")
	sum, err := a.Add(b)
	if err != nil || sum.Minor() != 1250 {
		t.Fatalf("add: %v %v", sum, err)
	}
	diff, _ := b.Sub(a)
	if diff.Minor() != -750 || diff.Amount() != "-7.50" {
		t.Fatalf("sub: %v", diff)
	}
	neg, _ := a.Neg()
	if neg.Minor() != -1000 {
		t.Fatalf("neg: %v", neg)
	}
	if c, _ := a.Cmp(b); c != 1 {
		t.Fatalf("cmp: %d", c)
	}
}

func TestMoneyOverflow(t *testing.T) {
	max, _ := domain.NewMoney(math.MaxInt64, "BRL")
	one, _ := domain.NewMoney(1, "BRL")
	if _, err := max.Add(one); !errors.Is(err, domain.ErrMoneyOverflow) {
		t.Errorf("add overflow: %v", err)
	}
	min, _ := domain.NewMoney(math.MinInt64, "BRL")
	if _, err := min.Sub(one); !errors.Is(err, domain.ErrMoneyOverflow) {
		t.Errorf("sub overflow: %v", err)
	}
	if _, err := min.Neg(); !errors.Is(err, domain.ErrMoneyOverflow) {
		t.Errorf("neg overflow: %v", err)
	}
	if got := min.Amount(); got != "-92233720368547758.08" {
		t.Errorf("min amount: %s", got)
	}
}

func TestMoneyCurrencyMismatchAndZeroValue(t *testing.T) {
	brl, _ := domain.NewMoney(100, "BRL")
	usd, _ := domain.NewMoney(100, "USD")
	if _, err := brl.Add(usd); !errors.Is(err, domain.ErrCurrencyMismatch) {
		t.Errorf("add: %v", err)
	}
	if _, err := brl.Cmp(usd); !errors.Is(err, domain.ErrCurrencyMismatch) {
		t.Errorf("cmp: %v", err)
	}
	var zero domain.Money
	if _, err := zero.Add(brl); !errors.Is(err, domain.ErrInvalidMoney) {
		t.Errorf("uninitialized: %v", err)
	}
	if _, err := json.Marshal(zero); err == nil {
		t.Error("uninitialized money must not serialize")
	}
}

func TestMoneyJSON(t *testing.T) {
	m, _ := domain.NewMoney(2500, "BRL")
	b, err := json.Marshal(m)
	if err != nil || string(b) != `{"amount":"25.00","currency":"BRL"}` {
		t.Fatalf("marshal: %s %v", b, err)
	}
	var back domain.Money
	if err := json.Unmarshal(b, &back); err != nil || !back.Equal(m) {
		t.Fatalf("roundtrip: %v %v", back, err)
	}
	for _, bad := range []string{`{"amount":25.00,"currency":"BRL"}`, `{"amount":"-1.00","currency":"BRL"}`, `{"amount":"1e2","currency":"BRL"}`, `"25.00"`} {
		var m domain.Money
		if err := json.Unmarshal([]byte(bad), &m); err == nil {
			t.Errorf("%s: expected error", bad)
		}
	}
}
