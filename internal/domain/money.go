package domain

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
)

type Currency string

var supportedCurrencies = map[Currency]struct{}{
	"BRL": {}, "USD": {}, "EUR": {}, "GBP": {},
}

func NewCurrency(code string) (Currency, error) {
	c := Currency(code)
	if _, ok := supportedCurrencies[c]; !ok {
		return "", invalid("currency", "unsupported ISO 4217 code %q", code)
	}
	return c, nil
}

// Money is an immutable amount in minor units (scale 2) with a currency.
// The int64 range bounds the value to +/- 92,233,720,368,547,758.07.
type Money struct {
	minor    int64
	currency Currency
}

func NewMoney(minor int64, currency Currency) (Money, error) {
	if _, err := NewCurrency(string(currency)); err != nil {
		return Money{}, err
	}
	return Money{minor: minor, currency: currency}, nil
}

func Zero(currency Currency) (Money, error) { return NewMoney(0, currency) }

// ParseMoney accepts only canonical non-negative decimals with exactly two fraction digits.
func ParseMoney(amount, currency string) (Money, error) {
	cur, err := NewCurrency(currency)
	if err != nil {
		return Money{}, err
	}
	whole, frac, ok := strings.Cut(amount, ".")
	if !ok || len(frac) != 2 || whole == "" {
		return Money{}, invalid("amount", "must be a decimal with exactly two fraction digits")
	}
	if len(whole) > 1 && whole[0] == '0' {
		return Money{}, invalid("amount", "leading zeros are not allowed")
	}
	if !allDigits(whole) || !allDigits(frac) {
		return Money{}, invalid("amount", "must contain digits only")
	}
	minor, err := strconv.ParseInt(whole+frac, 10, 64)
	if err != nil {
		return Money{}, ErrMoneyOverflow
	}
	return Money{minor: minor, currency: cur}, nil
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func (m Money) Minor() int64       { return m.minor }
func (m Money) Currency() Currency { return m.currency }
func (m Money) IsValid() bool      { return m.currency != "" }
func (m Money) IsZero() bool       { return m.minor == 0 }
func (m Money) IsPositive() bool   { return m.minor > 0 }
func (m Money) IsNegative() bool   { return m.minor < 0 }

func (m Money) Amount() string {
	n := m.minor
	sign := ""
	var u uint64
	if n < 0 {
		sign = "-"
		u = uint64(-(n + 1)) + 1
	} else {
		u = uint64(n)
	}
	s := strconv.FormatUint(u, 10)
	if len(s) < 3 {
		s = strings.Repeat("0", 3-len(s)) + s
	}
	return sign + s[:len(s)-2] + "." + s[len(s)-2:]
}

func (m Money) String() string { return m.Amount() + " " + string(m.currency) }

func (m Money) compatible(o Money) error {
	if !m.IsValid() || !o.IsValid() {
		return ErrInvalidMoney
	}
	if m.currency != o.currency {
		return ErrCurrencyMismatch
	}
	return nil
}

func (m Money) Add(o Money) (Money, error) {
	if err := m.compatible(o); err != nil {
		return Money{}, err
	}
	if (o.minor > 0 && m.minor > math.MaxInt64-o.minor) || (o.minor < 0 && m.minor < math.MinInt64-o.minor) {
		return Money{}, ErrMoneyOverflow
	}
	return Money{minor: m.minor + o.minor, currency: m.currency}, nil
}

func (m Money) Sub(o Money) (Money, error) {
	n, err := o.Neg()
	if err != nil {
		return Money{}, err
	}
	return m.Add(n)
}

func (m Money) Neg() (Money, error) {
	if !m.IsValid() {
		return Money{}, ErrInvalidMoney
	}
	if m.minor == math.MinInt64 {
		return Money{}, ErrMoneyOverflow
	}
	return Money{minor: -m.minor, currency: m.currency}, nil
}

// Cmp returns -1, 0 or 1.
func (m Money) Cmp(o Money) (int, error) {
	if err := m.compatible(o); err != nil {
		return 0, err
	}
	switch {
	case m.minor < o.minor:
		return -1, nil
	case m.minor > o.minor:
		return 1, nil
	}
	return 0, nil
}

func (m Money) Equal(o Money) bool { return m == o }

type moneyJSON struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func (m Money) MarshalJSON() ([]byte, error) {
	if !m.IsValid() {
		return nil, ErrInvalidMoney
	}
	return json.Marshal(moneyJSON{Amount: m.Amount(), Currency: string(m.currency)})
}

// UnmarshalJSON enforces the external contract: non-negative, canonical, string-typed amount.
func (m *Money) UnmarshalJSON(data []byte) error {
	var raw moneyJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return invalid("money", "must be {\"amount\":\"0.00\",\"currency\":\"BRL\"}")
	}
	parsed, err := ParseMoney(raw.Amount, raw.Currency)
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}
