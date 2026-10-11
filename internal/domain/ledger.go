package domain

import "time"

type Direction string

const (
	Debit  Direction = "DEBIT"
	Credit Direction = "CREDIT"
)

func (d Direction) Valid() bool { return d == Debit || d == Credit }

func (d Direction) Opposite() Direction {
	if d == Debit {
		return Credit
	}
	return Debit
}

// LedgerEntry is an immutable record of one balance movement.
type LedgerEntry struct {
	id            ID
	walletID      ID
	transactionID ID
	direction     Direction
	money         Money
	balanceBefore Money
	balanceAfter  Money
	createdAt     time.Time
}

func NewLedgerEntry(walletID, transactionID ID, direction Direction, money, before Money, now time.Time) (LedgerEntry, error) {
	if walletID.IsZero() || transactionID.IsZero() {
		return LedgerEntry{}, invalid("ledger", "wallet and transaction ids are required")
	}
	if !direction.Valid() {
		return LedgerEntry{}, invalid("direction", "must be DEBIT or CREDIT")
	}
	if !money.IsPositive() {
		return LedgerEntry{}, invalid("money", "ledger amount must be positive")
	}
	var after Money
	var err error
	if direction == Credit {
		after, err = before.Add(money)
	} else {
		after, err = before.Sub(money)
	}
	if err != nil {
		return LedgerEntry{}, err
	}
	if after.IsNegative() {
		return LedgerEntry{}, ErrInsufficientFunds
	}
	return LedgerEntry{
		id: NewID(), walletID: walletID, transactionID: transactionID, direction: direction,
		money: money, balanceBefore: before, balanceAfter: after, createdAt: now.UTC(),
	}, nil
}

// RehydrateLedgerEntry restores a persisted entry without applying anything.
func RehydrateLedgerEntry(id, walletID, transactionID ID, direction Direction, money, before, after Money, createdAt time.Time) LedgerEntry {
	return LedgerEntry{
		id: id, walletID: walletID, transactionID: transactionID, direction: direction,
		money: money, balanceBefore: before, balanceAfter: after, createdAt: createdAt,
	}
}

func (e LedgerEntry) ID() ID               { return e.id }
func (e LedgerEntry) WalletID() ID         { return e.walletID }
func (e LedgerEntry) TransactionID() ID    { return e.transactionID }
func (e LedgerEntry) Direction() Direction { return e.direction }
func (e LedgerEntry) Money() Money         { return e.money }
func (e LedgerEntry) BalanceBefore() Money { return e.balanceBefore }
func (e LedgerEntry) BalanceAfter() Money  { return e.balanceAfter }
func (e LedgerEntry) CreatedAt() time.Time { return e.createdAt }
