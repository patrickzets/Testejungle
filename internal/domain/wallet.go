package domain

import "time"

// Wallet is the financial aggregate root. Balance changes only through Credit and Debit,
// each of which returns the ledger entry that must be persisted with the new balance.
type Wallet struct {
	id            ID
	playerID      ID
	balance       Money
	version       int64
	loadedVersion int64
	createdAt     time.Time
	updatedAt     time.Time
}

// OpenWallet creates a wallet at version 1. A positive initial balance yields the OPENING ledger entry.
func OpenWallet(id, playerID ID, initial Money, openingTxID ID, now time.Time) (*Wallet, *LedgerEntry, error) {
	if id.IsZero() || playerID.IsZero() {
		return nil, nil, invalid("wallet", "id and player id are required")
	}
	if !initial.IsValid() {
		return nil, nil, ErrInvalidMoney
	}
	if initial.IsNegative() {
		return nil, nil, invalid("initialBalance", "must not be negative")
	}
	now = now.UTC()
	w := &Wallet{id: id, playerID: playerID, balance: initial, version: 1, createdAt: now, updatedAt: now}
	if initial.IsZero() {
		return w, nil, nil
	}
	if openingTxID.IsZero() {
		return nil, nil, invalid("transaction", "opening transaction id is required")
	}
	zero, _ := Zero(initial.Currency())
	entry, err := NewLedgerEntry(id, openingTxID, Credit, initial, zero, now)
	if err != nil {
		return nil, nil, err
	}
	return w, &entry, nil
}

// RehydrateWallet restores a persisted wallet without applying any movement.
func RehydrateWallet(id, playerID ID, balance Money, version int64, createdAt, updatedAt time.Time) (*Wallet, error) {
	if id.IsZero() || playerID.IsZero() || !balance.IsValid() || balance.IsNegative() || version < 1 {
		return nil, invalid("wallet", "persisted state is inconsistent")
	}
	return &Wallet{
		id: id, playerID: playerID, balance: balance, version: version, loadedVersion: version,
		createdAt: createdAt, updatedAt: updatedAt,
	}, nil
}

func (w *Wallet) ID() ID               { return w.id }
func (w *Wallet) PlayerID() ID         { return w.playerID }
func (w *Wallet) Balance() Money       { return w.balance }
func (w *Wallet) Currency() Currency   { return w.balance.Currency() }
func (w *Wallet) Version() int64       { return w.version }
func (w *Wallet) LoadedVersion() int64 { return w.loadedVersion }
func (w *Wallet) CreatedAt() time.Time { return w.createdAt }
func (w *Wallet) UpdatedAt() time.Time { return w.updatedAt }

func (w *Wallet) Credit(transactionID ID, amount Money, now time.Time) (LedgerEntry, error) {
	return w.move(Credit, transactionID, amount, now)
}

func (w *Wallet) Debit(transactionID ID, amount Money, now time.Time) (LedgerEntry, error) {
	return w.move(Debit, transactionID, amount, now)
}

func (w *Wallet) Apply(direction Direction, transactionID ID, amount Money, now time.Time) (LedgerEntry, error) {
	return w.move(direction, transactionID, amount, now)
}

func (w *Wallet) move(direction Direction, transactionID ID, amount Money, now time.Time) (LedgerEntry, error) {
	if !amount.IsPositive() {
		return LedgerEntry{}, invalid("money", "movement must be positive")
	}
	if amount.Currency() != w.balance.Currency() {
		return LedgerEntry{}, ErrCurrencyMismatch
	}
	entry, err := NewLedgerEntry(w.id, transactionID, direction, amount, w.balance, now)
	if err != nil {
		return LedgerEntry{}, err
	}
	w.balance = entry.BalanceAfter()
	w.version++
	w.updatedAt = now.UTC()
	return entry, nil
}
