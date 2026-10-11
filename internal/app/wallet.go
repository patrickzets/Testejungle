package app

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"

	"github.com/patrickkaua/wagering/internal/domain"
)

const (
	DefaultLedgerLimit = 50
	MaxLedgerLimit     = 200
)

// OpenWallet creates the wallet and, for a positive balance, the OPENING transaction, its ledger entry
// and the outbox events in a single commit.
func (s *Service) OpenWallet(ctx context.Context, playerID domain.ID, initial domain.Money, correlationID string) (*domain.Wallet, error) {
	var wallet *domain.Wallet
	err := s.Tx.WithinTx(ctx, func(ctx context.Context) error {
		now := s.Clock.Now()
		openingID := domain.NewID()
		w, entry, err := domain.OpenWallet(domain.NewID(), playerID, initial, openingID, now)
		if err != nil {
			return err
		}
		if err := s.Wallets.Insert(ctx, w); err != nil {
			return err
		}
		if entry != nil {
			opening, err := domain.NewOpeningTransaction(openingID, w.ID(), playerID, initial, now)
			if err != nil {
				return err
			}
			if err := s.Transactions.InsertInternal(ctx, opening); err != nil {
				return err
			}
			if err := s.Ledger.Append(ctx, *entry); err != nil {
				return err
			}
			processed, err := domain.NewWagerTransactionProcessedEvent(opening, correlationID, now)
			if err != nil {
				return err
			}
			changed, err := domain.NewWalletBalanceChangedEvent(*entry, w.Version(), correlationID, now)
			if err != nil {
				return err
			}
			if err := s.Outbox.Add(ctx, processed, changed); err != nil {
				return err
			}
		}
		wallet = w
		return nil
	})
	if err != nil {
		return nil, err
	}
	return wallet, nil
}

func (s *Service) GetWallet(ctx context.Context, id domain.ID) (*domain.Wallet, error) {
	return s.Wallets.Get(ctx, id)
}

func (s *Service) ListLedger(ctx context.Context, walletID domain.ID, cursor string, limit int) (LedgerPage, error) {
	if limit <= 0 {
		limit = DefaultLedgerLimit
	}
	if limit > MaxLedgerLimit {
		limit = MaxLedgerLimit
	}
	if cursor != "" {
		if _, err := DecodeCursor(cursor); err != nil {
			return LedgerPage{}, err
		}
	}
	if _, err := s.Wallets.Get(ctx, walletID); err != nil {
		return LedgerPage{}, err
	}
	return s.Ledger.List(ctx, walletID, cursor, limit)
}

func EncodeCursor(seq int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(seq, 10)))
}

func DecodeCursor(cursor string) (int64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, fmt.Errorf("%w: cursor", domain.ErrInvalidInput)
	}
	seq, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || seq < 0 {
		return 0, fmt.Errorf("%w: cursor", domain.ErrInvalidInput)
	}
	return seq, nil
}

type ReconciliationReport struct {
	WalletID   domain.ID
	Stored     domain.Money
	Calculated domain.Money
	Difference domain.Money
	Consistent bool
	Entries    int
}

// Reconcile rebuilds the balance from the ledger and compares it with the stored one. It never writes.
func (s *Service) Reconcile(ctx context.Context, walletID domain.ID) (ReconciliationReport, error) {
	r, err := s.Ledger.Reconcile(ctx, walletID)
	if err != nil {
		return ReconciliationReport{}, err
	}
	diff, err := r.Stored.Sub(r.Calculated)
	if err != nil {
		return ReconciliationReport{}, err
	}
	report := ReconciliationReport{
		WalletID: walletID, Stored: r.Stored, Calculated: r.Calculated, Difference: diff,
		Consistent: diff.IsZero(), Entries: r.Entries,
	}
	if !report.Consistent {
		s.Observer.ReconciliationDivergence()
	}
	return report, nil
}

func (s *Service) GetTransaction(ctx context.Context, id domain.ID) (*domain.WagerTransaction, error) {
	return s.Transactions.GetByID(ctx, id)
}

func (s *Service) GetProviderTransaction(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error) {
	return s.Transactions.GetByExternalID(ctx, providerID, externalID)
}
