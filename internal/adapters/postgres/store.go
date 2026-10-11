package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/patrickkaua/wagering/internal/app"
)

// Store aggregates database repositories and the transaction runner.
type Store struct {
	pool         *pgxpool.Pool
	tx           *TxRunner
	wallets      *WalletRepository
	transactions *TransactionRepository
	ledger       *LedgerRepository
	inbox        *InboxRepository
	outbox       *OutboxRepository
}

// NewStore initializes all PostgreSQL repositories with pool and observer wiring.
func NewStore(pool *pgxpool.Pool, obs app.Observer) *Store {
	return &Store{
		pool:         pool,
		tx:           NewTxRunner(pool, WithObserver(obs)),
		wallets:      NewWalletRepository(pool),
		transactions: NewTransactionRepository(pool),
		ledger:       NewLedgerRepository(pool),
		inbox:        NewInboxRepository(pool),
		outbox:       NewOutboxRepository(pool),
	}
}

func (s *Store) Tx() app.TxRunner {
	return s.tx
}

func (s *Store) Wallets() app.WalletRepository {
	return s.wallets
}

func (s *Store) Transactions() app.TransactionRepository {
	return s.transactions
}

func (s *Store) Ledger() app.LedgerRepository {
	return s.ledger
}

func (s *Store) Inbox() app.InboxRepository {
	return s.inbox
}

func (s *Store) Outbox() app.OutboxRepository {
	return s.outbox
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}
