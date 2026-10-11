package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/patrickzets/Testejungle/internal/app"
	"github.com/patrickzets/Testejungle/internal/domain"
)

type LedgerRepository struct {
	pool *pgxpool.Pool
}

func NewLedgerRepository(pool *pgxpool.Pool) *LedgerRepository {
	return &LedgerRepository{pool: pool}
}

func (r *LedgerRepository) Append(ctx context.Context, e domain.LedgerEntry) error {
	db := GetDBTX(ctx, r.pool)
	const query = `
		INSERT INTO wallet_ledger_entries (
			id, wallet_id, transaction_id, direction, amount_minor,
			balance_before, balance_after, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`
	_, err := db.Exec(ctx, query,
		e.ID().String(),
		e.WalletID().String(),
		e.TransactionID().String(),
		string(e.Direction()),
		e.Money().Minor(),
		e.BalanceBefore().Minor(),
		e.BalanceAfter().Minor(),
		e.CreatedAt(),
	)
	if err != nil {
		return fmt.Errorf("append ledger entry: %w", err)
	}
	return nil
}

func (r *LedgerRepository) List(ctx context.Context, walletID domain.ID, cursor string, limit int) (app.LedgerPage, error) {
	db := GetDBTX(ctx, r.pool)
	var (
		rows pgx.Rows
		err  error
	)

	fetchLimit := limit + 1
	if cursor == "" {
		const query = `
			SELECT l.id, l.seq, l.wallet_id, l.transaction_id, l.direction,
			       l.amount_minor, l.balance_before, l.balance_after, l.created_at, w.currency
			FROM wallet_ledger_entries l
			JOIN wallets w ON w.id = l.wallet_id
			WHERE l.wallet_id = $1
			ORDER BY l.seq ASC
			LIMIT $2
		`
		rows, err = db.Query(ctx, query, walletID.String(), fetchLimit)
	} else {
		startSeq, decodeErr := app.DecodeCursor(cursor)
		if decodeErr != nil {
			return app.LedgerPage{}, decodeErr
		}
		const query = `
			SELECT l.id, l.seq, l.wallet_id, l.transaction_id, l.direction,
			       l.amount_minor, l.balance_before, l.balance_after, l.created_at, w.currency
			FROM wallet_ledger_entries l
			JOIN wallets w ON w.id = l.wallet_id
			WHERE l.wallet_id = $1 AND l.seq > $2
			ORDER BY l.seq ASC
			LIMIT $3
		`
		rows, err = db.Query(ctx, query, walletID.String(), startSeq, fetchLimit)
	}

	if err != nil {
		return app.LedgerPage{}, fmt.Errorf("list ledger: %w", err)
	}
	defer rows.Close()

	var (
		entries []domain.LedgerEntry
		seqs    []int64
	)

	for rows.Next() {
		var (
			idStr        string
			seq          int64
			walletIDStr  string
			txIDStr      string
			directionStr string
			amountMinor  int64
			beforeMinor  int64
			afterMinor   int64
			createdAt    time.Time
			currencyStr  string
		)
		if err := rows.Scan(&idStr, &seq, &walletIDStr, &txIDStr, &directionStr, &amountMinor, &beforeMinor, &afterMinor, &createdAt, &currencyStr); err != nil {
			return app.LedgerPage{}, fmt.Errorf("scan ledger row: %w", err)
		}

		cur := domain.Currency(currencyStr)
		money, err := domain.NewMoney(amountMinor, cur)
		if err != nil {
			return app.LedgerPage{}, err
		}
		before, err := domain.NewMoney(beforeMinor, cur)
		if err != nil {
			return app.LedgerPage{}, err
		}
		after, err := domain.NewMoney(afterMinor, cur)
		if err != nil {
			return app.LedgerPage{}, err
		}

		entry := domain.RehydrateLedgerEntry(
			domain.ID(idStr),
			domain.ID(walletIDStr),
			domain.ID(txIDStr),
			domain.Direction(directionStr),
			money,
			before,
			after,
			createdAt,
		)
		entries = append(entries, entry)
		seqs = append(seqs, seq)
	}

	if err := rows.Err(); err != nil {
		return app.LedgerPage{}, fmt.Errorf("iterate ledger rows: %w", err)
	}

	nextCursor := ""
	if len(entries) > limit {
		nextCursor = app.EncodeCursor(seqs[limit-1])
		entries = entries[:limit]
	}

	return app.LedgerPage{
		Entries:    entries,
		NextCursor: nextCursor,
	}, nil
}

func (r *LedgerRepository) Reconcile(ctx context.Context, walletID domain.ID) (app.Reconciliation, error) {
	db := GetDBTX(ctx, r.pool)
	const query = `
		SELECT w.currency,
		       w.balance_minor,
		       COALESCE(SUM(CASE WHEN l.direction = 'CREDIT' THEN l.amount_minor WHEN l.direction = 'DEBIT' THEN -l.amount_minor ELSE 0 END), 0) AS calculated_minor,
		       COUNT(l.id) AS entries_count
		FROM wallets w
		LEFT JOIN wallet_ledger_entries l ON w.id = l.wallet_id
		WHERE w.id = $1
		GROUP BY w.id, w.currency, w.balance_minor
	`
	var (
		currencyStr     string
		storedMinor     int64
		calculatedMinor int64
		entriesCount    int64
	)

	err := db.QueryRow(ctx, query, walletID.String()).Scan(&currencyStr, &storedMinor, &calculatedMinor, &entriesCount)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return app.Reconciliation{}, app.ErrNotFound
		}
		return app.Reconciliation{}, fmt.Errorf("reconcile ledger: %w", err)
	}

	cur := domain.Currency(currencyStr)
	stored, err := domain.NewMoney(storedMinor, cur)
	if err != nil {
		return app.Reconciliation{}, err
	}
	calculated, err := domain.NewMoney(calculatedMinor, cur)
	if err != nil {
		return app.Reconciliation{}, err
	}

	return app.Reconciliation{
		Stored:     stored,
		Calculated: calculated,
		Entries:    int(entriesCount),
	}, nil
}
