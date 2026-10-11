package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/patrickzets/Testejungle/internal/app"
	"github.com/patrickzets/Testejungle/internal/domain"
)

type WalletRepository struct {
	pool *pgxpool.Pool
}

func NewWalletRepository(pool *pgxpool.Pool) *WalletRepository {
	return &WalletRepository{pool: pool}
}

func (r *WalletRepository) Insert(ctx context.Context, w *domain.Wallet) error {
	db := GetDBTX(ctx, r.pool)
	const query = `
		INSERT INTO wallets (id, player_id, currency, balance_minor, version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	_, err := db.Exec(ctx, query,
		w.ID().String(),
		w.PlayerID().String(),
		string(w.Currency()),
		w.Balance().Minor(),
		w.Version(),
		w.CreatedAt(),
		w.UpdatedAt(),
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return app.ErrWalletExists
		}
		return fmt.Errorf("insert wallet: %w", err)
	}
	return nil
}

func (r *WalletRepository) Get(ctx context.Context, id domain.ID) (*domain.Wallet, error) {
	db := GetDBTX(ctx, r.pool)
	const query = `
		SELECT id, player_id, currency, balance_minor, version, created_at, updated_at
		FROM wallets
		WHERE id = $1
	`
	return r.scanWallet(db.QueryRow(ctx, query, id.String()))
}

func (r *WalletRepository) GetForUpdate(ctx context.Context, id domain.ID) (*domain.Wallet, error) {
	db := GetDBTX(ctx, r.pool)
	const query = `
		SELECT id, player_id, currency, balance_minor, version, created_at, updated_at
		FROM wallets
		WHERE id = $1
		FOR UPDATE
	`
	return r.scanWallet(db.QueryRow(ctx, query, id.String()))
}

func (r *WalletRepository) Update(ctx context.Context, w *domain.Wallet) error {
	db := GetDBTX(ctx, r.pool)
	const query = `
		UPDATE wallets
		SET balance_minor = $1, version = $2, updated_at = $3
		WHERE id = $4 AND version = $5
	`
	tag, err := db.Exec(ctx, query,
		w.Balance().Minor(),
		w.Version(),
		w.UpdatedAt(),
		w.ID().String(),
		w.LoadedVersion(),
	)
	if err != nil {
		return fmt.Errorf("update wallet: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return app.ErrVersionConflict
	}
	return nil
}

func (r *WalletRepository) scanWallet(row pgx.Row) (*domain.Wallet, error) {
	var (
		idStr        string
		playerIDStr  string
		currencyStr  string
		balanceMinor int64
		version      int64
		createdAt    time.Time
		updatedAt    time.Time
	)
	err := row.Scan(&idStr, &playerIDStr, &currencyStr, &balanceMinor, &version, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, app.ErrNotFound
		}
		return nil, fmt.Errorf("scan wallet: %w", err)
	}

	money, err := domain.NewMoney(balanceMinor, domain.Currency(currencyStr))
	if err != nil {
		return nil, fmt.Errorf("rehydrate wallet money: %w", err)
	}

	wallet, err := domain.RehydrateWallet(domain.ID(idStr), domain.ID(playerIDStr), money, version, createdAt, updatedAt)
	if err != nil {
		return nil, fmt.Errorf("rehydrate wallet: %w", err)
	}
	return wallet, nil
}
