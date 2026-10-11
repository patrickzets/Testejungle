package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/patrickkaua/wagering/internal/app"
	"github.com/patrickkaua/wagering/internal/domain"
)

type TransactionRepository struct {
	pool *pgxpool.Pool
}

func NewTransactionRepository(pool *pgxpool.Pool) *TransactionRepository {
	return &TransactionRepository{pool: pool}
}

const txSelectColumns = `
	id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash,
	wallet_id, player_id, round_id, game_id, kind, amount_minor, currency,
	reference_external_transaction_id, reference_transaction_id, status, failure_code,
	result_balance_minor, attempts, next_attempt_at, first_pending_at, created_at, updated_at
`

func (r *TransactionRepository) InsertExternal(ctx context.Context, t *domain.WagerTransaction) (bool, error) {
	db := GetDBTX(ctx, r.pool)
	const query = `
		INSERT INTO wager_transactions (
			id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash,
			wallet_id, player_id, round_id, game_id, kind, amount_minor, currency,
			reference_external_transaction_id, reference_transaction_id, status, failure_code,
			result_balance_minor, attempts, next_attempt_at, first_pending_at, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23
		)
		ON CONFLICT DO NOTHING
	`
	params := extractTxInsertParams(t)
	tag, err := db.Exec(ctx, query, params...)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			return false, app.ErrWalletNotFound
		}
		return false, fmt.Errorf("insert external tx: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

func (r *TransactionRepository) InsertInternal(ctx context.Context, t *domain.WagerTransaction) error {
	db := GetDBTX(ctx, r.pool)
	const query = `
		INSERT INTO wager_transactions (
			id, origin, provider_id, external_transaction_id, idempotency_key, payload_hash,
			wallet_id, player_id, round_id, game_id, kind, amount_minor, currency,
			reference_external_transaction_id, reference_transaction_id, status, failure_code,
			result_balance_minor, attempts, next_attempt_at, first_pending_at, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23
		)
	`
	params := extractTxInsertParams(t)
	_, err := db.Exec(ctx, query, params...)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			return app.ErrWalletNotFound
		}
		return fmt.Errorf("insert internal tx: %w", err)
	}
	return nil
}

func (r *TransactionRepository) GetByID(ctx context.Context, id domain.ID) (*domain.WagerTransaction, error) {
	db := GetDBTX(ctx, r.pool)
	query := `SELECT ` + txSelectColumns + ` FROM wager_transactions WHERE id = $1`
	return r.scanTx(db.QueryRow(ctx, query, id.String()))
}

func (r *TransactionRepository) GetByIdempotencyKey(ctx context.Context, providerID, key string) (*domain.WagerTransaction, error) {
	db := GetDBTX(ctx, r.pool)
	query := `SELECT ` + txSelectColumns + ` FROM wager_transactions WHERE provider_id = $1 AND idempotency_key = $2`
	return r.scanTx(db.QueryRow(ctx, query, providerID, key))
}

func (r *TransactionRepository) GetByExternalID(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error) {
	db := GetDBTX(ctx, r.pool)
	query := `SELECT ` + txSelectColumns + ` FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2`
	return r.scanTx(db.QueryRow(ctx, query, providerID, externalID))
}

func (r *TransactionRepository) FindProcessedReversal(ctx context.Context, refTxID domain.ID) (*domain.WagerTransaction, error) {
	db := GetDBTX(ctx, r.pool)
	query := `
		SELECT ` + txSelectColumns + `
		FROM wager_transactions
		WHERE reference_transaction_id = $1
		  AND kind IN ('REFUND', 'ROLLBACK')
		  AND status = 'PROCESSED'
		LIMIT 1
	`
	return r.scanTx(db.QueryRow(ctx, query, refTxID.String()))
}

func (r *TransactionRepository) Update(ctx context.Context, t *domain.WagerTransaction) error {
	db := GetDBTX(ctx, r.pool)
	const query = `
		UPDATE wager_transactions
		SET status = $1,
		    failure_code = $2,
		    result_balance_minor = $3,
		    attempts = $4,
		    next_attempt_at = $5,
		    first_pending_at = $6,
		    reference_transaction_id = $7,
		    updated_at = $8
		WHERE id = $9
	`

	var failureCode *string
	if string(t.FailureCode()) != "" {
		s := string(t.FailureCode())
		failureCode = &s
	}

	var resultBalanceMinor *int64
	if t.ResultBalance() != nil {
		m := t.ResultBalance().Minor()
		resultBalanceMinor = &m
	}

	var refTxID *string
	if !t.ReferenceTxID().IsZero() {
		s := t.ReferenceTxID().String()
		refTxID = &s
	}

	tag, err := db.Exec(ctx, query,
		string(t.Status()),
		failureCode,
		resultBalanceMinor,
		t.Attempts(),
		t.NextAttemptAt(),
		t.FirstPendingAt(),
		refTxID,
		t.UpdatedAt(),
		t.ID().String(),
	)
	if err != nil {
		return fmt.Errorf("update tx: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return app.ErrNotFound
	}
	return nil
}

func (r *TransactionRepository) ClaimNextDue(ctx context.Context, now time.Time) (*domain.WagerTransaction, error) {
	db := GetDBTX(ctx, r.pool)
	query := `
		SELECT ` + txSelectColumns + `
		FROM wager_transactions
		WHERE status IN ('PENDING', 'PENDING_REFERENCE')
		  AND next_attempt_at <= $1
		ORDER BY next_attempt_at ASC
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`
	return r.scanTx(db.QueryRow(ctx, query, now))
}

func extractTxInsertParams(t *domain.WagerTransaction) []any {
	var providerID *string
	if t.ProviderID() != "" {
		s := t.ProviderID()
		providerID = &s
	}

	var externalID *string
	if t.ExternalID() != "" {
		s := t.ExternalID()
		externalID = &s
	}

	var idempotencyKey *string
	if t.IdempotencyKey() != "" {
		s := t.IdempotencyKey()
		idempotencyKey = &s
	}

	var payloadHash *string
	if t.PayloadHash() != "" {
		s := t.PayloadHash()
		payloadHash = &s
	}

	var roundID *string
	if t.RoundID() != "" {
		s := t.RoundID()
		roundID = &s
	}

	var gameID *string
	if t.GameID() != "" {
		s := t.GameID()
		gameID = &s
	}

	var refExternalID *string
	if t.ReferenceExternalID() != "" {
		s := t.ReferenceExternalID()
		refExternalID = &s
	}

	var refTxID *string
	if !t.ReferenceTxID().IsZero() {
		s := t.ReferenceTxID().String()
		refTxID = &s
	}

	var failureCode *string
	if string(t.FailureCode()) != "" {
		s := string(t.FailureCode())
		failureCode = &s
	}

	var resultBalanceMinor *int64
	if t.ResultBalance() != nil {
		m := t.ResultBalance().Minor()
		resultBalanceMinor = &m
	}

	return []any{
		t.ID().String(),
		string(t.Origin()),
		providerID,
		externalID,
		idempotencyKey,
		payloadHash,
		t.WalletID().String(),
		t.PlayerID().String(),
		roundID,
		gameID,
		string(t.Kind()),
		t.Money().Minor(),
		string(t.Money().Currency()),
		refExternalID,
		refTxID,
		string(t.Status()),
		failureCode,
		resultBalanceMinor,
		t.Attempts(),
		t.NextAttemptAt(),
		t.FirstPendingAt(),
		t.CreatedAt(),
		t.UpdatedAt(),
	}
}

func (r *TransactionRepository) scanTx(row pgx.Row) (*domain.WagerTransaction, error) {
	var (
		idStr            string
		originStr        string
		providerID       *string
		externalID       *string
		idempotencyKey   *string
		payloadHash      *string
		walletIDStr      string
		playerIDStr      string
		roundID          *string
		gameID           *string
		kindStr          string
		amountMinor      int64
		currencyStr      string
		refExternalID    *string
		refTxID          *string
		statusStr        string
		failureCode      *string
		resultBalanceMin *int64
		attempts         int
		nextAttemptAt    *time.Time
		firstPendingAt   *time.Time
		createdAt        time.Time
		updatedAt        time.Time
	)

	err := row.Scan(
		&idStr,
		&originStr,
		&providerID,
		&externalID,
		&idempotencyKey,
		&payloadHash,
		&walletIDStr,
		&playerIDStr,
		&roundID,
		&gameID,
		&kindStr,
		&amountMinor,
		&currencyStr,
		&refExternalID,
		&refTxID,
		&statusStr,
		&failureCode,
		&resultBalanceMin,
		&attempts,
		&nextAttemptAt,
		&firstPendingAt,
		&createdAt,
		&updatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, app.ErrNotFound
		}
		return nil, fmt.Errorf("scan tx: %w", err)
	}

	cur := domain.Currency(currencyStr)
	money, err := domain.NewMoney(amountMinor, cur)
	if err != nil {
		return nil, fmt.Errorf("rehydrate tx money: %w", err)
	}

	var resultBal *domain.Money
	if resultBalanceMin != nil {
		rb, err := domain.NewMoney(*resultBalanceMin, cur)
		if err == nil {
			resultBal = &rb
		}
	}

	var op domain.ExternalOperation
	if originStr == string(domain.OriginExternal) {
		op = domain.ExternalOperation{
			ProviderID:                     derefString(providerID),
			ExternalTransactionID:          derefString(externalID),
			PlayerID:                       domain.ID(playerIDStr),
			WalletID:                       domain.ID(walletIDStr),
			RoundID:                        derefString(roundID),
			GameID:                         derefString(gameID),
			Kind:                           domain.Kind(kindStr),
			Money:                          money,
			ReferenceExternalTransactionID: derefString(refExternalID),
		}
	}

	var refID domain.ID
	if refTxID != nil {
		refID = domain.ID(*refTxID)
	}

	var fCode domain.FailureCode
	if failureCode != nil {
		fCode = domain.FailureCode(*failureCode)
	}

	persisted := domain.PersistedTransaction{
		ID:             domain.ID(idStr),
		Origin:         domain.Origin(originStr),
		Operation:      op,
		IdempotencyKey: derefString(idempotencyKey),
		PayloadHash:    derefString(payloadHash),
		WalletID:       domain.ID(walletIDStr),
		PlayerID:       domain.ID(playerIDStr),
		Kind:           domain.Kind(kindStr),
		Money:          money,
		ReferenceTxID:  refID,
		Status:         domain.Status(statusStr),
		FailureCode:    fCode,
		ResultBalance:  resultBal,
		Attempts:       attempts,
		NextAttemptAt:  nextAttemptAt,
		FirstPendingAt: firstPendingAt,
		CreatedAt:      createdAt,
		UpdatedAt:      updatedAt,
	}

	return domain.RehydrateTransaction(persisted), nil
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
