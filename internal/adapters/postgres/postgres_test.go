//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/patrickkaua/wagering/internal/adapters/postgres"
	"github.com/patrickkaua/wagering/internal/app"
	"github.com/patrickkaua/wagering/internal/domain"
)

func getDatabaseURL(t *testing.T) string {
	t.Helper()
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Skip("DATABASE_URL not set; skipping integration test")
	}
	return dbURL
}

func getMigrationsDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs("../../../migrations")
	if err != nil {
		t.Fatalf("resolve migrations path: %v", err)
	}
	return dir
}

func setupTestStore(t *testing.T) (*postgres.Store, *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dbURL := getDatabaseURL(t)
	migrationsDir := getMigrationsDir(t)

	migrator, err := postgres.NewMigrator(migrationsDir, dbURL)
	if err != nil {
		t.Fatalf("failed to create migrator: %v", err)
	}

	if err := migrator.Up(); err != nil {
		t.Fatalf("failed to apply migrations up: %v", err)
	}

	pool, err := postgres.NewPool(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed to connect to db pool: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
	})

	store := postgres.NewStore(pool, app.NopObserver{})
	return store, pool
}

func setupTestDB(t *testing.T) (*pgxpool.Pool, *postgres.TxRunner) {
	store, pool := setupTestStore(t)
	return pool, store.Tx().(*postgres.TxRunner)
}

func TestMigrationsUpDownUp(t *testing.T) {
	dbURL := getDatabaseURL(t)
	migrationsDir := getMigrationsDir(t)

	migrator, err := postgres.NewMigrator(migrationsDir, dbURL)
	if err != nil {
		t.Fatalf("failed to create migrator: %v", err)
	}

	// 1. Up
	if err := migrator.Up(); err != nil {
		t.Fatalf("first migrator.Up failed: %v", err)
	}

	// 2. Down
	if err := migrator.Down(); err != nil {
		t.Fatalf("migrator.Down failed: %v", err)
	}

	// 3. Up again
	if err := migrator.Up(); err != nil {
		t.Fatalf("second migrator.Up failed: %v", err)
	}
}

func TestTxRunner_WithinTx_CommitAndRollback(t *testing.T) {
	pool, runner := setupTestDB(t)
	ctx := context.Background()

	// Verify Commit
	var walletID string
	err := runner.WithinTx(ctx, func(txCtx context.Context) error {
		tx, ok := postgres.TxFromContext(txCtx)
		if !ok || tx == nil {
			return fmt.Errorf("expected tx in context")
		}

		err := tx.QueryRow(txCtx, `
			INSERT INTO wallets (player_id, currency, balance_minor, version)
			VALUES (gen_random_uuid(), 'USD', 1000, 1)
			RETURNING id
		`).Scan(&walletID)
		return err
	})
	if err != nil {
		t.Fatalf("WithinTx commit failed: %v", err)
	}

	// Verify wallet row exists
	var count int
	err = pool.QueryRow(ctx, "SELECT count(*) FROM wallets WHERE id = $1", walletID).Scan(&count)
	if err != nil || count != 1 {
		t.Fatalf("expected wallet to exist after commit, count=%d, err=%v", count, err)
	}

	// Verify Rollback on error
	expectedErr := fmt.Errorf("intentional failure")
	var uncommittedWalletID string
	err = runner.WithinTx(ctx, func(txCtx context.Context) error {
		tx, _ := postgres.TxFromContext(txCtx)
		_ = tx.QueryRow(txCtx, `
			INSERT INTO wallets (player_id, currency, balance_minor, version)
			VALUES (gen_random_uuid(), 'EUR', 500, 1)
			RETURNING id
		`).Scan(&uncommittedWalletID)
		return expectedErr
	})
	if err != expectedErr {
		t.Fatalf("expected %v, got %v", expectedErr, err)
	}

	// Verify uncommitted wallet row was rolled back
	err = pool.QueryRow(ctx, "SELECT count(*) FROM wallets WHERE id = $1", uncommittedWalletID).Scan(&count)
	if err != nil || count != 0 {
		t.Fatalf("expected rolled back wallet to not exist, count=%d, err=%v", count, err)
	}
}

func TestWalletConstraints(t *testing.T) {
	pool, _ := setupTestDB(t)
	ctx := context.Background()

	// 1. CHECK balance_minor >= 0
	_, err := pool.Exec(ctx, `
		INSERT INTO wallets (player_id, currency, balance_minor, version)
		VALUES (gen_random_uuid(), 'USD', -50, 1)
	`)
	if err == nil {
		t.Fatal("expected CHECK constraint violation on negative balance, got nil")
	}

	// 2. UNIQUE (player_id, currency)
	playerID := "a0000000-0000-0000-0000-000000000001"
	_, err = pool.Exec(ctx, `
		INSERT INTO wallets (player_id, currency, balance_minor, version)
		VALUES ($1, 'BRL', 100, 1)
	`, playerID)
	if err != nil {
		t.Fatalf("insert first wallet failed: %v", err)
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO wallets (player_id, currency, balance_minor, version)
		VALUES ($1, 'BRL', 200, 1)
	`, playerID)
	if err == nil {
		t.Fatal("expected UNIQUE(player_id, currency) violation, got nil")
	}
}

func TestWagerTransactionsConstraints(t *testing.T) {
	pool, _ := setupTestDB(t)
	ctx := context.Background()

	var walletID string
	err := pool.QueryRow(ctx, `
		INSERT INTO wallets (player_id, currency, balance_minor, version)
		VALUES (gen_random_uuid(), 'USD', 10000, 1)
		RETURNING id
	`).Scan(&walletID)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	// 1. One OPENING per wallet
	var openTxID string
	err = pool.QueryRow(ctx, `
		INSERT INTO wager_transactions (
			origin, wallet_id, player_id, kind, amount_minor, currency, status
		) VALUES (
			'INTERNAL', $1, gen_random_uuid(), 'OPENING', 10000, 'USD', 'PROCESSED'
		) RETURNING id
	`, walletID).Scan(&openTxID)
	if err != nil {
		t.Fatalf("insert OPENING tx: %v", err)
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO wager_transactions (
			origin, wallet_id, player_id, kind, amount_minor, currency, status
		) VALUES (
			'INTERNAL', $1, gen_random_uuid(), 'OPENING', 5000, 'USD', 'PROCESSED'
		)
	`, walletID)
	if err == nil {
		t.Fatal("expected violation of one OPENING per wallet, got nil")
	}

	// 2. Shape check: INTERNAL cannot have provider_id
	_, err = pool.Exec(ctx, `
		INSERT INTO wager_transactions (
			origin, provider_id, wallet_id, player_id, kind, amount_minor, currency, status
		) VALUES (
			'INTERNAL', 'provider-x', $1, gen_random_uuid(), 'OPENING', 100, 'USD', 'PROCESSED'
		)
	`, walletID)
	if err == nil {
		t.Fatal("expected shape check violation for INTERNAL with provider_id, got nil")
	}

	// 3. Shape check: EXTERNAL requires provider_id, external_id, idempotency_key, hash, round, game
	_, err = pool.Exec(ctx, `
		INSERT INTO wager_transactions (
			origin, provider_id, external_transaction_id, wallet_id, player_id,
			kind, amount_minor, currency, status
		) VALUES (
			'EXTERNAL', 'provider-x', 'ext-1', $1, gen_random_uuid(),
			'BET', 100, 'USD', 'PROCESSED'
		)
	`, walletID)
	if err == nil {
		t.Fatal("expected shape check violation for EXTERNAL missing required fields, got nil")
	}

	// 4. Valid EXTERNAL BET
	var betTxID string
	err = pool.QueryRow(ctx, `
		INSERT INTO wager_transactions (
			origin, provider_id, external_transaction_id, idempotency_key, payload_hash,
			wallet_id, player_id, round_id, game_id, kind, amount_minor, currency, status
		) VALUES (
			'EXTERNAL', 'provider-a', 'tx-bet-1', 'idem-bet-1', 'hash123',
			$1, gen_random_uuid(), 'round-1', 'game-1', 'BET', 1000, 'USD', 'PROCESSED'
		) RETURNING id
	`, walletID).Scan(&betTxID)
	if err != nil {
		t.Fatalf("valid BET insert failed: %v", err)
	}

	// 5. Unique Idempotency Key violation
	_, err = pool.Exec(ctx, `
		INSERT INTO wager_transactions (
			origin, provider_id, external_transaction_id, idempotency_key, payload_hash,
			wallet_id, player_id, round_id, game_id, kind, amount_minor, currency, status
		) VALUES (
			'EXTERNAL', 'provider-a', 'tx-bet-2', 'idem-bet-1', 'hash456',
			$1, gen_random_uuid(), 'round-1', 'game-1', 'BET', 1000, 'USD', 'PROCESSED'
		)
	`, walletID)
	if err == nil {
		t.Fatal("expected unique idempotency_key violation, got nil")
	}

	// 6. Partial unique index: only ONE PROCESSED reversal (REFUND/ROLLBACK) per reference_transaction_id
	_, err = pool.Exec(ctx, `
		INSERT INTO wager_transactions (
			origin, provider_id, external_transaction_id, idempotency_key, payload_hash,
			wallet_id, player_id, round_id, game_id, kind, amount_minor, currency,
			reference_external_transaction_id, reference_transaction_id, status
		) VALUES (
			'EXTERNAL', 'provider-a', 'tx-ref-1', 'idem-ref-1', 'hashref1',
			$1, gen_random_uuid(), 'round-1', 'game-1', 'REFUND', 1000, 'USD',
			'tx-bet-1', $2, 'PROCESSED'
		)
	`, walletID, betTxID)
	if err != nil {
		t.Fatalf("first PROCESSED REFUND failed: %v", err)
	}

	// Second PROCESSED reversal for same reference must fail
	_, err = pool.Exec(ctx, `
		INSERT INTO wager_transactions (
			origin, provider_id, external_transaction_id, idempotency_key, payload_hash,
			wallet_id, player_id, round_id, game_id, kind, amount_minor, currency,
			reference_external_transaction_id, reference_transaction_id, status
		) VALUES (
			'EXTERNAL', 'provider-a', 'tx-roll-1', 'idem-roll-1', 'hashroll1',
			$1, gen_random_uuid(), 'round-1', 'game-1', 'ROLLBACK', 1000, 'USD',
			'tx-bet-1', $2, 'PROCESSED'
		)
	`, walletID, betTxID)
	if err == nil {
		t.Fatal("expected partial unique reversal violation on second reversal, got nil")
	}
}

func TestWalletLedgerEntries_AppendOnlyAndBalanceConstraints(t *testing.T) {
	pool, _ := setupTestDB(t)
	ctx := context.Background()

	var walletID, txID string
	err := pool.QueryRow(ctx, `
		INSERT INTO wallets (player_id, currency, balance_minor, version)
		VALUES (gen_random_uuid(), 'USD', 5000, 1)
		RETURNING id
	`).Scan(&walletID)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	err = pool.QueryRow(ctx, `
		INSERT INTO wager_transactions (
			origin, wallet_id, player_id, kind, amount_minor, currency, status
		) VALUES (
			'INTERNAL', $1, gen_random_uuid(), 'OPENING', 5000, 'USD', 'PROCESSED'
		) RETURNING id
	`, walletID).Scan(&txID)
	if err != nil {
		t.Fatalf("create tx: %v", err)
	}

	// 1. Valid Ledger insert (CREDIT: balance_after = balance_before + amount)
	var ledgerID string
	err = pool.QueryRow(ctx, `
		INSERT INTO wallet_ledger_entries (
			wallet_id, transaction_id, direction, amount_minor, balance_before, balance_after
		) VALUES (
			$1, $2, 'CREDIT', 5000, 0, 5000
		) RETURNING id
	`, walletID, txID).Scan(&ledgerID)
	if err != nil {
		t.Fatalf("insert valid ledger entry failed: %v", err)
	}

	// 2. CHECK constraint: balance mismatch for CREDIT
	_, err = pool.Exec(ctx, `
		INSERT INTO wallet_ledger_entries (
			wallet_id, transaction_id, direction, amount_minor, balance_before, balance_after
		) VALUES (
			$1, gen_random_uuid(), 'CREDIT', 5000, 0, 4999
		)
	`, walletID)
	if err == nil {
		t.Fatal("expected CHECK constraint violation for invalid CREDIT balance math, got nil")
	}

	// 3. CHECK constraint: balance mismatch for DEBIT
	_, err = pool.Exec(ctx, `
		INSERT INTO wallet_ledger_entries (
			wallet_id, transaction_id, direction, amount_minor, balance_before, balance_after
		) VALUES (
			$1, gen_random_uuid(), 'DEBIT', 1000, 5000, 3000
		)
	`, walletID)
	if err == nil {
		t.Fatal("expected CHECK constraint violation for invalid DEBIT balance math, got nil")
	}

	// 4. Trigger prevents UPDATE on wallet_ledger_entries
	_, err = pool.Exec(ctx, `
		UPDATE wallet_ledger_entries SET amount_minor = 9999 WHERE id = $1
	`, ledgerID)
	if err == nil {
		t.Fatal("expected trigger error on UPDATE wallet_ledger_entries, got nil")
	}

	// 5. Trigger prevents DELETE on wallet_ledger_entries
	_, err = pool.Exec(ctx, `
		DELETE FROM wallet_ledger_entries WHERE id = $1
	`, ledgerID)
	if err == nil {
		t.Fatal("expected trigger error on DELETE wallet_ledger_entries, got nil")
	}

	// 6. Trigger prevents TRUNCATE on wallet_ledger_entries
	_, err = pool.Exec(ctx, `TRUNCATE wallet_ledger_entries`)
	if err == nil {
		t.Fatal("expected trigger error on TRUNCATE wallet_ledger_entries, got nil")
	}
}

func TestInboxAndOutbox(t *testing.T) {
	pool, _ := setupTestDB(t)
	ctx := context.Background()

	// 1. Inbox PK uniqueness
	_, err := pool.Exec(ctx, `
		INSERT INTO inbox (consumer_name, message_id, payload_hash)
		VALUES ('consumer-1', 'msg-1', 'hash-abc')
	`)
	if err != nil {
		t.Fatalf("insert inbox: %v", err)
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO inbox (consumer_name, message_id, payload_hash)
		VALUES ('consumer-1', 'msg-1', 'hash-different')
	`)
	if err == nil {
		t.Fatal("expected PK violation on inbox duplicate (consumer_name, message_id), got nil")
	}

	// 2. Outbox insertion and claimable lookup
	_, err = pool.Exec(ctx, `
		INSERT INTO outbox (
			event_id, aggregate_id, event_type, payload, next_attempt_at
		) VALUES (
			gen_random_uuid(), 'agg-1', 'WagerProcessed', '{"foo":"bar"}'::jsonb, now()
		)
	`)
	if err != nil {
		t.Fatalf("insert outbox: %v", err)
	}

	var count int
	err = pool.QueryRow(ctx, `
		SELECT count(*) FROM outbox
		WHERE published_at IS NULL AND next_attempt_at <= now()
	`).Scan(&count)
	if err != nil || count < 1 {
		t.Fatalf("expected claimable outbox rows, count=%d, err=%v", count, err)
	}
}

func TestTxRunner_RetryableErrorLogic(t *testing.T) {
	if !postgres.IsRetryable(&pgconn.PgError{Code: "40001"}) {
		t.Fatal("expected 40001 to be retryable")
	}
	if !postgres.IsRetryable(&pgconn.PgError{Code: "40P01"}) {
		t.Fatal("expected 40P01 to be retryable")
	}
	if postgres.IsRetryable(&pgconn.PgError{Code: "23505"}) {
		t.Fatal("expected 23505 to not be retryable")
	}
	if postgres.IsRetryable(errors.New("generic error")) {
		t.Fatal("expected generic error to not be retryable")
	}

	pool, runner := setupTestDB(t)
	_ = pool
	ctx := context.Background()

	attempts := 0
	err := runner.WithinTx(ctx, func(txCtx context.Context) error {
		attempts++
		if attempts < 3 {
			return &pgconn.PgError{Code: "40001", Message: "simulated serialization conflict"}
		}
		var result int
		tx, ok := postgres.TxFromContext(txCtx)
		if !ok {
			return fmt.Errorf("missing tx in context")
		}
		return tx.QueryRow(txCtx, "SELECT 1").Scan(&result)
	})
	if err != nil {
		t.Fatalf("expected retry loop to succeed, got: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestService_ConcurrentDebitSeparatePools(t *testing.T) {
	dbURL := getDatabaseURL(t)
	migrationsDir := getMigrationsDir(t)

	migrator, err := postgres.NewMigrator(migrationsDir, dbURL)
	if err != nil {
		t.Fatalf("failed to create migrator: %v", err)
	}
	if err := migrator.Up(); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	ctx := context.Background()
	pool1, err := postgres.NewPool(ctx, dbURL)
	if err != nil {
		t.Fatalf("pool1: %v", err)
	}
	defer pool1.Close()

	pool2, err := postgres.NewPool(ctx, dbURL)
	if err != nil {
		t.Fatalf("pool2: %v", err)
	}
	defer pool2.Close()

	store1 := postgres.NewStore(pool1, app.NopObserver{})
	store2 := postgres.NewStore(pool2, app.NopObserver{})

	svc1 := app.NewService(app.Deps{
		Tx:           store1.Tx(),
		Wallets:      store1.Wallets(),
		Transactions: store1.Transactions(),
		Ledger:       store1.Ledger(),
		Inbox:        store1.Inbox(),
		Outbox:       store1.Outbox(),
	})
	svc2 := app.NewService(app.Deps{
		Tx:           store2.Tx(),
		Wallets:      store2.Wallets(),
		Transactions: store2.Transactions(),
		Ledger:       store2.Ledger(),
		Inbox:        store2.Inbox(),
		Outbox:       store2.Outbox(),
	})

	playerID := domain.NewID()
	initialMoney, _ := domain.ParseMoney("100.00", "USD")
	wallet, err := svc1.OpenWallet(ctx, playerID, initialMoney, "corr-open")
	if err != nil {
		t.Fatalf("open wallet: %v", err)
	}

	betMoney, _ := domain.ParseMoney("80.00", "USD")
	var wg sync.WaitGroup
	var res1, res2 app.SubmitResult
	var err1, err2 error

	extTx1 := fmt.Sprintf("ext-cd-1-%s", domain.NewID())
	extTx2 := fmt.Sprintf("ext-cd-2-%s", domain.NewID())
	idemKey1 := fmt.Sprintf("idem-cd-1-%s", domain.NewID())
	idemKey2 := fmt.Sprintf("idem-cd-2-%s", domain.NewID())

	wg.Add(2)
	go func() {
		defer wg.Done()
		res1, err1 = svc1.Submit(ctx, app.SubmitInput{
			Operation: domain.ExternalOperation{
				ProviderID:            "prov-a",
				ExternalTransactionID: extTx1,
				PlayerID:              playerID,
				WalletID:              wallet.ID(),
				RoundID:               "round-1",
				GameID:                "game-1",
				Kind:                  domain.KindBet,
				Money:                 betMoney,
			},
			IdempotencyKey: idemKey1,
			CorrelationID:  "corr-1",
		})
	}()

	go func() {
		defer wg.Done()
		res2, err2 = svc2.Submit(ctx, app.SubmitInput{
			Operation: domain.ExternalOperation{
				ProviderID:            "prov-a",
				ExternalTransactionID: extTx2,
				PlayerID:              playerID,
				WalletID:              wallet.ID(),
				RoundID:               "round-2",
				GameID:                "game-1",
				Kind:                  domain.KindBet,
				Money:                 betMoney,
			},
			IdempotencyKey: idemKey2,
			CorrelationID:  "corr-2",
		})
	}()
	wg.Wait()

	if err1 != nil {
		t.Fatalf("unexpected error from svc1: %v", err1)
	}
	if err2 != nil {
		t.Fatalf("unexpected error from svc2: %v", err2)
	}

	successes := 0
	rejections := 0
	if res1.Transaction.Status() == domain.StatusProcessed {
		successes++
	} else if res1.Transaction.Status() == domain.StatusRejected && res1.Transaction.FailureCode() == domain.CodeInsufficientFunds {
		rejections++
	}

	if res2.Transaction.Status() == domain.StatusProcessed {
		successes++
	} else if res2.Transaction.Status() == domain.StatusRejected && res2.Transaction.FailureCode() == domain.CodeInsufficientFunds {
		rejections++
	}

	if successes != 1 || rejections != 1 {
		t.Fatalf("expected 1 PROCESSED and 1 REJECTED, got successes=%d, rejections=%d", successes, rejections)
	}

	updatedWallet, err := svc1.GetWallet(ctx, wallet.ID())
	if err != nil {
		t.Fatalf("get wallet: %v", err)
	}
	if updatedWallet.Balance().Amount() != "20.00" {
		t.Fatalf("expected balance 20.00, got %s", updatedWallet.Balance().Amount())
	}

	page, err := svc1.ListLedger(ctx, wallet.ID(), "", 10)
	if err != nil {
		t.Fatalf("list ledger: %v", err)
	}
	if len(page.Entries) != 2 {
		t.Fatalf("expected 2 ledger entries (OPENING + 1 BET), got %d", len(page.Entries))
	}
}

func TestService_FiftyParallelSubmitsSameIdempotencyKey(t *testing.T) {
	store, _ := setupTestStore(t)
	ctx := context.Background()

	svc := app.NewService(app.Deps{
		Tx:           store.Tx(),
		Wallets:      store.Wallets(),
		Transactions: store.Transactions(),
		Ledger:       store.Ledger(),
		Inbox:        store.Inbox(),
		Outbox:       store.Outbox(),
	})

	playerID := domain.NewID()
	initialMoney, _ := domain.ParseMoney("100.00", "USD")
	wallet, err := svc.OpenWallet(ctx, playerID, initialMoney, "corr-open-50")
	if err != nil {
		t.Fatalf("open wallet: %v", err)
	}

	betMoney, _ := domain.ParseMoney("10.00", "USD")
	const concurrency = 50
	var wg sync.WaitGroup
	errs := make([]error, concurrency)
	results := make([]app.SubmitResult, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = svc.Submit(ctx, app.SubmitInput{
				Operation: domain.ExternalOperation{
					ProviderID:            "prov-parallel",
					ExternalTransactionID: "ext-parallel-1",
					PlayerID:              playerID,
					WalletID:              wallet.ID(),
					RoundID:               "round-parallel",
					GameID:                "game-parallel",
					Kind:                  domain.KindBet,
					Money:                 betMoney,
				},
				IdempotencyKey: "idem-parallel-key-1",
				CorrelationID:  fmt.Sprintf("corr-%d", idx),
			})
		}(i)
	}
	wg.Wait()

	processedCount := 0
	replayCount := 0
	for i := 0; i < concurrency; i++ {
		if errs[i] != nil {
			t.Fatalf("submit %d failed: %v", i, errs[i])
		}
		if results[i].Transaction.Status() != domain.StatusProcessed {
			t.Fatalf("submit %d transaction not processed: %s", i, results[i].Transaction.Status())
		}
		if results[i].Replay {
			replayCount++
		} else {
			processedCount++
		}
	}

	if processedCount != 1 {
		t.Fatalf("expected exactly 1 non-replay processed execution, got %d", processedCount)
	}
	if replayCount != concurrency-1 {
		t.Fatalf("expected %d replays, got %d", concurrency-1, replayCount)
	}

	updatedWallet, err := svc.GetWallet(ctx, wallet.ID())
	if err != nil {
		t.Fatalf("get wallet: %v", err)
	}
	if updatedWallet.Balance().Amount() != "90.00" {
		t.Fatalf("expected balance 90.00, got %s", updatedWallet.Balance().Amount())
	}

	page, err := svc.ListLedger(ctx, wallet.ID(), "", 10)
	if err != nil {
		t.Fatalf("list ledger: %v", err)
	}
	if len(page.Entries) != 2 {
		t.Fatalf("expected 2 ledger entries (OPENING + 1 debit), got %d", len(page.Entries))
	}
}

func TestOutbox_ConcurrentClaimSeparatePools(t *testing.T) {
	dbURL := getDatabaseURL(t)
	ctx := context.Background()

	pool1, err := postgres.NewPool(ctx, dbURL)
	if err != nil {
		t.Fatalf("pool1: %v", err)
	}
	defer pool1.Close()

	pool2, err := postgres.NewPool(ctx, dbURL)
	if err != nil {
		t.Fatalf("pool2: %v", err)
	}
	defer pool2.Close()

	store1 := postgres.NewStore(pool1, app.NopObserver{})
	store2 := postgres.NewStore(pool2, app.NopObserver{})

	if _, err := pool1.Exec(ctx, "DELETE FROM outbox"); err != nil {
		t.Fatalf("clear outbox: %v", err)
	}

	now := time.Now().UTC()
	var events []domain.Event
	for i := 0; i < 10; i++ {
		evt := domain.Event{
			EventID:     domain.NewID(),
			EventType:   "TestEvent",
			AggregateID: domain.NewID(),
			OccurredAt:  now,
			Version:     1,
			Data:        []byte(`{"test":true}`),
		}
		events = append(events, evt)
	}
	if err := store1.Outbox().Add(ctx, events...); err != nil {
		t.Fatalf("add events: %v", err)
	}

	var wg sync.WaitGroup
	var claim1, claim2 []app.OutboxRecord
	var err1, err2 error

	wg.Add(2)
	go func() {
		defer wg.Done()
		claim1, err1 = store1.Outbox().Claim(ctx, "worker-1", now, 1*time.Minute, 10)
	}()
	go func() {
		defer wg.Done()
		claim2, err2 = store2.Outbox().Claim(ctx, "worker-2", now, 1*time.Minute, 10)
	}()
	wg.Wait()

	if err1 != nil {
		t.Fatalf("claim1 err: %v", err1)
	}
	if err2 != nil {
		t.Fatalf("claim2 err: %v", err2)
	}

	claimedIDs := make(map[domain.ID]string)
	for _, rec := range claim1 {
		claimedIDs[rec.Event.EventID] = "worker-1"
	}
	for _, rec := range claim2 {
		if prev, exists := claimedIDs[rec.Event.EventID]; exists {
			t.Fatalf("event %s claimed by both %s and worker-2!", rec.Event.EventID, prev)
		}
		claimedIDs[rec.Event.EventID] = "worker-2"
	}

	if len(claimedIDs) != 10 {
		t.Fatalf("expected 10 claimed events across both workers, got %d", len(claimedIDs))
	}

	targetEventID := events[0].EventID
	_, err = pool1.Exec(ctx, "UPDATE outbox SET locked_until = $1 WHERE event_id = $2", now.Add(-10*time.Second), targetEventID.String())
	if err != nil {
		t.Fatalf("expire lease: %v", err)
	}

	reclaimed, err := store1.Outbox().Claim(ctx, "worker-reclaim", now, 1*time.Minute, 10)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	found := false
	for _, r := range reclaimed {
		if r.Event.EventID == targetEventID {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected expired lease event to be claimable again")
	}
}

func TestLedger_ReconcileAndPagination(t *testing.T) {
	store, _ := setupTestStore(t)
	ctx := context.Background()

	svc := app.NewService(app.Deps{
		Tx:           store.Tx(),
		Wallets:      store.Wallets(),
		Transactions: store.Transactions(),
		Ledger:       store.Ledger(),
		Inbox:        store.Inbox(),
		Outbox:       store.Outbox(),
	})

	playerID := domain.NewID()
	initial, _ := domain.ParseMoney("100.00", "USD")
	wallet, err := svc.OpenWallet(ctx, playerID, initial, "corr-rec")
	if err != nil {
		t.Fatalf("open wallet: %v", err)
	}

	betMoney, _ := domain.ParseMoney("10.00", "USD")
	for i := 1; i <= 3; i++ {
		_, err := svc.Submit(ctx, app.SubmitInput{
			Operation: domain.ExternalOperation{
				ProviderID:            "prov-rec",
				ExternalTransactionID: fmt.Sprintf("ext-rec-%d", i),
				PlayerID:              playerID,
				WalletID:              wallet.ID(),
				RoundID:               fmt.Sprintf("round-%d", i),
				GameID:                "game-rec",
				Kind:                  domain.KindBet,
				Money:                 betMoney,
			},
			IdempotencyKey: fmt.Sprintf("idem-rec-%d", i),
			CorrelationID:  fmt.Sprintf("corr-%d", i),
		})
		if err != nil {
			t.Fatalf("submit bet %d: %v", i, err)
		}
	}

	report, err := svc.Reconcile(ctx, wallet.ID())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !report.Consistent {
		t.Fatalf("expected reconciliation to be consistent, got difference: %s", report.Difference.Amount())
	}
	if report.Stored.Amount() != "70.00" || report.Calculated.Amount() != "70.00" {
		t.Fatalf("expected 70.00, got stored=%s, calc=%s", report.Stored.Amount(), report.Calculated.Amount())
	}
	if report.Entries != 4 {
		t.Fatalf("expected 4 entries, got %d", report.Entries)
	}

	page1, err := svc.ListLedger(ctx, wallet.ID(), "", 2)
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	if len(page1.Entries) != 2 || page1.NextCursor == "" {
		t.Fatalf("expected page1 len 2 with nextCursor, got len=%d cursor=%q", len(page1.Entries), page1.NextCursor)
	}

	page2, err := svc.ListLedger(ctx, wallet.ID(), page1.NextCursor, 2)
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	if len(page2.Entries) != 2 {
		t.Fatalf("expected page2 len 2, got %d", len(page2.Entries))
	}
}

func TestInbox_BeginAndComplete(t *testing.T) {
	store, _ := setupTestStore(t)
	ctx := context.Background()

	now := time.Now().UTC()
	status, err := store.Inbox().Begin(ctx, "consumer-test", "msg-1", "hash-original", now)
	if err != nil {
		t.Fatalf("inbox begin: %v", err)
	}
	if status != app.InboxNew {
		t.Fatalf("expected InboxNew, got %d", status)
	}

	// Same hash -> Duplicate
	status, err = store.Inbox().Begin(ctx, "consumer-test", "msg-1", "hash-original", now)
	if err != nil {
		t.Fatalf("inbox duplicate: %v", err)
	}
	if status != app.InboxDuplicate {
		t.Fatalf("expected InboxDuplicate, got %d", status)
	}

	// Different hash -> Conflict
	status, err = store.Inbox().Begin(ctx, "consumer-test", "msg-1", "hash-changed", now)
	if err != nil {
		t.Fatalf("inbox conflict: %v", err)
	}
	if status != app.InboxConflict {
		t.Fatalf("expected InboxConflict, got %d", status)
	}

	if err := store.Inbox().Complete(ctx, "consumer-test", "msg-1", now); err != nil {
		t.Fatalf("inbox complete: %v", err)
	}
}
