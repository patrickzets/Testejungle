//go:build integration

package sqs_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/patrickzets/Testejungle/internal/adapters/postgres"
	sqsadapter "github.com/patrickzets/Testejungle/internal/adapters/sqs"
	"github.com/patrickzets/Testejungle/internal/app"
	"github.com/patrickzets/Testejungle/internal/domain"
	"github.com/patrickzets/Testejungle/internal/platform"
)

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func testConfig() platform.Config {
	return platform.Config{
		DatabaseURL:     getEnv("DATABASE_URL", "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable"),
		AWSEndpointURL:  getEnv("AWS_ENDPOINT_URL", "http://localhost:4566"),
		AWSRegion:       getEnv("AWS_REGION", "us-east-1"),
		WagerQueueURL:   getEnv("WAGER_QUEUE_URL", "http://localhost:4566/000000000000/wager-transactions.fifo"),
		EventsQueueURL:  getEnv("EVENTS_QUEUE_URL", "http://localhost:4566/000000000000/wager-events.fifo"),
		ConsumerWorkers: 2,
	}
}

func createUniqueQueues(t *testing.T, ctx context.Context, client *awssqs.Client, visibilitySeconds int) (string, string) {
	t.Helper()
	id := domain.NewID().String()
	dlqName := fmt.Sprintf("test-dlq-%s.fifo", id)
	mainName := fmt.Sprintf("test-wager-%s.fifo", id)

	dlqOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String(dlqName),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "false",
		},
	})
	if err != nil {
		t.Fatalf("create test dlq %s: %v", dlqName, err)
	}
	dlqURL := *dlqOut.QueueUrl

	dlqAttr, err := client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(dlqURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("get dlq attributes: %v", err)
	}
	dlqARN := dlqAttr.Attributes[string(types.QueueAttributeNameQueueArn)]

	redrivePolicy := fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":"5"}`, dlqARN)
	visStr := strconv.Itoa(visibilitySeconds)

	mainOut, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName: aws.String(mainName),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "false",
			"DeduplicationScope":        "messageGroup",
			"FifoThroughputLimit":       "perMessageGroupId",
			"VisibilityTimeout":         visStr,
			"RedrivePolicy":             redrivePolicy,
		},
	})
	if err != nil {
		t.Fatalf("create test wager queue %s: %v", mainName, err)
	}
	wagerQueueURL := *mainOut.QueueUrl

	t.Cleanup(func() {
		delCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = client.DeleteQueue(delCtx, &awssqs.DeleteQueueInput{QueueUrl: aws.String(wagerQueueURL)})
		_, _ = client.DeleteQueue(delCtx, &awssqs.DeleteQueueInput{QueueUrl: aws.String(dlqURL)})
	})

	return wagerQueueURL, dlqURL
}

func setupIntegrationDeps(t *testing.T) (*postgres.Store, *awssqs.Client, *app.Service, platform.Config) {
	t.Helper()
	cfg := testConfig()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	migrationsDir, err := filepath.Abs("../../../migrations")
	if err != nil {
		t.Fatalf("resolve migrations dir: %v", err)
	}

	migrator, err := postgres.NewMigrator(migrationsDir, cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("init migrator: %v", err)
	}
	if err := migrator.Up(); err != nil {
		t.Fatalf("run migrations up: %v", err)
	}

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(func() { pool.Close() })

	store := postgres.NewStore(pool, nil)
	client, err := sqsadapter.NewClient(ctx, cfg)
	if err != nil {
		t.Fatalf("create sqs client: %v", err)
	}

	svc := app.NewService(app.Deps{
		Tx:           store.Tx(),
		Wallets:      store.Wallets(),
		Transactions: store.Transactions(),
		Ledger:       store.Ledger(),
		Inbox:        store.Inbox(),
		Outbox:       store.Outbox(),
	})

	return store, client, svc, cfg
}

func createTestWallet(t *testing.T, ctx context.Context, store *postgres.Store, initialMinor int64) (domain.ID, domain.ID) {
	t.Helper()
	walletID := domain.NewID()
	playerID := domain.NewID()
	now := time.Now().UTC()

	initial, err := domain.NewMoney(initialMinor, "BRL")
	if err != nil {
		t.Fatalf("create money: %v", err)
	}

	openingTxID := domain.NewID()
	wallet, entry, err := domain.OpenWallet(walletID, playerID, initial, openingTxID, now)
	if err != nil {
		t.Fatalf("open wallet: %v", err)
	}

	if err := store.Wallets().Insert(ctx, wallet); err != nil {
		t.Fatalf("insert wallet: %v", err)
	}

	openingTx, err := domain.NewOpeningTransaction(openingTxID, walletID, playerID, initial, now)
	if err != nil {
		t.Fatalf("create opening tx: %v", err)
	}

	if err := store.Transactions().InsertInternal(ctx, openingTx); err != nil {
		t.Fatalf("insert opening tx: %v", err)
	}

	if entry != nil {
		if err := store.Ledger().Append(ctx, *entry); err != nil {
			t.Fatalf("append opening ledger: %v", err)
		}
	}

	return walletID, playerID
}

// 1. publish -> consume -> single debit
func TestIntegration_PublishConsumeSingleDebit(t *testing.T) {
	store, client, svc, cfg := setupIntegrationDeps(t)
	ctx := context.Background()

	wagerQueueURL, _ := createUniqueQueues(t, ctx, client, 30)
	cfg.WagerQueueURL = wagerQueueURL

	walletID, playerID := createTestWallet(t, ctx, store, 10000) // 100.00 BRL
	msgID := fmt.Sprintf("msg-debit-%s", domain.NewID())
	extTxID := fmt.Sprintf("ext-%s", domain.NewID())
	idempKey := fmt.Sprintf("idemp-%s", domain.NewID())

	envelope := sqsadapter.WagerRequestEnvelope{
		MessageID:  msgID,
		Type:       "WagerTransactionRequested",
		OccurredAt: time.Now().UTC(),
		Data: sqsadapter.WagerRequestData{
			ProviderID:            "provider-a",
			ExternalTransactionID: extTxID,
			IdempotencyKey:        idempKey,
			PlayerID:              string(playerID),
			WalletID:              string(walletID),
			RoundID:               "round-1",
			GameID:                "game-1",
			Kind:                  "BET",
			Money:                 mustMoney(2500, "BRL"), // 25.00 BRL
		},
	}
	bodyBytes, _ := json.Marshal(envelope)

	// Run consumer
	metrics := platform.NewMetrics()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	consumer := sqsadapter.NewConsumer(client, cfg, svc, metrics, logger)
	consumer.SetWaitTimeSeconds(1)
	consumer.Start()
	time.Sleep(100 * time.Millisecond)

	// Publish to WAGER_QUEUE_URL
	_, err := client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:               aws.String(cfg.WagerQueueURL),
		MessageBody:            aws.String(string(bodyBytes)),
		MessageGroupId:         aws.String(string(walletID)),
		MessageDeduplicationId: aws.String(msgID),
	})
	if err != nil {
		t.Fatalf("send message to sqs: %v", err)
	}

	// Wait for wallet to be debited
	var w *domain.Wallet
	for i := 0; i < 30; i++ {
		time.Sleep(200 * time.Millisecond)
		w, err = store.Wallets().Get(ctx, walletID)
		if err == nil && w.Balance().Amount() == "75.00" {
			break
		}
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = consumer.Stop(stopCtx)

	if w == nil || w.Balance().Amount() != "75.00" {
		t.Fatalf("expected balance 75.00, got %v (err: %v)", w, err)
	}

	tx, err := store.Transactions().GetByIdempotencyKey(ctx, "provider-a", idempKey)
	if err != nil {
		t.Fatalf("get tx: %v", err)
	}
	if tx.Status() != domain.StatusProcessed {
		t.Fatalf("expected tx status PROCESSED, got %s", tx.Status())
	}
}

// 2. same message delivered twice -> single debit
func TestIntegration_SameMessageDeliveredTwice(t *testing.T) {
	store, client, svc, cfg := setupIntegrationDeps(t)
	ctx := context.Background()

	wagerQueueURL, _ := createUniqueQueues(t, ctx, client, 30)
	cfg.WagerQueueURL = wagerQueueURL

	walletID, playerID := createTestWallet(t, ctx, store, 10000) // 100.00 BRL
	msgID := fmt.Sprintf("msg-twice-%s", domain.NewID())
	extTxID := fmt.Sprintf("ext-twice-%s", domain.NewID())
	idempKey := fmt.Sprintf("idemp-twice-%s", domain.NewID())

	envelope := sqsadapter.WagerRequestEnvelope{
		MessageID:  msgID,
		Type:       "WagerTransactionRequested",
		OccurredAt: time.Now().UTC(),
		Data: sqsadapter.WagerRequestData{
			ProviderID:            "provider-a",
			ExternalTransactionID: extTxID,
			IdempotencyKey:        idempKey,
			PlayerID:              string(playerID),
			WalletID:              string(walletID),
			RoundID:               "round-twice",
			GameID:                "game-twice",
			Kind:                  "BET",
			Money:                 mustMoney(3000, "BRL"), // 30.00 BRL
		},
	}
	bodyBytes, _ := json.Marshal(envelope)

	metrics := platform.NewMetrics()
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	consumer := sqsadapter.NewConsumer(client, cfg, svc, metrics, logger)
	consumer.SetWaitTimeSeconds(1)
	consumer.Start()
	time.Sleep(100 * time.Millisecond)

	// Send message 1st time
	sendOut, err := client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:               aws.String(cfg.WagerQueueURL),
		MessageBody:            aws.String(string(bodyBytes)),
		MessageGroupId:         aws.String(string(walletID)),
		MessageDeduplicationId: aws.String(msgID + "-1"),
	})
	if err != nil {
		t.Fatalf("send 1st message: %v", err)
	}
	t.Logf("send 1st message: ID=%s, Sequence=%v", aws.ToString(sendOut.MessageId), aws.ToString(sendOut.SequenceNumber))

	// Wait for 1st message processing
	for i := 0; i < 30; i++ {
		time.Sleep(200 * time.Millisecond)
		w, err := store.Wallets().Get(ctx, walletID)
		if err == nil && w.Balance().Amount() == "70.00" {
			break
		}
	}

	// Send same message 2nd time (duplicate message ID and payload)
	_, err = client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:               aws.String(cfg.WagerQueueURL),
		MessageBody:            aws.String(string(bodyBytes)),
		MessageGroupId:         aws.String(string(walletID)),
		MessageDeduplicationId: aws.String(msgID + "-2"),
	})
	if err != nil {
		t.Fatalf("send 2nd message: %v", err)
	}

	time.Sleep(1 * time.Second)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = consumer.Stop(stopCtx)

	w, err := store.Wallets().Get(ctx, walletID)
	if err != nil {
		t.Fatalf("get wallet: %v", err)
	}
	// Balance must still be 70.00, NOT 40.00
	if w.Balance().Amount() != "70.00" {
		t.Fatalf("expected balance 70.00 after duplicate delivery, got %s", w.Balance().Amount())
	}
}

// 3. consumer killed after commit before delete -> redelivery is a no-op
func TestIntegration_ConsumerKilledAfterCommitBeforeDelete(t *testing.T) {
	store, client, svc, cfg := setupIntegrationDeps(t)
	ctx := context.Background()

	wagerQueueURL, _ := createUniqueQueues(t, ctx, client, 30)
	cfg.WagerQueueURL = wagerQueueURL

	walletID, playerID := createTestWallet(t, ctx, store, 10000) // 100.00 BRL
	msgID := fmt.Sprintf("msg-kill-%s", domain.NewID())
	extTxID := fmt.Sprintf("ext-kill-%s", domain.NewID())
	idempKey := fmt.Sprintf("idemp-kill-%s", domain.NewID())

	envelope := sqsadapter.WagerRequestEnvelope{
		MessageID:  msgID,
		Type:       "WagerTransactionRequested",
		OccurredAt: time.Now().UTC(),
		Data: sqsadapter.WagerRequestData{
			ProviderID:            "provider-a",
			ExternalTransactionID: extTxID,
			IdempotencyKey:        idempKey,
			PlayerID:              string(playerID),
			WalletID:              string(walletID),
			RoundID:               "round-kill",
			GameID:                "game-kill",
			Kind:                  "BET",
			Money:                 mustMoney(2000, "BRL"), // 20.00 BRL
		},
	}
	bodyBytes, _ := json.Marshal(envelope)

	// Send message to SQS
	_, err := client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:               aws.String(cfg.WagerQueueURL),
		MessageBody:            aws.String(string(bodyBytes)),
		MessageGroupId:         aws.String(string(walletID)),
		MessageDeduplicationId: aws.String(msgID),
	})
	if err != nil {
		t.Fatalf("send message: %v", err)
	}

	// First: Manually execute SubmitFromMessage to simulate that the DB commit succeeded,
	// but the consumer crashed before SQS DeleteMessage was called.
	op := domain.ExternalOperation{
		ProviderID:            envelope.Data.ProviderID,
		ExternalTransactionID: envelope.Data.ExternalTransactionID,
		PlayerID:              playerID,
		WalletID:              walletID,
		RoundID:               envelope.Data.RoundID,
		GameID:                envelope.Data.GameID,
		Kind:                  domain.KindBet,
		Money:                 envelope.Data.Money,
	}
	rawHash := sha256.Sum256(bodyBytes)
	sha256Hex := hex.EncodeToString(rawHash[:])
	_, err = svc.SubmitFromMessage(ctx, "wager-consumer", msgID, sha256Hex, app.SubmitInput{
		Operation:      op,
		IdempotencyKey: idempKey,
		CorrelationID:  msgID,
	})
	if err != nil {
		t.Fatalf("pre-commit message: %v", err)
	}

	// Verify balance is 80.00
	w, _ := store.Wallets().Get(ctx, walletID)
	if w.Balance().Amount() != "80.00" {
		t.Fatalf("pre-commit balance should be 80.00, got %s", w.Balance().Amount())
	}

	// Now run the consumer. It will pick up the un-deleted message from SQS,
	// replay it through inbox without double-debiting, and delete it!
	metrics := platform.NewMetrics()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	consumer := sqsadapter.NewConsumer(client, cfg, svc, metrics, logger)
	consumer.SetWaitTimeSeconds(1)
	consumer.Start()

	time.Sleep(1500 * time.Millisecond)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = consumer.Stop(stopCtx)

	// Verify balance remains 80.00
	w, err = store.Wallets().Get(ctx, walletID)
	if err != nil {
		t.Fatalf("get wallet: %v", err)
	}
	if w.Balance().Amount() != "80.00" {
		t.Fatalf("expected balance to stay 80.00, got %s", w.Balance().Amount())
	}
}

// 4. DLQ after 5 receives for a poison message
func TestIntegration_DLQAfter5ReceivesForPoisonMessage(t *testing.T) {
	_, client, svc, cfg := setupIntegrationDeps(t)
	ctx := context.Background()

	wagerQueueURL, dlqURL := createUniqueQueues(t, ctx, client, 1)
	cfg.WagerQueueURL = wagerQueueURL

	poisonMsgID := fmt.Sprintf("poison-%s", domain.NewID())
	poisonBody := `{
		"messageId": "` + poisonMsgID + `",
		"type": "InvalidMessageTypePoison",
		"occurredAt": "2026-10-11T12:00:00Z",
		"data": {
			"invalid": true
		}
	}`

	_, err := client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:               aws.String(cfg.WagerQueueURL),
		MessageBody:            aws.String(poisonBody),
		MessageGroupId:         aws.String("poison-group"),
		MessageDeduplicationId: aws.String(poisonMsgID),
	})
	if err != nil {
		t.Fatalf("send poison message: %v", err)
	}

	metrics := platform.NewMetrics()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	consumer := sqsadapter.NewConsumer(client, cfg, svc, metrics, logger)
	consumer.SetWaitTimeSeconds(1)
	consumer.Start()

	// Wait for redrive to DLQ (after 5 receives, message moves to DLQ)
	var foundInDLQ bool
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		out, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String(dlqURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     1,
		})
		if err == nil {
			for _, m := range out.Messages {
				if m.Body != nil && *m.Body == poisonBody {
					foundInDLQ = true
					break
				}
			}
		}
		if foundInDLQ {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	_ = consumer.Stop(stopCtx)

	if !foundInDLQ {
		t.Fatalf("poison message was not found in DLQ (%s) after redrive", dlqURL)
	}
}

func mustMoney(minor int64, curr string) domain.Money {
	m, err := domain.NewMoney(minor, domain.Currency(curr))
	if err != nil {
		panic(err)
	}
	return m
}
