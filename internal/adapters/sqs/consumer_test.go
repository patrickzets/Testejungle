package sqs

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/patrickkaua/wagering/internal/app"
	"github.com/patrickkaua/wagering/internal/domain"
	"github.com/patrickkaua/wagering/internal/platform"
)

type fakeSQSClient struct {
	mu                   sync.Mutex
	receiveFunc          func(ctx context.Context, params *awssqs.ReceiveMessageInput) (*awssqs.ReceiveMessageOutput, error)
	deleteMessageHook    func(params *awssqs.DeleteMessageInput)
	changeVisibilityHook func(params *awssqs.ChangeMessageVisibilityInput)
	deleteCalls          []*awssqs.DeleteMessageInput
	visibilityCalls      []*awssqs.ChangeMessageVisibilityInput
	calls                []string
}

func (f *fakeSQSClient) ReceiveMessage(ctx context.Context, params *awssqs.ReceiveMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	rf := f.receiveFunc
	f.mu.Unlock()
	if rf != nil {
		return rf(ctx, params)
	}
	return &awssqs.ReceiveMessageOutput{}, nil
}

func (f *fakeSQSClient) DeleteMessage(ctx context.Context, params *awssqs.DeleteMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	f.calls = append(f.calls, "delete")
	f.deleteCalls = append(f.deleteCalls, params)
	hook := f.deleteMessageHook
	f.mu.Unlock()
	if hook != nil {
		hook(params)
	}
	return &awssqs.DeleteMessageOutput{}, nil
}

func (f *fakeSQSClient) ChangeMessageVisibility(ctx context.Context, params *awssqs.ChangeMessageVisibilityInput, optFns ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	f.calls = append(f.calls, "visibility")
	f.visibilityCalls = append(f.visibilityCalls, params)
	hook := f.changeVisibilityHook
	f.mu.Unlock()
	if hook != nil {
		hook(params)
	}
	return &awssqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeSQSClient) DeleteCalls() []*awssqs.DeleteMessageInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*awssqs.DeleteMessageInput, len(f.deleteCalls))
	copy(out, f.deleteCalls)
	return out
}

func (f *fakeSQSClient) VisibilityCalls() []*awssqs.ChangeMessageVisibilityInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*awssqs.ChangeMessageVisibilityInput, len(f.visibilityCalls))
	copy(out, f.visibilityCalls)
	return out
}

// In-memory repo stubs for app.Service
type stubTxRunner struct {
	onCommit func()
}

func (r stubTxRunner) WithinTx(ctx context.Context, fn func(context.Context) error) error {
	if err := fn(ctx); err != nil {
		return err
	}
	if r.onCommit != nil {
		r.onCommit()
	}
	return nil
}

type stubInbox struct {
	status  app.InboxStatus
	err     error
	beginFn func(ctx context.Context, c, msgID, hash string, now time.Time) (app.InboxStatus, error)
}

func (i stubInbox) Begin(ctx context.Context, c, msgID, hash string, now time.Time) (app.InboxStatus, error) {
	if i.beginFn != nil {
		return i.beginFn(ctx, c, msgID, hash, now)
	}
	return i.status, i.err
}

func (i stubInbox) Complete(ctx context.Context, c, msgID string, now time.Time) error {
	return nil
}

type stubWallets struct {
	wallet  *domain.Wallet
	wallets map[domain.ID]*domain.Wallet
	err     error
}

func (w stubWallets) Insert(ctx context.Context, wallet *domain.Wallet) error { return nil }
func (w stubWallets) Get(ctx context.Context, id domain.ID) (*domain.Wallet, error) {
	if w.wallets != nil {
		if wl, ok := w.wallets[id]; ok {
			return wl, nil
		}
	}
	return w.wallet, w.err
}
func (w stubWallets) GetForUpdate(ctx context.Context, id domain.ID) (*domain.Wallet, error) {
	if w.wallets != nil {
		if wl, ok := w.wallets[id]; ok {
			return wl, nil
		}
	}
	return w.wallet, w.err
}
func (w stubWallets) Update(ctx context.Context, wallet *domain.Wallet) error { return nil }

type stubTransactions struct {
	tx  *domain.WagerTransaction
	err error
}

func (t stubTransactions) InsertExternal(ctx context.Context, tx *domain.WagerTransaction) (bool, error) {
	return true, t.err
}
func (t stubTransactions) InsertInternal(ctx context.Context, tx *domain.WagerTransaction) error {
	return nil
}
func (t stubTransactions) GetByID(ctx context.Context, id domain.ID) (*domain.WagerTransaction, error) {
	return t.tx, t.err
}
func (t stubTransactions) GetByIdempotencyKey(ctx context.Context, providerID, key string) (*domain.WagerTransaction, error) {
	if t.tx == nil && t.err == nil {
		return nil, app.ErrNotFound
	}
	return t.tx, t.err
}
func (t stubTransactions) GetByExternalID(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error) {
	return t.tx, t.err
}
func (t stubTransactions) FindProcessedReversal(ctx context.Context, refTxID domain.ID) (*domain.WagerTransaction, error) {
	return t.tx, t.err
}
func (t stubTransactions) Update(ctx context.Context, tx *domain.WagerTransaction) error {
	return nil
}
func (t stubTransactions) ClaimNextDue(ctx context.Context, now time.Time) (*domain.WagerTransaction, error) {
	return nil, app.ErrNotFound
}

type stubLedger struct{}

func (stubLedger) Append(ctx context.Context, e domain.LedgerEntry) error { return nil }
func (stubLedger) List(ctx context.Context, walletID domain.ID, cursor string, limit int) (app.LedgerPage, error) {
	return app.LedgerPage{}, nil
}
func (stubLedger) Reconcile(ctx context.Context, walletID domain.ID) (app.Reconciliation, error) {
	return app.Reconciliation{}, nil
}

type stubOutbox struct{}

func (stubOutbox) Add(ctx context.Context, events ...domain.Event) error { return nil }
func (stubOutbox) Claim(ctx context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]app.OutboxRecord, error) {
	return nil, nil
}
func (stubOutbox) MarkPublished(ctx context.Context, eventID domain.ID, now time.Time) error {
	return nil
}
func (stubOutbox) Retry(ctx context.Context, eventID domain.ID, next time.Time) error { return nil }
func (stubOutbox) Lag(ctx context.Context, now time.Time) (time.Duration, error)      { return 0, nil }

func newTestService(txRunner stubTxRunner, inbox stubInbox, wallets stubWallets, txs stubTransactions) *app.Service {
	return app.NewService(app.Deps{
		Tx:           txRunner,
		Inbox:        inbox,
		Wallets:      wallets,
		Transactions: txs,
		Ledger:       stubLedger{},
		Outbox:       stubOutbox{},
	})
}

func brl(minor int64) domain.Money {
	m, _ := domain.NewMoney(minor, "BRL")
	return m
}

func testWallet(id, playerID domain.ID, minor int64) *domain.Wallet {
	now := time.Now().UTC()
	w, _ := domain.RehydrateWallet(id, playerID, brl(minor), 1, now, now)
	return w
}

func TestBackoffCalculation(t *testing.T) {
	cases := []struct {
		receiveCount int
		want         time.Duration
	}{
		{0, 2 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{5, 32 * time.Second},
		{6, 64 * time.Second},
		{7, 128 * time.Second},
		{8, 256 * time.Second},
		{9, 5 * time.Minute},
		{20, 5 * time.Minute},
	}

	for _, tc := range cases {
		got := calculateBackoff(tc.receiveCount)
		if got != tc.want {
			t.Errorf("calculateBackoff(%d) = %v, want %v", tc.receiveCount, got, tc.want)
		}
	}
}

func TestConsumer_DeleteAfterCommitOrdering(t *testing.T) {
	var callOrder []string
	var mu sync.Mutex

	fakeSQS := &fakeSQSClient{}
	wID := domain.NewID()
	pID := domain.NewID()
	wallet := testWallet(wID, pID, 10000)

	svc := newTestService(
		stubTxRunner{
			onCommit: func() {
				mu.Lock()
				callOrder = append(callOrder, "commit")
				mu.Unlock()
			},
		},
		stubInbox{status: app.InboxNew},
		stubWallets{wallet: wallet},
		stubTransactions{},
	)

	cfg := platform.Config{
		WagerQueueURL:   "http://localhost:4566/000000000000/wager-transactions.fifo",
		ConsumerWorkers: 1,
	}
	metrics := platform.NewMetrics()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	consumer := newConsumerWithAPI(fakeSQS, cfg, svc, metrics, logger)

	validMsg := types.Message{
		ReceiptHandle: aws.String("receipt-123"),
		Body: aws.String(`{
			"messageId": "msg-1",
			"type": "WagerTransactionRequested",
			"occurredAt": "2026-10-11T12:00:00Z",
			"data": {
				"providerId": "provider-a",
				"externalTransactionId": "ext-1",
				"idempotencyKey": "key-1",
				"playerId": "` + string(pID) + `",
				"walletId": "` + string(wID) + `",
				"roundId": "round-1",
				"gameId": "game-1",
				"kind": "BET",
				"money": {
					"amount": "10.00",
					"currency": "BRL"
				}
			}
		}`),
		Attributes: map[string]string{
			"ApproximateReceiveCount": "1",
		},
	}

	consumer.handleMessage(context.Background(), validMsg)

	if len(fakeSQS.deleteCalls) != 1 {
		t.Fatalf("expected 1 delete call, got %d", len(fakeSQS.deleteCalls))
	}
	if aws.ToString(fakeSQS.deleteCalls[0].ReceiptHandle) != "receipt-123" {
		t.Errorf("wrong receipt handle deleted: %v", fakeSQS.deleteCalls[0].ReceiptHandle)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(callOrder) != 1 || callOrder[0] != "commit" {
		t.Errorf("expected commit before delete, got order: %v", callOrder)
	}
}

func TestConsumer_NoDeleteOnError(t *testing.T) {
	fakeSQS := &fakeSQSClient{}
	wID := domain.NewID()
	pID := domain.NewID()

	svc := newTestService(
		stubTxRunner{},
		stubInbox{err: errors.New("database connection lost")},
		stubWallets{err: errors.New("database connection lost")},
		stubTransactions{err: errors.New("database connection lost")},
	)

	cfg := platform.Config{
		WagerQueueURL:   "http://localhost:4566/000000000000/wager-transactions.fifo",
		ConsumerWorkers: 1,
	}
	metrics := platform.NewMetrics()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	consumer := newConsumerWithAPI(fakeSQS, cfg, svc, metrics, logger)

	msg := types.Message{
		ReceiptHandle: aws.String("receipt-error"),
		Body: aws.String(`{
			"messageId": "msg-err",
			"type": "WagerTransactionRequested",
			"occurredAt": "2026-10-11T12:00:00Z",
			"data": {
				"providerId": "provider-a",
				"externalTransactionId": "ext-err",
				"idempotencyKey": "key-err",
				"playerId": "` + string(pID) + `",
				"walletId": "` + string(wID) + `",
				"roundId": "round-1",
				"gameId": "game-1",
				"kind": "BET",
				"money": {
					"amount": "10.00",
					"currency": "BRL"
				}
			}
		}`),
		Attributes: map[string]string{
			"ApproximateReceiveCount": "2",
		},
	}

	consumer.handleMessage(context.Background(), msg)

	if len(fakeSQS.deleteCalls) != 0 {
		t.Fatalf("expected message NOT to be deleted, got %d delete calls", len(fakeSQS.deleteCalls))
	}
	if len(fakeSQS.visibilityCalls) != 1 {
		t.Fatalf("expected 1 visibility change call, got %d", len(fakeSQS.visibilityCalls))
	}
	// For receiveCount 2, backoff should be 4s
	if fakeSQS.visibilityCalls[0].VisibilityTimeout != 4 {
		t.Errorf("expected visibility timeout 4s for receiveCount 2, got %d", fakeSQS.visibilityCalls[0].VisibilityTimeout)
	}
}

func TestConsumer_PoisonHandling(t *testing.T) {
	testCases := []struct {
		name       string
		body       string
		inboxErr   error
		inboxState app.InboxStatus
	}{
		{
			name: "malformed json",
			body: "{not a valid json",
		},
		{
			name: "wrong message type",
			body: `{
				"messageId": "msg-wrong-type",
				"type": "InvalidNotification",
				"occurredAt": "2026-10-11T12:00:00Z",
				"data": {}
			}`,
		},
		{
			name: "unknown field disallowed",
			body: `{
				"messageId": "msg-unknown",
				"type": "WagerTransactionRequested",
				"occurredAt": "2026-10-11T12:00:00Z",
				"unknownExtraField": "danger",
				"data": {
					"providerId": "provider-a",
					"externalTransactionId": "ext-1",
					"idempotencyKey": "key-1",
					"playerId": "` + string(domain.NewID()) + `",
					"walletId": "` + string(domain.NewID()) + `",
					"roundId": "round-1",
					"gameId": "game-1",
					"kind": "BET",
					"money": {"amount": "10.00", "currency": "BRL"}
				}
			}`,
		},
		{
			name: "invalid kind OPENING",
			body: `{
				"messageId": "msg-opening",
				"type": "WagerTransactionRequested",
				"occurredAt": "2026-10-11T12:00:00Z",
				"data": {
					"providerId": "provider-a",
					"externalTransactionId": "ext-1",
					"idempotencyKey": "key-1",
					"playerId": "` + string(domain.NewID()) + `",
					"walletId": "` + string(domain.NewID()) + `",
					"roundId": "round-1",
					"gameId": "game-1",
					"kind": "OPENING",
					"money": {"amount": "10.00", "currency": "BRL"}
				}
			}`,
		},
		{
			name:       "inbox conflict poison message",
			inboxState: app.InboxConflict,
			body: `{
				"messageId": "msg-poison",
				"type": "WagerTransactionRequested",
				"occurredAt": "2026-10-11T12:00:00Z",
				"data": {
					"providerId": "provider-a",
					"externalTransactionId": "ext-1",
					"idempotencyKey": "key-1",
					"playerId": "` + string(domain.NewID()) + `",
					"walletId": "` + string(domain.NewID()) + `",
					"roundId": "round-1",
					"gameId": "game-1",
					"kind": "BET",
					"money": {"amount": "10.00", "currency": "BRL"}
				}
			}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fakeSQS := &fakeSQSClient{}
			svc := newTestService(
				stubTxRunner{},
				stubInbox{status: tc.inboxState, err: tc.inboxErr},
				stubWallets{},
				stubTransactions{},
			)

			cfg := platform.Config{
				WagerQueueURL:   "http://localhost:4566/000000000000/wager-transactions.fifo",
				ConsumerWorkers: 1,
			}
			metrics := platform.NewMetrics()
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))

			consumer := newConsumerWithAPI(fakeSQS, cfg, svc, metrics, logger)

			msg := types.Message{
				ReceiptHandle: aws.String("receipt-" + tc.name),
				Body:          aws.String(tc.body),
				Attributes: map[string]string{
					"ApproximateReceiveCount": "1",
				},
			}

			consumer.handleMessage(context.Background(), msg)

			if len(fakeSQS.deleteCalls) != 0 {
				t.Errorf("%s: expected 0 delete calls, got %d", tc.name, len(fakeSQS.deleteCalls))
			}
			if len(fakeSQS.visibilityCalls) != 1 {
				t.Fatalf("%s: expected 1 visibility call, got %d", tc.name, len(fakeSQS.visibilityCalls))
			}
			if fakeSQS.visibilityCalls[0].VisibilityTimeout != 0 {
				t.Errorf("%s: expected visibility timeout 0, got %d", tc.name, fakeSQS.visibilityCalls[0].VisibilityTimeout)
			}
		})
	}
}

func TestGroupByMessageGroupKeepsOrderWithinGroup(t *testing.T) {
	mk := func(id, group string) types.Message {
		return types.Message{
			MessageId:  aws.String(id),
			Attributes: map[string]string{string(types.MessageSystemAttributeNameMessageGroupId): group},
		}
	}
	groups := groupByMessageGroup([]types.Message{mk("a1", "A"), mk("b1", "B"), mk("a2", "A"), mk("a3", "A"), mk("b2", "B")})
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}
	order := func(g []types.Message) string {
		out := ""
		for _, m := range g {
			out += aws.ToString(m.MessageId) + " "
		}
		return out
	}
	if order(groups[0]) != "a1 a2 a3 " || order(groups[1]) != "b1 b2 " {
		t.Fatalf("unexpected order: %q %q", order(groups[0]), order(groups[1]))
	}
}

func makeTestWagerMessage(msgID, handle, groupID, walletID, playerID, extTxID, idempKey string) types.Message {
	return types.Message{
		MessageId:     aws.String(msgID),
		ReceiptHandle: aws.String(handle),
		Attributes: map[string]string{
			string(types.MessageSystemAttributeNameMessageGroupId):          groupID,
			string(types.MessageSystemAttributeNameApproximateReceiveCount): "1",
		},
		Body: aws.String(`{
			"messageId": "` + msgID + `",
			"type": "WagerTransactionRequested",
			"occurredAt": "2026-10-11T12:00:00Z",
			"data": {
				"providerId": "provider-a",
				"externalTransactionId": "` + extTxID + `",
				"idempotencyKey": "` + idempKey + `",
				"playerId": "` + playerID + `",
				"walletId": "` + walletID + `",
				"roundId": "round-1",
				"gameId": "game-1",
				"kind": "BET",
				"money": {
					"amount": "10.00",
					"currency": "BRL"
				}
			}
		}`),
	}
}

func TestConsumer_GroupSerializationAndConcurrency(t *testing.T) {
	t.Run("same group batch stops on failure and sets remaining visibility to 0", func(t *testing.T) {
		wID := domain.NewID()
		pID := domain.NewID()
		wallet := testWallet(wID, pID, 10000)

		var mu sync.Mutex
		var handledIDs []string

		inbox := stubInbox{
			beginFn: func(ctx context.Context, c, msgID, hash string, now time.Time) (app.InboxStatus, error) {
				mu.Lock()
				handledIDs = append(handledIDs, msgID)
				mu.Unlock()
				if msgID == "msg-1" {
					return 0, errors.New("simulated error on first message")
				}
				return app.InboxNew, nil
			},
		}

		svc := newTestService(
			stubTxRunner{},
			inbox,
			stubWallets{wallet: wallet},
			stubTransactions{},
		)

		msg1 := makeTestWagerMessage("msg-1", "handle-1", "group-same", string(wID), string(pID), "ext-1", "idemp-1")
		msg2 := makeTestWagerMessage("msg-2", "handle-2", "group-same", string(wID), string(pID), "ext-2", "idemp-2")
		msg3 := makeTestWagerMessage("msg-3", "handle-3", "group-same", string(wID), string(pID), "ext-3", "idemp-3")

		batch := []types.Message{msg1, msg2, msg3}
		var returned atomic.Bool
		doneHandle3 := make(chan struct{})
		var once3 sync.Once

		fakeSQS := &fakeSQSClient{
			receiveFunc: func(ctx context.Context, params *awssqs.ReceiveMessageInput) (*awssqs.ReceiveMessageOutput, error) {
				if returned.CompareAndSwap(false, true) {
					return &awssqs.ReceiveMessageOutput{Messages: batch}, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			},
			changeVisibilityHook: func(params *awssqs.ChangeMessageVisibilityInput) {
				if aws.ToString(params.ReceiptHandle) == "handle-3" {
					once3.Do(func() { close(doneHandle3) })
				}
			},
		}

		cfg := platform.Config{
			WagerQueueURL:   "http://localhost:4566/000000000000/wager-transactions.fifo",
			ConsumerWorkers: 2,
			SQSWaitSeconds:  1,
		}
		metrics := platform.NewMetrics()
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))

		consumer := newConsumerWithAPI(fakeSQS, cfg, svc, metrics, logger)
		consumer.Start()

		select {
		case <-doneHandle3:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for handle-3 visibility to be reset")
		}

		stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := consumer.Stop(stopCtx); err != nil {
			t.Fatalf("consumer.Stop failed: %v", err)
		}

		// Assert msg 2 and msg 3 were NOT handled
		mu.Lock()
		ids := append([]string(nil), handledIDs...)
		mu.Unlock()

		if len(ids) != 1 || ids[0] != "msg-1" {
			t.Errorf("expected only msg-1 to be handled, got: %v", ids)
		}

		// Assert none were deleted
		if deleteCalls := fakeSQS.DeleteCalls(); len(deleteCalls) != 0 {
			t.Errorf("expected 0 delete calls, got %d", len(deleteCalls))
		}

		// Assert visibility of 2nd and 3rd messages was set to 0
		visCalls := fakeSQS.VisibilityCalls()
		foundHandle2 := false
		foundHandle3 := false
		for _, call := range visCalls {
			handle := aws.ToString(call.ReceiptHandle)
			if handle == "handle-2" && call.VisibilityTimeout == 0 {
				foundHandle2 = true
			}
			if handle == "handle-3" && call.VisibilityTimeout == 0 {
				foundHandle3 = true
			}
		}

		if !foundHandle2 {
			t.Errorf("expected visibility of msg 2 (handle-2) to be reset to 0, calls: %+v", visCalls)
		}
		if !foundHandle3 {
			t.Errorf("expected visibility of msg 3 (handle-3) to be reset to 0, calls: %+v", visCalls)
		}
	})

	t.Run("different groups process concurrently", func(t *testing.T) {
		wIDA, pIDA := domain.NewID(), domain.NewID()
		wIDB, pIDB := domain.NewID(), domain.NewID()

		wallets := map[domain.ID]*domain.Wallet{
			wIDA: testWallet(wIDA, pIDA, 10000),
			wIDB: testWallet(wIDB, pIDB, 10000),
		}

		aStarted := make(chan struct{})
		bStarted := make(chan struct{})

		inbox := stubInbox{
			beginFn: func(ctx context.Context, c, msgID, hash string, now time.Time) (app.InboxStatus, error) {
				if msgID == "msg-A" {
					close(aStarted)
					select {
					case <-bStarted:
					case <-time.After(2 * time.Second):
						return 0, errors.New("timeout waiting for msg-B to run concurrently")
					}
				} else if msgID == "msg-B" {
					close(bStarted)
					select {
					case <-aStarted:
					case <-time.After(2 * time.Second):
						return 0, errors.New("timeout waiting for msg-A to run concurrently")
					}
				}
				return app.InboxNew, nil
			},
		}

		svc := newTestService(
			stubTxRunner{},
			inbox,
			stubWallets{wallets: wallets},
			stubTransactions{},
		)

		msgA := makeTestWagerMessage("msg-A", "handle-A", "group-A", string(wIDA), string(pIDA), "ext-A", "idemp-A")
		msgB := makeTestWagerMessage("msg-B", "handle-B", "group-B", string(wIDB), string(pIDB), "ext-B", "idemp-B")

		batch := []types.Message{msgA, msgB}
		var returned atomic.Bool
		doneDeleted := make(chan struct{})
		var deleteCount atomic.Int32

		fakeSQS := &fakeSQSClient{
			receiveFunc: func(ctx context.Context, params *awssqs.ReceiveMessageInput) (*awssqs.ReceiveMessageOutput, error) {
				if returned.CompareAndSwap(false, true) {
					return &awssqs.ReceiveMessageOutput{Messages: batch}, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			},
			deleteMessageHook: func(params *awssqs.DeleteMessageInput) {
				if deleteCount.Add(1) == 2 {
					close(doneDeleted)
				}
			},
		}

		cfg := platform.Config{
			WagerQueueURL:   "http://localhost:4566/000000000000/wager-transactions.fifo",
			ConsumerWorkers: 2,
			SQSWaitSeconds:  1,
		}
		metrics := platform.NewMetrics()
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))

		consumer := newConsumerWithAPI(fakeSQS, cfg, svc, metrics, logger)
		consumer.Start()

		select {
		case <-doneDeleted:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for both messages to be deleted")
		}

		stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := consumer.Stop(stopCtx); err != nil {
			t.Fatalf("consumer.Stop failed: %v", err)
		}

		deleteCalls := fakeSQS.DeleteCalls()
		if len(deleteCalls) != 2 {
			t.Fatalf("expected 2 delete calls for both concurrent groups, got %d", len(deleteCalls))
		}

		deletedHandles := make(map[string]bool)
		for _, call := range deleteCalls {
			deletedHandles[aws.ToString(call.ReceiptHandle)] = true
		}
		if !deletedHandles["handle-A"] || !deletedHandles["handle-B"] {
			t.Errorf("expected handles handle-A and handle-B to be deleted, got %v", deletedHandles)
		}
	})
}
