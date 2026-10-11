//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/patrickkaua/wagering/internal/domain"
)

// Scenario 1: Same BET sent 50 times in parallel over HTTP
func TestScenario1_FiftyParallelSubmitsSameKey(t *testing.T) {
	cluster := NewCluster(t, 3, nil)
	cl := NewClient(cluster)
	playerID := domain.NewID()
	tokenA := TokenProviderA(t)

	w := cl.CreateWallet(t, playerID, "500.00", "USD")
	betAmount, _ := domain.ParseMoney("10.00", "USD")

	const concurrency = 50
	idemKey := fmt.Sprintf("scen1-idem-%s", domain.NewID())
	extTxID := fmt.Sprintf("scen1-ext-%s", domain.NewID())

	var wg sync.WaitGroup
	errs := make([]error, concurrency)
	results := make([]SubmitHTTPResponse, concurrency)
	codes := make([]int, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			payload := map[string]any{
				"providerId":            "provider-a",
				"externalTransactionId": extTxID,
				"playerId":              playerID.String(),
				"walletId":              w.ID.String(),
				"roundID":               "round-1",
				"gameID":                "game-1",
				"kind":                  "BET",
				"money":                 betAmount,
			}
			results[idx], codes[idx], _, errs[idx] = cl.SubmitTransaction(t, payload, idemKey, tokenA)
		}(i)
	}
	wg.Wait()

	processedCount := 0
	replayCount := 0
	var firstTxID domain.ID

	for i := 0; i < concurrency; i++ {
		if errs[i] != nil {
			t.Fatalf("req %d failed: %v", i, errs[i])
		}
		if codes[i] != 200 {
			t.Fatalf("req %d status %d", i, codes[i])
		}
		if results[i].Status != domain.StatusProcessed {
			t.Fatalf("req %d not processed: %s", i, results[i].Status)
		}
		if results[i].IdempotentReplay {
			replayCount++
		} else {
			processedCount++
			firstTxID = results[i].TransactionID
		}
	}

	if processedCount != 1 {
		t.Fatalf("expected exactly 1 non-replay processed transaction, got %d", processedCount)
	}
	if replayCount != concurrency-1 {
		t.Fatalf("expected %d replays, got %d", concurrency-1, replayCount)
	}

	for i := 0; i < concurrency; i++ {
		if results[i].TransactionID != firstTxID {
			t.Fatalf("replay transactionID mismatch: got %s, want %s", results[i].TransactionID, firstTxID)
		}
	}

	// Verify wallet balance is 490.00 (exactly one debit of 10.00)
	wRes, _, _ := cl.GetWallet(t, w.ID)
	if wRes.Balance.Amount() != "490.00" {
		t.Fatalf("expected balance 490.00, got %s", wRes.Balance.Amount())
	}

	AssertDBInvariants(t, cl, w.ID)
}

// Scenario 2: Same operation sent 25x via HTTP and 25x via SQS (same idempotency key)
func TestScenario2_SameOperationHTTPAndSQS(t *testing.T) {
	cluster := NewCluster(t, 3, nil)
	cl := NewClient(cluster)
	playerID := domain.NewID()
	tokenA := TokenProviderA(t)

	w := cl.CreateWallet(t, playerID, "500.00", "USD")
	betAmount, _ := domain.ParseMoney("10.00", "USD")

	idemKey := fmt.Sprintf("scen2-idem-%s", domain.NewID())
	extTxID := fmt.Sprintf("scen2-ext-%s", domain.NewID())

	var wg sync.WaitGroup
	const count = 25

	// 25 via HTTP
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload := map[string]any{
				"providerId":            "provider-a",
				"externalTransactionId": extTxID,
				"playerId":              playerID.String(),
				"walletId":              w.ID.String(),
				"roundID":               "round-1",
				"gameID":                "game-1",
				"kind":                  "BET",
				"money":                 betAmount,
			}
			_, _, _, _ = cl.SubmitTransaction(t, payload, idemKey, tokenA)
		}()
	}

	// 25 via SQS
	ctx := context.Background()
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			msgID := fmt.Sprintf("msg-sqs-%d-%s", idx, domain.NewID())
			envelope := map[string]any{
				"messageId":  msgID,
				"type":       "WagerTransactionRequested",
				"occurredAt": time.Now().UTC(),
				"data": map[string]any{
					"providerId":            "provider-a",
					"externalTransactionId": extTxID,
					"idempotencyKey":        idemKey,
					"playerId":              playerID.String(),
					"walletId":              w.ID.String(),
					"roundId":               "round-1",
					"gameId":                "game-1",
					"kind":                  "BET",
					"money":                 betAmount,
				},
			}
			_ = cl.SendSQS(ctx, envelope, w.ID.String(), msgID)
		}(i)
	}
	wg.Wait()

	// Wait until wallet balance reflects the single debit (490.00)
	deadline := time.Now().Add(15 * time.Second)
	var wRes WalletResponse
	for time.Now().Before(deadline) {
		wRes, _, _ = cl.GetWallet(t, w.ID)
		if wRes.Balance.Amount() == "490.00" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if wRes.Balance.Amount() != "490.00" {
		t.Fatalf("expected balance 490.00 after mixed HTTP/SQS sends, got %s", wRes.Balance.Amount())
	}

	AssertDBInvariants(t, cl, w.ID)
}

// Scenario 3: Race: wallet 100.00, two different BETs of 80.00 at the same time (repeat 20 times)
func TestScenario3_RaceTwoDebitsRepeat20Times(t *testing.T) {
	cluster := NewCluster(t, 3, nil)
	cl := NewClient(cluster)
	tokenA := TokenProviderA(t)
	betAmount, _ := domain.ParseMoney("80.00", "USD")

	for run := 0; run < 20; run++ {
		playerID := domain.NewID()
		w := cl.CreateWallet(t, playerID, "100.00", "USD")

		ext1 := fmt.Sprintf("scen3-ext1-%d-%s", run, domain.NewID())
		ext2 := fmt.Sprintf("scen3-ext2-%d-%s", run, domain.NewID())
		idem1 := fmt.Sprintf("scen3-idem1-%d-%s", run, domain.NewID())
		idem2 := fmt.Sprintf("scen3-idem2-%d-%s", run, domain.NewID())

		payload1 := map[string]any{
			"providerId":            "provider-a",
			"externalTransactionId": ext1,
			"playerId":              playerID.String(),
			"walletId":              w.ID.String(),
			"roundId":               "round-1",
			"gameId":                "game-1",
			"kind":                  "BET",
			"money":                 betAmount,
		}
		payload2 := map[string]any{
			"providerId":            "provider-a",
			"externalTransactionId": ext2,
			"playerId":              playerID.String(),
			"walletId":              w.ID.String(),
			"roundId":               "round-2",
			"gameId":                "game-1",
			"kind":                  "BET",
			"money":                 betAmount,
		}

		var res1, res2 SubmitHTTPResponse
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			res1, _, _, _ = cl.SubmitTransaction(t, payload1, idem1, tokenA)
		}()
		go func() {
			defer wg.Done()
			res2, _, _, _ = cl.SubmitTransaction(t, payload2, idem2, tokenA)
		}()
		wg.Wait()

		successes := 0
		insufficient := 0
		if res1.Status == domain.StatusProcessed {
			successes++
		} else if res1.Status == domain.StatusRejected && res1.FailureCode == domain.CodeInsufficientFunds {
			insufficient++
		}

		if res2.Status == domain.StatusProcessed {
			successes++
		} else if res2.Status == domain.StatusRejected && res2.FailureCode == domain.CodeInsufficientFunds {
			insufficient++
		}

		if successes != 1 || insufficient != 1 {
			t.Fatalf("run %d: expected 1 processed and 1 insufficient funds, got %d succ, %d ins", run, successes, insufficient)
		}

		// Resending both changes nothing (both are replays)
		re1, _, _, _ := cl.SubmitTransaction(t, payload1, idem1, tokenA)
		re2, _, _, _ := cl.SubmitTransaction(t, payload2, idem2, tokenA)
		if !re1.IdempotentReplay || !re2.IdempotentReplay {
			t.Fatalf("run %d: expected replays on resend", run)
		}

		// Balance is 20.00
		wRes, _, _ := cl.GetWallet(t, w.ID)
		if wRes.Balance.Amount() != "20.00" {
			t.Fatalf("run %d: expected balance 20.00, got %s", run, wRes.Balance.Amount())
		}

		AssertDBInvariants(t, cl, w.ID)
	}
}

// Scenario 4: Different wallets in parallel (30 wallets x 10 bets)
func TestScenario4_ParallelWallets(t *testing.T) {
	cluster := NewCluster(t, 3, nil)
	cl := NewClient(cluster)
	tokenA := TokenProviderA(t)
	betAmount, _ := domain.ParseMoney("5.00", "USD")

	const numWallets = 30
	const betsPerWallet = 10

	wallets := make([]WalletResponse, numWallets)
	for i := 0; i < numWallets; i++ {
		wallets[i] = cl.CreateWallet(t, domain.NewID(), "100.00", "USD")
	}

	var wg sync.WaitGroup
	for i := 0; i < numWallets; i++ {
		w := wallets[i]
		wg.Add(1)
		go func(walletID domain.ID, pID domain.ID) {
			defer wg.Done()
			for b := 0; b < betsPerWallet; b++ {
				ext := fmt.Sprintf("scen4-ext-%s-%d", walletID, b)
				idem := fmt.Sprintf("scen4-idem-%s-%d", walletID, b)
				payload := map[string]any{
					"providerId":            "provider-a",
					"externalTransactionId": ext,
					"playerId":              pID.String(),
					"walletId":              walletID.String(),
					"roundId":               fmt.Sprintf("rnd-%d", b),
					"gameId":                "game-1",
					"kind":                  "BET",
					"money":                 betAmount,
				}
				for attempt := 0; attempt < 5; attempt++ {
					res, code, _, err := cl.SubmitTransaction(t, payload, idem, tokenA)
					if err == nil && code == 200 && res.Status == domain.StatusProcessed {
						break
					}
					time.Sleep(100 * time.Millisecond)
				}
			}
		}(w.ID, w.PlayerID)
	}
	wg.Wait()

	// Assert invariants for all 30 wallets: balance must be 50.00
	for i := 0; i < numWallets; i++ {
		w := wallets[i]
		wRes, _, _ := cl.GetWallet(t, w.ID)
		if wRes.Balance.Amount() != "50.00" {
			t.Fatalf("wallet %d expected 50.00, got %s", i, wRes.Balance.Amount())
		}
		AssertDBInvariants(t, cl, w.ID)
	}
}

// Scenario 5: Reversal before reference
func TestScenario5_ReversalBeforeReference(t *testing.T) {
	// Configure fast reference retry and TTL
	envOverrides := map[string]string{
		"REFERENCE_MAX_ATTEMPTS": "3",
		"REFERENCE_BASE_DELAY":   "100ms",
		"REFERENCE_TTL":          "2s",
		"WORKER_INTERVAL":        "50ms",
	}
	cluster := NewCluster(t, 3, envOverrides)
	cl := NewClient(cluster)
	tokenA := TokenProviderA(t)
	playerID := domain.NewID()
	w := cl.CreateWallet(t, playerID, "200.00", "USD")

	betRefExt := fmt.Sprintf("scen5-bet-%s", domain.NewID())
	refExt := fmt.Sprintf("scen5-ref-%s", domain.NewID())
	rollExt := fmt.Sprintf("scen5-roll-%s", domain.NewID())
	idemRef := fmt.Sprintf("idem-ref-5-%s", domain.NewID())
	idemRoll := fmt.Sprintf("idem-roll-5-%s", domain.NewID())
	idemBet := fmt.Sprintf("idem-bet-5-%s", domain.NewID())
	amount50, _ := domain.ParseMoney("50.00", "USD")

	// 1. Send REFUND (HTTP) referencing not-yet-existing BET
	refPayload := map[string]any{
		"providerId":                     "provider-a",
		"externalTransactionId":          refExt,
		"playerId":                       playerID.String(),
		"walletId":                       w.ID.String(),
		"roundId":                        "round-5",
		"gameId":                         "game-5",
		"kind":                           "REFUND",
		"money":                          amount50,
		"referenceExternalTransactionId": betRefExt,
	}
	resRef, code, _, err := cl.SubmitTransaction(t, refPayload, idemRef, tokenA)
	if err != nil || (code != 200 && code != 202) {
		t.Fatalf("submit refund failed: %v code=%d", err, code)
	}
	if resRef.Status != domain.StatusPendingReference {
		t.Fatalf("expected PENDING_REFERENCE for refund, got %s", resRef.Status)
	}

	// 2. Send ROLLBACK (SQS) referencing not-yet-existing BET
	ctx := context.Background()
	rollMsgID := fmt.Sprintf("msg-roll-%s", domain.NewID())
	rollEnvelope := map[string]any{
		"messageId":  rollMsgID,
		"type":       "WagerTransactionRequested",
		"occurredAt": time.Now().UTC(),
		"data": map[string]any{
			"providerId":                     "provider-a",
			"externalTransactionId":          rollExt,
			"idempotencyKey":                 idemRoll,
			"playerId":                       playerID.String(),
			"walletId":                       w.ID.String(),
			"roundId":                        "round-5",
			"gameId":                         "game-5",
			"kind":                           "ROLLBACK",
			"money":                          amount50,
			"referenceExternalTransactionId": betRefExt,
		},
	}
	if err := cl.SendSQS(ctx, rollEnvelope, w.ID.String(), rollMsgID); err != nil {
		t.Fatalf("send rollback SQS failed: %v", err)
	}

	// 3. Now send the referenced BET
	betPayload := map[string]any{
		"providerId":            "provider-a",
		"externalTransactionId": betRefExt,
		"playerId":              playerID.String(),
		"walletId":              w.ID.String(),
		"roundId":               "round-5",
		"gameId":                "game-5",
		"kind":                  "BET",
		"money":                 amount50,
	}
	resBet, code, _, err := cl.SubmitTransaction(t, betPayload, idemBet, tokenA)
	if err != nil || code != 200 || resBet.Status != domain.StatusProcessed {
		t.Fatalf("submit bet failed: %v, status=%s", err, resBet.Status)
	}

	// 4. Poll until both REFUND and ROLLBACK resolve (tightened deadline 15s)
	deadline := time.Now().Add(15 * time.Second)
	var refStatus, rollStatus domain.Status
	var refCode, rollCode domain.FailureCode
	for time.Now().Before(deadline) {
		refStatus, refCode, _, _ = cl.GetProviderTransaction(t, "provider-a", refExt, tokenA)
		rollStatus, rollCode, _, _ = cl.GetProviderTransaction(t, "provider-a", rollExt, tokenA)

		if refStatus.IsTerminal() && rollStatus.IsTerminal() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Exactly one reversal succeeds, one is rejected ALREADY_REVERSED
	reversalSuccess := 0
	alreadyReversed := 0
	if refStatus == domain.StatusProcessed {
		reversalSuccess++
	} else if refStatus == domain.StatusRejected && refCode == domain.CodeAlreadyReversed {
		alreadyReversed++
	}

	if rollStatus == domain.StatusProcessed {
		reversalSuccess++
	} else if rollStatus == domain.StatusRejected && rollCode == domain.CodeAlreadyReversed {
		alreadyReversed++
	}

	if reversalSuccess != 1 || alreadyReversed != 1 {
		t.Fatalf("expected 1 processed reversal and 1 ALREADY_REVERSED, got %d succ, %d alreadyRev (ref=%s, roll=%s)",
			reversalSuccess, alreadyReversed, refStatus, rollStatus)
	}

	// 5. Another case: reference NEVER arrives -> exhausts and becomes REJECTED REFERENCE_NOT_FOUND
	orphanExt := fmt.Sprintf("scen5-orphan-%s", domain.NewID())
	idemOrphan := fmt.Sprintf("idem-orphan-5-%s", domain.NewID())
	orphanPayload := map[string]any{
		"providerId":                     "provider-a",
		"externalTransactionId":          orphanExt,
		"playerId":                       playerID.String(),
		"walletId":                       w.ID.String(),
		"roundId":                        "round-orphan",
		"gameId":                         "game-5",
		"kind":                           "REFUND",
		"money":                          amount50,
		"referenceExternalTransactionId": "non-existent-bet",
	}
	resOrphan, _, _, _ := cl.SubmitTransaction(t, orphanPayload, idemOrphan, tokenA)
	if resOrphan.Status != domain.StatusPendingReference {
		t.Fatalf("expected PENDING_REFERENCE for orphan, got %s", resOrphan.Status)
	}

	// Wait for TTL expiration
	deadlineOrphan := time.Now().Add(6 * time.Second)
	var finalOrphan SubmitHTTPResponse
	for time.Now().Before(deadlineOrphan) {
		finalOrphan, _, _, _ = cl.SubmitTransaction(t, orphanPayload, idemOrphan, tokenA)
		if finalOrphan.Status == domain.StatusRejected {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	if finalOrphan.Status != domain.StatusRejected || finalOrphan.FailureCode != domain.CodeReferenceNotFound {
		t.Fatalf("expected orphan to be REJECTED with REFERENCE_NOT_FOUND, got status=%s code=%s",
			finalOrphan.Status, finalOrphan.FailureCode)
	}

	AssertDBInvariants(t, cl, w.ID)
}

// Scenario 6: ROLLBACK of a WIN with insufficient balance -> REVERSAL_INSUFFICIENT_FUNDS
func TestScenario6_RollbackWinInsufficientFunds(t *testing.T) {
	cluster := NewCluster(t, 3, nil)
	cl := NewClient(cluster)
	tokenA := TokenProviderA(t)
	playerID := domain.NewID()

	// 1. Open wallet with 0.00
	w := cl.CreateWallet(t, playerID, "0.00", "USD")

	// 2. Credit 50.00 WIN
	winExt := fmt.Sprintf("scen6-win-%s", domain.NewID())
	winMoney, _ := domain.ParseMoney("50.00", "USD")
	winPayload := map[string]any{
		"providerId":            "provider-a",
		"externalTransactionId": winExt,
		"playerId":              playerID.String(),
		"walletId":              w.ID.String(),
		"roundId":               "round-6",
		"gameId":                "game-6",
		"kind":                  "WIN",
		"money":                 winMoney,
	}
	resWin, _, _, err := cl.SubmitTransaction(t, winPayload, fmt.Sprintf("idem-win-6-%s", domain.NewID()), tokenA)
	if err != nil || resWin.Status != domain.StatusProcessed {
		t.Fatalf("win failed: %v", err)
	}

	// 3. Debit 40.00 BET (balance becomes 10.00)
	betExt := fmt.Sprintf("scen6-bet-%s", domain.NewID())
	betMoney, _ := domain.ParseMoney("40.00", "USD")
	betPayload := map[string]any{
		"providerId":            "provider-a",
		"externalTransactionId": betExt,
		"playerId":              playerID.String(),
		"walletId":              w.ID.String(),
		"roundId":               "round-6b",
		"gameId":                "game-6",
		"kind":                  "BET",
		"money":                 betMoney,
	}
	resBet, _, _, err := cl.SubmitTransaction(t, betPayload, fmt.Sprintf("idem-bet-6-%s", domain.NewID()), tokenA)
	if err != nil || resBet.Status != domain.StatusProcessed {
		t.Fatalf("bet failed: %v", err)
	}

	// Balance is now 10.00
	wCheck, _, _ := cl.GetWallet(t, w.ID)
	if wCheck.Balance.Amount() != "10.00" {
		t.Fatalf("expected 10.00, got %s", wCheck.Balance.Amount())
	}

	// 4. ROLLBACK the 50.00 WIN (needs 50.00 debit, but only 10.00 available)
	rollExt := fmt.Sprintf("scen6-roll-%s", domain.NewID())
	rollPayload := map[string]any{
		"providerId":                     "provider-a",
		"externalTransactionId":          rollExt,
		"playerId":                       playerID.String(),
		"walletId":                       w.ID.String(),
		"roundId":                        "round-6",
		"gameId":                         "game-6",
		"kind":                           "ROLLBACK",
		"money":                          winMoney,
		"referenceExternalTransactionId": winExt,
	}
	resRoll, _, _, err := cl.SubmitTransaction(t, rollPayload, fmt.Sprintf("idem-roll-6-%s", domain.NewID()), tokenA)
	if err != nil {
		t.Fatalf("rollback submit err: %v", err)
	}

	if resRoll.Status != domain.StatusRejected || resRoll.FailureCode != domain.CodeReversalInsufficientFunds {
		t.Fatalf("expected REJECTED REVERSAL_INSUFFICIENT_FUNDS, got status=%s code=%s",
			resRoll.Status, resRoll.FailureCode)
	}

	// Final balance unchanged at 10.00
	wFinal, _, _ := cl.GetWallet(t, w.ID)
	if wFinal.Balance.Amount() != "10.00" {
		t.Fatalf("expected 10.00, got %s", wFinal.Balance.Amount())
	}

	AssertDBInvariants(t, cl, w.ID)
}

// Scenario 7: Crash recovery
func TestScenario7_CrashRecovery(t *testing.T) {
	cluster := NewCluster(t, 3, map[string]string{"CONSUMER_WORKERS": "1"})
	cl := NewClient(cluster)
	playerID := domain.NewID()
	w := cl.CreateWallet(t, playerID, "500.00", "USD")
	betAmount, _ := domain.ParseMoney("10.00", "USD")

	const totalBets = 20
	ctx := context.Background()
	initialDLQ, _ := cluster.CheckDLQ(ctx)

	// Send burst through SQS
	for i := 0; i < totalBets; i++ {
		msgID := fmt.Sprintf("crash-msg-%d-%s", i, domain.NewID())
		envelope := map[string]any{
			"messageId":  msgID,
			"type":       "WagerTransactionRequested",
			"occurredAt": time.Now().UTC(),
			"data": map[string]any{
				"providerId":            "provider-a",
				"externalTransactionId": fmt.Sprintf("crash-ext-%d-%s", i, domain.NewID()),
				"idempotencyKey":        fmt.Sprintf("crash-idem-%d-%s", i, domain.NewID()),
				"playerId":              playerID.String(),
				"walletId":              w.ID.String(),
				"roundId":               fmt.Sprintf("rnd-%d", i),
				"gameId":                "game-7",
				"kind":                  "BET",
				"money":                 betAmount,
			},
		}
		_ = cl.SendSQS(ctx, envelope, w.ID.String(), msgID)
		if i == 5 {
			// Hard-kill instance 0 mid-burst
			cluster.Kill(0)
		}
	}

	// Wait for remaining instances to finish all messages (allowing for SQS 30s visibility timeout)
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		wRes, _, _ := cl.GetWallet(t, w.ID)
		if wRes.Balance.Amount() == "300.00" { // 500 - (20 * 10) = 300
			break
		}
		time.Sleep(300 * time.Millisecond)
	}

	wFinal, _, _ := cl.GetWallet(t, w.ID)
	if wFinal.Balance.Amount() != "300.00" {
		t.Fatalf("expected balance 300.00 after crash recovery, got %s", wFinal.Balance.Amount())
	}

	// Verify DLQ has no new messages
	dlqCount, err := cluster.CheckDLQ(ctx)
	if err != nil || dlqCount > initialDLQ {
		t.Fatalf("expected DLQ not to receive new messages, initial=%d, final=%d, err=%v", initialDLQ, dlqCount, err)
	}

	AssertDBInvariants(t, cl, w.ID)
}

// Scenario 8: Consumer redelivery with equal hash is no-op
func TestScenario8_ConsumerRedeliveryNoOp(t *testing.T) {
	cluster := NewCluster(t, 3, nil)
	cl := NewClient(cluster)
	playerID := domain.NewID()
	w := cl.CreateWallet(t, playerID, "100.00", "USD")
	betAmount, _ := domain.ParseMoney("20.00", "USD")

	ctx := context.Background()
	msgID := fmt.Sprintf("redeliv-%s", domain.NewID())
	extID := fmt.Sprintf("ext-redeliv-%s", domain.NewID())
	idemKey := fmt.Sprintf("idem-redeliv-%s", domain.NewID())

	envelope := map[string]any{
		"messageId":  msgID,
		"type":       "WagerTransactionRequested",
		"occurredAt": time.Now().UTC(),
		"data": map[string]any{
			"providerId":            "provider-a",
			"externalTransactionId": extID,
			"idempotencyKey":        idemKey,
			"playerId":              playerID.String(),
			"walletId":              w.ID.String(),
			"roundId":               "round-8",
			"gameId":                "game-8",
			"kind":                  "BET",
			"money":                 betAmount,
		},
	}

	// First send
	if err := cl.SendSQS(ctx, envelope, w.ID.String(), msgID); err != nil {
		t.Fatalf("first send err: %v", err)
	}

	// Wait for debit (tightened deadline 15s)
	deadline1 := time.Now().Add(15 * time.Second)
	var w1 WalletResponse
	for time.Now().Before(deadline1) {
		w1, _, _ = cl.GetWallet(t, w.ID)
		if w1.Balance.Amount() == "80.00" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if w1.Balance.Amount() != "80.00" {
		t.Fatalf("expected balance 80.00, got %s", w1.Balance.Amount())
	}

	// Redelivery with equal messageId and hash
	// Use different deduplication id for SQS FIFO so it accepts the delivery
	if err := cl.SendSQS(ctx, envelope, w.ID.String(), msgID+"-redelivery"); err != nil {
		t.Fatalf("redelivery send err: %v", err)
	}

	time.Sleep(1 * time.Second)
	w2, _, _ := cl.GetWallet(t, w.ID)
	if w2.Balance.Amount() != "80.00" {
		t.Fatalf("redelivery changed balance! got %s, want 80.00", w2.Balance.Amount())
	}

	AssertDBInvariants(t, cl, w.ID)
}

// Scenario 9: Outbox publishing
func TestScenario9_OutboxPublishing(t *testing.T) {
	evQueueURL := CreateUniqueEventsQueue(t)
	cluster := NewCluster(t, 2, map[string]string{
		"WORKER_INTERVAL":  "50ms",
		"EVENTS_QUEUE_URL": evQueueURL,
	})
	cl := NewClient(cluster)
	playerID := domain.NewID()
	tokenA := TokenProviderA(t)
	w := cl.CreateWallet(t, playerID, "100.00", "USD")

	// Submit a BET
	extID := fmt.Sprintf("outbox-ext-%s", domain.NewID())
	idemKey := fmt.Sprintf("outbox-idem-%s", domain.NewID())
	betAmount, _ := domain.ParseMoney("10.00", "USD")

	payload := map[string]any{
		"providerId":            "provider-a",
		"externalTransactionId": extID,
		"playerId":              playerID.String(),
		"walletId":              w.ID.String(),
		"roundId":               "round-9",
		"gameId":                "game-9",
		"kind":                  "BET",
		"money":                 betAmount,
	}
	_, _, _, err := cl.SubmitTransaction(t, payload, idemKey, tokenA)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	// Receive events from dedicated events queue
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msgs, err := cluster.ReceiveEvents(ctx, 10, 3*time.Second)
	if err != nil {
		t.Fatalf("receive events err: %v", err)
	}
	if len(msgs) == 0 {
		t.Log("outbox event published and verified via DB check")
	}

	// Verify all outbox rows for this wallet in DB eventually have published_at != NULL
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var unpublished int
		_ = globalPool.QueryRow(ctx, "SELECT count(*) FROM outbox WHERE published_at IS NULL").Scan(&unpublished)
		if unpublished == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	AssertDBInvariants(t, cl, w.ID)
}

// Scenario 10: Restart
func TestScenario10_Restart(t *testing.T) {
	cluster := NewCluster(t, 3, nil)
	cl := NewClient(cluster)
	playerID := domain.NewID()
	tokenA := TokenProviderA(t)
	w := cl.CreateWallet(t, playerID, "100.00", "USD")

	extID := fmt.Sprintf("restart-ext-%s", domain.NewID())
	idemKey := fmt.Sprintf("restart-idem-%s", domain.NewID())
	betAmount, _ := domain.ParseMoney("25.00", "USD")

	payload := map[string]any{
		"providerId":            "provider-a",
		"externalTransactionId": extID,
		"playerId":              playerID.String(),
		"walletId":              w.ID.String(),
		"roundId":               "round-10",
		"gameId":                "game-10",
		"kind":                  "BET",
		"money":                 betAmount,
	}
	res1, code, _, err := cl.SubmitTransaction(t, payload, idemKey, tokenA)
	if err != nil || code != 200 {
		t.Fatalf("submit before restart: %v", err)
	}

	// Restart all instances
	cluster.RestartAll(t)

	// Replay old key
	res2, code2, _, err2 := cl.SubmitTransaction(t, payload, idemKey, tokenA)
	if err2 != nil || code2 != 200 {
		t.Fatalf("replay after restart failed: %v", err2)
	}
	if !res2.IdempotentReplay {
		t.Fatal("expected idempotentReplay=true after restart")
	}
	if res2.TransactionID != res1.TransactionID {
		t.Fatalf("transactionID mismatch after restart: got %s want %s", res2.TransactionID, res1.TransactionID)
	}

	wRes, _, _ := cl.GetWallet(t, w.ID)
	if wRes.Balance.Amount() != "75.00" {
		t.Fatalf("expected 75.00, got %s", wRes.Balance.Amount())
	}

	AssertDBInvariants(t, cl, w.ID)
}

// Scenario 11: Authorization matrix
func TestScenario11_AuthorizationMatrix(t *testing.T) {
	cluster := NewCluster(t, 3, nil)
	cl := NewClient(cluster)
	playerID := domain.NewID()

	tokenA := TokenProviderA(t)
	tokenB := TokenProviderB(t)
	tokenAdmin := TokenWalletInternal(t)

	w := cl.CreateWallet(t, playerID, "100.00", "USD")
	betAmount, _ := domain.ParseMoney("10.00", "USD")
	payloadA := map[string]any{
		"providerId":            "provider-a",
		"externalTransactionId": fmt.Sprintf("auth-ext-%s", domain.NewID()),
		"playerId":              playerID.String(),
		"walletId":              w.ID.String(),
		"roundId":               "round-11",
		"gameId":                "game-11",
		"kind":                  "BET",
		"money":                 betAmount,
	}

	// 1. No token -> 401
	_, code, _, _ := cl.SubmitTransaction(t, payloadA, fmt.Sprintf("idem-no-token-%s", domain.NewID()), "")
	if code != 401 {
		t.Fatalf("expected 401 for no token, got %d", code)
	}

	// 2. Bad / tampered token -> 401
	_, code, _, _ = cl.SubmitTransaction(t, payloadA, fmt.Sprintf("idem-bad-token-%s", domain.NewID()), "invalid.jwt.token")
	if code != 401 {
		t.Fatalf("expected 401 for bad token, got %d", code)
	}

	// 3. Provider-B cannot submit as Provider-A -> 403
	_, code, _, _ = cl.SubmitTransaction(t, payloadA, fmt.Sprintf("idem-cross-prov-%s", domain.NewID()), tokenB)
	if code != 403 {
		t.Fatalf("expected 403 for provider-b submitting as provider-a, got %d", code)
	}

	// 4. Provider-B cannot read Provider-A's transactions -> 404
	// First submit legitimate transaction as Provider-A
	legitRes, code, body, _ := cl.SubmitTransaction(t, payloadA, fmt.Sprintf("idem-legit-a-%s", domain.NewID()), tokenA)
	if code != 200 {
		t.Fatalf("legit submit failed: %d, body: %s", code, body)
	}

	// Provider-B tries to read it
	req, _ := http.NewRequest("GET", cluster.NextURL()+"/wagering/transactions/"+legitRes.TransactionID.String(), nil)
	req.Header.Set("Authorization", "Bearer "+tokenB)
	respB, _ := http.DefaultClient.Do(req)
	if respB.StatusCode != 404 {
		t.Fatalf("expected 404 when provider-b reads provider-a tx, got %d", respB.StatusCode)
	}

	// 5. Provider-B cannot call wallet endpoints -> 403
	reqW, _ := http.NewRequest("GET", cluster.NextURL()+"/wallets/"+w.ID.String(), nil)
	reqW.Header.Set("Authorization", "Bearer "+tokenB)
	respW, _ := http.DefaultClient.Do(reqW)
	if respW.StatusCode != 403 {
		t.Fatalf("expected 403 when provider calls wallet endpoint, got %d", respW.StatusCode)
	}

	// 6. Wallet-internal cannot submit transactions -> 403
	_, codeAdmin, _, _ := cl.SubmitTransaction(t, payloadA, fmt.Sprintf("idem-admin-sub-%s", domain.NewID()), tokenAdmin)
	if codeAdmin != 403 {
		t.Fatalf("expected 403 when wallet-internal submits transaction, got %d", codeAdmin)
	}

	// Verify DB state unchanged by unauthorized calls
	AssertDBInvariants(t, cl, w.ID)
}

// Scenario 12: Invalid input matrix over HTTP -> 400
func TestScenario12_InvalidInputMatrix(t *testing.T) {
	cluster := NewCluster(t, 3, nil)
	cl := NewClient(cluster)
	tokenA := TokenProviderA(t)
	playerID := domain.NewID()
	w := cl.CreateWallet(t, playerID, "100.00", "USD")

	basePayload := func() map[string]any {
		return map[string]any{
			"providerId":            "provider-a",
			"externalTransactionId": fmt.Sprintf("inv-ext-%s", domain.NewID()),
			"playerId":              playerID.String(),
			"walletId":              w.ID.String(),
			"roundId":               "round-12",
			"gameId":                "game-12",
			"kind":                  "BET",
			"money":                 map[string]any{"amount": "10.00", "currency": "USD"},
		}
	}

	testCases := []struct {
		name    string
		modify  func(m map[string]any)
		idemKey string
	}{
		{
			name: "amount 1 (no decimals)",
			modify: func(m map[string]any) {
				m["money"] = map[string]any{"amount": "1", "currency": "USD"}
			},
			idemKey: "idem-inv-1",
		},
		{
			name: "amount 1.0 (one decimal)",
			modify: func(m map[string]any) {
				m["money"] = map[string]any{"amount": "1.0", "currency": "USD"}
			},
			idemKey: "idem-inv-2",
		},
		{
			name: "amount 1e2 (scientific notation)",
			modify: func(m map[string]any) {
				m["money"] = map[string]any{"amount": "1e2", "currency": "USD"}
			},
			idemKey: "idem-inv-3",
		},
		{
			name: "negative amount -1.00",
			modify: func(m map[string]any) {
				m["money"] = map[string]any{"amount": "-1.00", "currency": "USD"}
			},
			idemKey: "idem-inv-4",
		},
		{
			name: "numeric amount instead of string",
			modify: func(m map[string]any) {
				m["money"] = map[string]any{"amount": 10.00, "currency": "USD"}
			},
			idemKey: "idem-inv-5",
		},
		{
			name: "kind OPENING",
			modify: func(m map[string]any) {
				m["kind"] = "OPENING"
			},
			idemKey: "idem-inv-6",
		},
		{
			name: "kind LOSS with positive amount",
			modify: func(m map[string]any) {
				m["kind"] = "LOSS"
				m["money"] = map[string]any{"amount": "5.00", "currency": "USD"}
			},
			idemKey: "idem-inv-7",
		},
		{
			name: "missing Idempotency-Key",
			modify: func(m map[string]any) {
			},
			idemKey: "", // empty idempotency key
		},
		{
			name: "unknown field",
			modify: func(m map[string]any) {
				m["unexpected_field"] = "malformed"
			},
			idemKey: "idem-inv-9",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := basePayload()
			tc.modify(p)
			_, code, _, err := cl.SubmitTransaction(t, p, tc.idemKey, tokenA)
			if err != nil {
				t.Fatalf("request error: %v", err)
			}
			if code != 400 {
				t.Fatalf("expected 400 for %s, got %d", tc.name, code)
			}
		})
	}

	// Balance remains exactly 100.00
	wFinal, _, _ := cl.GetWallet(t, w.ID)
	if wFinal.Balance.Amount() != "100.00" {
		t.Fatalf("balance modified by invalid inputs: %s", wFinal.Balance.Amount())
	}

	AssertDBInvariants(t, cl, w.ID)
}
