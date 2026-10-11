package sqs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/patrickzets/Testejungle/internal/app"
	"github.com/patrickzets/Testejungle/internal/domain"
	"github.com/patrickzets/Testejungle/internal/platform"
)

var (
	ErrWrongMessageType = errors.New("wrong message type")
	ErrMalformedMessage = errors.New("malformed message")
)

type sqsAPI interface {
	ReceiveMessage(ctx context.Context, params *awssqs.ReceiveMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *awssqs.DeleteMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *awssqs.ChangeMessageVisibilityInput, optFns ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error)
}

type WagerRequestEnvelope struct {
	MessageID  string           `json:"messageId"`
	Type       string           `json:"type"`
	OccurredAt time.Time        `json:"occurredAt"`
	Data       WagerRequestData `json:"data"`
}

type WagerRequestData struct {
	ProviderID                     string       `json:"providerId"`
	ExternalTransactionID          string       `json:"externalTransactionId"`
	IdempotencyKey                 string       `json:"idempotencyKey"`
	PlayerID                       string       `json:"playerId"`
	WalletID                       string       `json:"walletId"`
	RoundID                        string       `json:"roundId"`
	GameID                         string       `json:"gameId"`
	Kind                           string       `json:"kind"`
	Money                          domain.Money `json:"money"`
	ReferenceExternalTransactionID string       `json:"referenceExternalTransactionId,omitempty"`
}

// Consumer long-polls the wager queue and processes requests concurrently.
type Consumer struct {
	client  sqsAPI
	cfg     platform.Config
	svc     *app.Service
	metrics *platform.Metrics
	log     *slog.Logger

	waitTimeSeconds int32

	mu         sync.Mutex
	cancel     context.CancelFunc
	workCancel context.CancelFunc
	done       chan struct{}

	sem        chan struct{}
	inFlightMu sync.Mutex
	inFlight   map[string]types.Message
	inFlightWg sync.WaitGroup
}

// NewConsumer creates a consumer for the wager SQS FIFO queue.
func NewConsumer(client *awssqs.Client, cfg platform.Config, svc *app.Service, m *platform.Metrics, log *slog.Logger) *Consumer {
	return newConsumerWithAPI(client, cfg, svc, m, log)
}

func waitSeconds(cfg platform.Config) int32 {
	if cfg.SQSWaitSeconds < 0 || cfg.SQSWaitSeconds > 20 {
		return 10
	}
	return int32(cfg.SQSWaitSeconds)
}

func newConsumerWithAPI(client sqsAPI, cfg platform.Config, svc *app.Service, m *platform.Metrics, log *slog.Logger) *Consumer {
	workers := cfg.ConsumerWorkers
	if workers < 1 {
		workers = 4
	}
	return &Consumer{
		client:          client,
		cfg:             cfg,
		svc:             svc,
		metrics:         m,
		log:             log.With("component", "wager-consumer"),
		waitTimeSeconds: waitSeconds(cfg),
		sem:             make(chan struct{}, workers),
		inFlight:        make(map[string]types.Message),
	}
}

// SetWaitTimeSeconds overrides the long-poll wait time (default 10s).
func (c *Consumer) SetWaitTimeSeconds(s int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.waitTimeSeconds = s
}

// Start launches background polling.
func (c *Consumer) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	workCtx, workCancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.workCancel = workCancel
	c.done = make(chan struct{})
	go c.pollLoop(ctx, workCtx)
}

// Stop terminates polling and waits for in-flight messages within ctx deadline.
// If the shutdown deadline expires, remaining messages have their visibility reset to 0.
func (c *Consumer) Stop(ctx context.Context) error {
	c.mu.Lock()
	cancel, workCancel, done := c.cancel, c.workCancel, c.done
	c.mu.Unlock()
	if cancel == nil {
		return nil
	}
	defer workCancel()

	cancel()

	select {
	case <-done:
	case <-ctx.Done():
	}

	waitDone := make(chan struct{})
	go func() {
		c.inFlightWg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		c.log.Info("consumer stopped cleanly")
		return nil
	case <-ctx.Done():
		c.log.Warn("consumer shutdown timed out, releasing remaining message visibility")
		workCancel()
		c.releaseAllInFlightVisibility()
		return ctx.Err()
	}
}

func (c *Consumer) pollLoop(ctx, workCtx context.Context) {
	defer close(c.done)
	c.log.Info("consumer polling started", "queue", c.cfg.WagerQueueURL, "workers", c.cfg.ConsumerWorkers)

	for ctx.Err() == nil {
		out, err := c.client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.cfg.WagerQueueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     c.waitTimeSeconds,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameApproximateReceiveCount,
			},
		})
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			c.log.Error("error receiving sqs messages", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		if len(out.Messages) > 0 {
			c.log.Info("received messages from sqs", "count", len(out.Messages))
		}

		for _, group := range groupByMessageGroup(out.Messages) {
			if ctx.Err() != nil {
				c.releaseGroup(group)
				continue
			}

			select {
			case c.sem <- struct{}{}:
			case <-ctx.Done():
				c.releaseGroup(group)
				continue
			}

			for _, msg := range group {
				c.trackInFlight(aws.ToString(msg.ReceiptHandle), msg)
			}
			c.inFlightWg.Add(1)

			go func(group []types.Message) {
				defer func() {
					<-c.sem
					c.inFlightWg.Done()
				}()
				c.processGroup(workCtx, group)
			}(group)
		}
	}
}

func (c *Consumer) releaseGroup(group []types.Message) {
	for _, msg := range group {
		c.releaseMessageVisibility(msg)
	}
}

// processGroup handles messages of one FIFO message group strictly in order. After a failure the remaining
// messages are released so SQS redelivers them behind the failed one.
func (c *Consumer) processGroup(ctx context.Context, group []types.Message) {
	for i, msg := range group {
		ok := c.handleMessage(ctx, msg)
		c.untrackInFlight(aws.ToString(msg.ReceiptHandle))
		if ok {
			continue
		}
		for _, rest := range group[i+1:] {
			c.releaseMessageVisibility(rest)
			c.untrackInFlight(aws.ToString(rest.ReceiptHandle))
		}
		return
	}
}

func groupByMessageGroup(msgs []types.Message) [][]types.Message {
	index := make(map[string]int, len(msgs))
	var groups [][]types.Message
	for _, m := range msgs {
		key := m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)]
		if key == "" {
			groups = append(groups, []types.Message{m})
			continue
		}
		i, ok := index[key]
		if !ok {
			i = len(groups)
			index[key] = i
			groups = append(groups, nil)
		}
		groups[i] = append(groups[i], m)
	}
	return groups
}

func (c *Consumer) trackInFlight(handle string, msg types.Message) {
	c.inFlightMu.Lock()
	defer c.inFlightMu.Unlock()
	c.inFlight[handle] = msg
}

func (c *Consumer) untrackInFlight(handle string) {
	c.inFlightMu.Lock()
	defer c.inFlightMu.Unlock()
	delete(c.inFlight, handle)
}

func (c *Consumer) releaseMessageVisibility(msg types.Message) {
	handle := aws.ToString(msg.ReceiptHandle)
	if handle == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = c.client.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(c.cfg.WagerQueueURL),
		ReceiptHandle:     aws.String(handle),
		VisibilityTimeout: 0,
	})
}

func (c *Consumer) releaseAllInFlightVisibility() {
	c.inFlightMu.Lock()
	messages := make([]types.Message, 0, len(c.inFlight))
	for _, m := range c.inFlight {
		messages = append(messages, m)
	}
	c.inFlightMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, msg := range messages {
		handle := aws.ToString(msg.ReceiptHandle)
		if handle == "" {
			continue
		}
		_, _ = c.client.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
			QueueUrl:          aws.String(c.cfg.WagerQueueURL),
			ReceiptHandle:     aws.String(handle),
			VisibilityTimeout: 0,
		})
	}
}

// handleMessage reports whether the message was committed and deleted.
func (c *Consumer) handleMessage(ctx context.Context, msg types.Message) bool {
	start := time.Now()
	defer func() {
		if c.metrics != nil {
			c.metrics.ObserveProcessing("sqs", time.Since(start))
		}
	}()

	receiptHandle := aws.ToString(msg.ReceiptHandle)
	receiveCount := 1
	if countStr, ok := msg.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)]; ok {
		if n, err := strconv.Atoi(countStr); err == nil && n > 0 {
			receiveCount = n
		}
	}

	rawBody := aws.ToString(msg.Body)

	var envelope WagerRequestEnvelope
	decoder := json.NewDecoder(bytes.NewReader([]byte(rawBody)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		c.handleError(ctx, msg, err, "", "", "", "", receiveCount)
		return false
	}

	if envelope.Type != "WagerTransactionRequested" {
		err := fmt.Errorf("%w: expected WagerTransactionRequested, got %q", ErrWrongMessageType, envelope.Type)
		c.handleError(ctx, msg, err, envelope.MessageID, envelope.Data.WalletID, envelope.Data.ProviderID, envelope.Data.ExternalTransactionID, receiveCount)
		return false
	}

	if envelope.MessageID == "" || envelope.Data.IdempotencyKey == "" {
		err := fmt.Errorf("%w: messageId and idempotencyKey are required", domain.ErrInvalidInput)
		c.handleError(ctx, msg, err, envelope.MessageID, envelope.Data.WalletID, envelope.Data.ProviderID, envelope.Data.ExternalTransactionID, receiveCount)
		return false
	}

	playerID, err := domain.ParseID(envelope.Data.PlayerID)
	if err != nil {
		c.handleError(ctx, msg, err, envelope.MessageID, envelope.Data.WalletID, envelope.Data.ProviderID, envelope.Data.ExternalTransactionID, receiveCount)
		return false
	}

	walletID, err := domain.ParseID(envelope.Data.WalletID)
	if err != nil {
		c.handleError(ctx, msg, err, envelope.MessageID, envelope.Data.WalletID, envelope.Data.ProviderID, envelope.Data.ExternalTransactionID, receiveCount)
		return false
	}

	kind, err := domain.ParseExternalKind(envelope.Data.Kind)
	if err != nil {
		c.handleError(ctx, msg, err, envelope.MessageID, envelope.Data.WalletID, envelope.Data.ProviderID, envelope.Data.ExternalTransactionID, receiveCount)
		return false
	}

	op := domain.ExternalOperation{
		ProviderID:                     envelope.Data.ProviderID,
		ExternalTransactionID:          envelope.Data.ExternalTransactionID,
		PlayerID:                       playerID,
		WalletID:                       walletID,
		RoundID:                        envelope.Data.RoundID,
		GameID:                         envelope.Data.GameID,
		Kind:                           kind,
		Money:                          envelope.Data.Money,
		ReferenceExternalTransactionID: envelope.Data.ReferenceExternalTransactionID,
	}

	if err := op.Validate(); err != nil {
		c.handleError(ctx, msg, err, envelope.MessageID, string(walletID), envelope.Data.ProviderID, envelope.Data.ExternalTransactionID, receiveCount)
		return false
	}

	hash := sha256.Sum256([]byte(rawBody))
	sha256Hex := hex.EncodeToString(hash[:])

	input := app.SubmitInput{
		Operation:      op,
		IdempotencyKey: envelope.Data.IdempotencyKey,
		CorrelationID:  envelope.MessageID,
	}

	_, err = c.svc.SubmitFromMessage(ctx, "wager-consumer", envelope.MessageID, sha256Hex, input)
	if err != nil {
		c.handleError(ctx, msg, err, envelope.MessageID, string(walletID), envelope.Data.ProviderID, envelope.Data.ExternalTransactionID, receiveCount)
		return false
	}

	delCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, delErr := c.client.DeleteMessage(delCtx, &awssqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.cfg.WagerQueueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	if delErr != nil {
		c.log.Error("failed to delete sqs message after commit",
			"messageId", envelope.MessageID,
			"walletId", string(walletID),
			"providerId", envelope.Data.ProviderID,
			"transactionId", envelope.Data.ExternalTransactionID,
			"receiveCount", receiveCount,
			"error", delErr,
		)
		return false
	}

	if c.metrics != nil {
		c.metrics.SQSMessage("processed")
	}
	c.log.Info("sqs message processed successfully",
		"messageId", envelope.MessageID,
		"walletId", string(walletID),
		"providerId", envelope.Data.ProviderID,
		"transactionId", envelope.Data.ExternalTransactionID,
		"receiveCount", receiveCount,
	)
	return true
}

func (c *Consumer) handleError(ctx context.Context, msg types.Message, err error, messageID, walletID, providerID, txID string, receiveCount int) {
	receiptHandle := aws.ToString(msg.ReceiptHandle)

	if isInvalidOrPoison(err) {
		if c.metrics != nil {
			c.metrics.SQSMessage("invalid")
		}
		c.log.Warn("invalid or poison sqs message",
			"messageId", messageID,
			"walletId", walletID,
			"providerId", providerID,
			"transactionId", txID,
			"receiveCount", receiveCount,
			"error", err,
		)
		visCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = c.client.ChangeMessageVisibility(visCtx, &awssqs.ChangeMessageVisibilityInput{
			QueueUrl:          aws.String(c.cfg.WagerQueueURL),
			ReceiptHandle:     aws.String(receiptHandle),
			VisibilityTimeout: 0,
		})
		return
	}

	if c.metrics != nil {
		c.metrics.SQSMessage("retried")
	}
	backoff := calculateBackoff(receiveCount)
	c.log.Error("transient error processing sqs message, retrying with backoff",
		"messageId", messageID,
		"walletId", walletID,
		"providerId", providerID,
		"transactionId", txID,
		"receiveCount", receiveCount,
		"backoff", backoff,
		"error", err,
	)
	visCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = c.client.ChangeMessageVisibility(visCtx, &awssqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(c.cfg.WagerQueueURL),
		ReceiptHandle:     aws.String(receiptHandle),
		VisibilityTimeout: int32(backoff.Seconds()),
	})
}

func calculateBackoff(receiveCount int) time.Duration {
	if receiveCount < 1 {
		receiveCount = 1
	}
	d := 2 * time.Second
	for i := 1; i < receiveCount; i++ {
		d *= 2
		if d >= 5*time.Minute {
			return 5 * time.Minute
		}
	}
	if d > 5*time.Minute {
		return 5 * time.Minute
	}
	return d
}

func isInvalidOrPoison(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, domain.ErrInvalidInput) ||
		errors.Is(err, domain.ErrOpeningNotAllowed) ||
		errors.Is(err, domain.ErrMoneyOverflow) ||
		errors.Is(err, domain.ErrInvalidMoney) ||
		errors.Is(err, app.ErrIdempotencyConflict) ||
		errors.Is(err, app.ErrDuplicateExternal) ||
		errors.Is(err, app.ErrPoisonMessage) ||
		errors.Is(err, ErrWrongMessageType) ||
		errors.Is(err, ErrMalformedMessage) {
		return true
	}

	var syntaxErr *json.SyntaxError
	var unmarshalTypeErr *json.UnmarshalTypeError
	if errors.As(err, &syntaxErr) || errors.As(err, &unmarshalTypeErr) {
		return true
	}
	if strings.Contains(err.Error(), "unknown field") {
		return true
	}
	return false
}
