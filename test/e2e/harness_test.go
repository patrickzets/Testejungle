//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jackc/pgx/v5/pgxpool"

	internalsqs "github.com/patrickzets/Testejungle/internal/adapters/sqs"
	"github.com/patrickzets/Testejungle/internal/domain"
	internalplatform "github.com/patrickzets/Testejungle/internal/platform"
)

const (
	defaultDatabaseURL = "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable"
	defaultAWSEndpoint = "http://localhost:4566"
	defaultWagerQueue  = "http://localhost:4566/000000000000/wager-transactions.fifo"
	defaultDLQQueue    = "http://localhost:4566/000000000000/wager-transactions-dlq.fifo"
	defaultEventsQueue = "http://localhost:4566/000000000000/wager-events.fifo"
	defaultOIDCIssuer  = "http://localhost:8080/realms/wagering"
	defaultOIDCAud     = "wagering-api"
)

var (
	binaryPath string
	globalPool *pgxpool.Pool
	sqsClient  *sqs.Client
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. Compile wagering binary
	tempDir, err := os.MkdirTemp("", "wagering-e2e-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tempDir)

	binName := "wagering"
	if os.PathSeparator == '\\' {
		binName += ".exe"
	}
	binaryPath = filepath.Join(tempDir, binName)

	buildCmd := exec.Command("go", "build", "-o", binaryPath, "github.com/patrickzets/Testejungle/cmd/wagering")
	buildCmd.Stdout = os.Stdout
	buildCmd.Stderr = os.Stderr
	if err := buildCmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to build wagering binary: %v\n", err)
		os.Exit(1)
	}

	// 2. Connect to DB
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = defaultDatabaseURL
	}
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to db: %v\n", err)
		os.Exit(1)
	}
	globalPool = pool
	defer pool.Close()

	// 3. Connect to SQS
	cfg := internalplatform.Config{
		AWSEndpointURL: defaultAWSEndpoint,
		AWSRegion:      "us-east-1",
	}
	client, err := internalsqs.NewClient(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to init sqs client: %v\n", err)
		os.Exit(1)
	}
	sqsClient = client

	code := m.Run()
	os.Exit(code)
}

type Instance struct {
	ID   string
	Port int
	Cmd  *exec.Cmd
	URL  string
}

type Cluster struct {
	mu          sync.RWMutex
	instances   []*Instance
	rrIndex     uint64
	envMap      map[string]string
	WagerQueue  string
	DLQQueue    string
	EventsQueue string
	LogDir      string
	ClusterSeq  uint64
}

var globalClusterSeq uint64

func normalizeQueueURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	u.Scheme = "http"
	u.Host = "localhost:4566"
	return u.String()
}

func createClusterQueues(t *testing.T, clusterSeq uint64) (mainQueueURL, dlqURL string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	uniqueID := domain.NewID().String()[:8]
	dlqName := fmt.Sprintf("wager-dlq-%d-%s.fifo", clusterSeq, uniqueID)
	dlqOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(dlqName),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "false",
		},
	})
	if err != nil {
		t.Fatalf("failed to create cluster DLQ %s: %v", dlqName, err)
	}
	dlqURL = normalizeQueueURL(aws.ToString(dlqOut.QueueUrl))

	arnOut, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(dlqURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("failed to get cluster DLQ ARN: %v", err)
	}
	dlqARN := arnOut.Attributes[string(types.QueueAttributeNameQueueArn)]

	redrivePolicy := fmt.Sprintf(`{"deadLetterTargetArn": "%s", "maxReceiveCount": "5"}`, dlqARN)
	mainName := fmt.Sprintf("wager-tx-%d-%s.fifo", clusterSeq, uniqueID)
	mainOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(mainName),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "false",
			"DeduplicationScope":        "messageGroup",
			"FifoThroughputLimit":       "perMessageGroupId",
			"VisibilityTimeout":         "30",
			"RedrivePolicy":             redrivePolicy,
		},
	})
	if err != nil {
		t.Fatalf("failed to create cluster main queue %s: %v", mainName, err)
	}
	mainQueueURL = normalizeQueueURL(aws.ToString(mainOut.QueueUrl))
	return mainQueueURL, dlqURL
}

func CreateUniqueEventsQueue(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	uniqueID := domain.NewID().String()[:8]
	evQueueName := fmt.Sprintf("wager-events-%s.fifo", uniqueID)
	evOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(evQueueName),
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "false",
		},
	})
	if err != nil {
		t.Fatalf("failed to create events queue %s: %v", evQueueName, err)
	}
	evURL := normalizeQueueURL(aws.ToString(evOut.QueueUrl))
	t.Cleanup(func() {
		delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = sqsClient.DeleteQueue(delCtx, &sqs.DeleteQueueInput{QueueUrl: aws.String(evURL)})
	})
	return evURL
}

func NewCluster(t *testing.T, count int, envOverrides map[string]string) *Cluster {
	t.Helper()

	clusterSeq := atomic.AddUint64(&globalClusterSeq, 1)

	// Create dedicated temp log directory for this cluster run
	logDir, err := os.MkdirTemp("", fmt.Sprintf("wagering-e2e-%d-*", clusterSeq))
	if err != nil {
		t.Fatalf("failed to create cluster log dir: %v", err)
	}

	mainQueue, dlqQueue := createClusterQueues(t, clusterSeq)
	eventsQueue := defaultEventsQueue
	if eq, ok := envOverrides["EVENTS_QUEUE_URL"]; ok && eq != "" {
		eventsQueue = eq
	}

	c := &Cluster{
		instances:   make([]*Instance, 0, count),
		envMap:      envOverrides,
		WagerQueue:  mainQueue,
		DLQQueue:    dlqQueue,
		EventsQueue: eventsQueue,
		LogDir:      logDir,
		ClusterSeq:  clusterSeq,
	}

	basePort := 8100 + int(clusterSeq*10)
	for i := 1; i <= count; i++ {
		port := basePort + i
		inst := c.startInstance(t, fmt.Sprintf("inst-%d-%d", clusterSeq, i), port, envOverrides)
		c.instances = append(c.instances, inst)
	}

	t.Cleanup(func() {
		c.StopAll()

		// Delete dedicated queues
		delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if c.WagerQueue != "" {
			_, _ = sqsClient.DeleteQueue(delCtx, &sqs.DeleteQueueInput{QueueUrl: aws.String(c.WagerQueue)})
		}
		if c.DLQQueue != "" {
			_, _ = sqsClient.DeleteQueue(delCtx, &sqs.DeleteQueueInput{QueueUrl: aws.String(c.DLQQueue)})
		}

		if t.Failed() {
			t.Logf("[DIAGNOSTIC] Scenario failed. Cluster %d instance logs kept at: %s", c.ClusterSeq, c.LogDir)
			entries, _ := os.ReadDir(c.LogDir)
			for _, e := range entries {
				t.Logf("  -> Log: %s", filepath.Join(c.LogDir, e.Name()))
			}
		} else {
			_ = os.RemoveAll(c.LogDir)
		}
	})

	return c
}

func (c *Cluster) startInstance(t *testing.T, id string, port int, envOverrides map[string]string) *Instance {
	t.Helper()
	httpAddr := fmt.Sprintf(":%d", port)
	instURL := fmt.Sprintf("http://localhost:%d", port)

	cmd := exec.Command(binaryPath)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("HTTP_ADDR=%s", httpAddr),
		fmt.Sprintf("DATABASE_URL=%s", defaultDatabaseURL),
		fmt.Sprintf("AWS_ENDPOINT_URL=%s", defaultAWSEndpoint),
		"AWS_REGION=us-east-1",
		"AWS_ACCESS_KEY_ID=test",
		"AWS_SECRET_ACCESS_KEY=test",
		fmt.Sprintf("WAGER_QUEUE_URL=%s", c.WagerQueue),
		fmt.Sprintf("WAGER_DLQ_URL=%s", c.DLQQueue),
		fmt.Sprintf("EVENTS_QUEUE_URL=%s", c.EventsQueue),
		fmt.Sprintf("OIDC_ISSUER_URL=%s", defaultOIDCIssuer),
		fmt.Sprintf("OIDC_AUDIENCE=%s", defaultOIDCAud),
		fmt.Sprintf("INSTANCE_ID=%s", id),
		"WORKER_INTERVAL=50ms",
		"CONSUMER_WORKERS=4",
		"SQS_WAIT_SECONDS=1",
		"SHUTDOWN_TIMEOUT=5s",
	)

	for k, v := range envOverrides {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

	logFilePath := filepath.Join(c.LogDir, fmt.Sprintf("%s.log", id))
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err == nil {
		cmd.Stdout = logFile
		cmd.Stderr = logFile
	}

	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start instance %s on port %d: %v", id, port, err)
	}

	inst := &Instance{
		ID:   id,
		Port: port,
		Cmd:  cmd,
		URL:  instURL,
	}

	// Wait for /health/ready
	if err := waitForReady(instURL, 15*time.Second); err != nil {
		_ = cmd.Process.Kill()
		t.Fatalf("instance %s failed to become ready: %v", id, err)
	}

	return inst
}

func waitForReady(instURL string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(instURL + "/health/ready")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s to become ready", instURL)
}

func (inst *Instance) Stop() {
	if inst.Cmd != nil && inst.Cmd.Process != nil {
		_ = inst.Cmd.Process.Signal(os.Interrupt)
		_ = inst.Cmd.Wait()
	}
}

func (inst *Instance) Kill() {
	if inst.Cmd != nil && inst.Cmd.Process != nil {
		_ = inst.Cmd.Process.Kill()
		_ = inst.Cmd.Wait()
	}
}

func (c *Cluster) StopAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, inst := range c.instances {
		if inst.Cmd != nil && inst.Cmd.Process != nil {
			_ = inst.Cmd.Process.Kill()
			_ = inst.Cmd.Wait()
		}
	}
	c.instances = nil
}

func (c *Cluster) Stop(idx int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if idx < len(c.instances) && c.instances[idx] != nil {
		c.instances[idx].Stop()
		c.instances = append(c.instances[:idx], c.instances[idx+1:]...)
	}
}

func (c *Cluster) Kill(idx int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if idx < len(c.instances) && c.instances[idx] != nil {
		c.instances[idx].Kill()
		c.instances = append(c.instances[:idx], c.instances[idx+1:]...)
	}
}

func (c *Cluster) RestartAll(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()

	ports := make([]int, len(c.instances))
	ids := make([]string, len(c.instances))
	for i, inst := range c.instances {
		ports[i] = inst.Port
		ids[i] = inst.ID
		_ = inst.Cmd.Process.Kill()
		_ = inst.Cmd.Wait()
	}
	c.instances = nil

	for i := range ports {
		inst := c.startInstance(t, ids[i], ports[i], c.envMap)
		c.instances = append(c.instances, inst)
	}
}

func (c *Cluster) NextURL() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.instances) == 0 {
		return ""
	}
	idx := atomic.AddUint64(&c.rrIndex, 1) % uint64(len(c.instances))
	return c.instances[idx].URL
}

func (c *Cluster) AllURLs() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	urls := make([]string, len(c.instances))
	for i, inst := range c.instances {
		urls[i] = inst.URL
	}
	return urls
}

// Token helper
var tokenCache sync.Map

func GetToken(t *testing.T, clientID, clientSecret string) string {
	t.Helper()
	key := clientID + ":" + clientSecret
	if tok, ok := tokenCache.Load(key); ok {
		return tok.(string)
	}

	tokenURL := defaultOIDCIssuer + "/protocol/openid-connect/token"
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {clientID},
		"client_secret": {clientSecret},
	}

	resp, err := http.PostForm(tokenURL, form)
	if err != nil {
		t.Fatalf("failed to fetch token: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("token endpoint returned status %d: %s", resp.StatusCode, string(body))
	}

	var res struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		t.Fatalf("failed to decode token response: %v", err)
	}

	tokenCache.Store(key, res.AccessToken)
	return res.AccessToken
}

func TokenProviderA(t *testing.T) string {
	return GetToken(t, "provider-a", "provider-a-secret")
}

func TokenProviderB(t *testing.T) string {
	return GetToken(t, "provider-b", "provider-b-secret")
}

func TokenWalletInternal(t *testing.T) string {
	return GetToken(t, "wallet-internal", "wallet-internal-secret")
}

// HTTP client helpers
type Client struct {
	cluster *Cluster
	http    *http.Client
}

func NewClient(c *Cluster) *Client {
	return &Client{
		cluster: c,
		http: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

type WalletResponse struct {
	ID       domain.ID    `json:"id"`
	PlayerID domain.ID    `json:"playerId"`
	Balance  domain.Money `json:"balance"`
	Version  int64        `json:"version"`
}

func (cl *Client) CreateWallet(t *testing.T, playerID domain.ID, amount, currency string) WalletResponse {
	t.Helper()
	money, err := domain.ParseMoney(amount, currency)
	if err != nil {
		t.Fatalf("parse money: %v", err)
	}

	reqBody, _ := json.Marshal(map[string]any{
		"playerId":       playerID.String(),
		"initialBalance": money,
	})

	req, _ := http.NewRequest("POST", cl.cluster.NextURL()+"/wallets", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+TokenWalletInternal(t))

	resp, err := cl.http.Do(req)
	if err != nil {
		t.Fatalf("create wallet request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("create wallet failed with status %d: %s", resp.StatusCode, string(body))
	}

	var w WalletResponse
	if err := json.NewDecoder(resp.Body).Decode(&w); err != nil {
		t.Fatalf("decode wallet response: %v", err)
	}
	return w
}

func (cl *Client) GetWallet(t *testing.T, walletID domain.ID) (WalletResponse, int, error) {
	t.Helper()
	req, _ := http.NewRequest("GET", cl.cluster.NextURL()+"/wallets/"+walletID.String(), nil)
	req.Header.Set("Authorization", "Bearer "+TokenWalletInternal(t))

	resp, err := cl.http.Do(req)
	if err != nil {
		return WalletResponse{}, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return WalletResponse{}, resp.StatusCode, nil
	}

	var w WalletResponse
	if err := json.NewDecoder(resp.Body).Decode(&w); err != nil {
		return WalletResponse{}, resp.StatusCode, err
	}
	return w, resp.StatusCode, nil
}

type SubmitHTTPResponse struct {
	TransactionID    domain.ID          `json:"transactionId"`
	Status           domain.Status      `json:"status"`
	Balance          *domain.Money      `json:"balance,omitempty"`
	FailureCode      domain.FailureCode `json:"failureCode,omitempty"`
	IdempotentReplay bool               `json:"idempotentReplay"`
}

func (cl *Client) SubmitTransaction(t *testing.T, payload map[string]any, idempotencyKey, token string) (SubmitHTTPResponse, int, string, error) {
	reqBody, _ := json.Marshal(payload)
	req, _ := http.NewRequest("POST", cl.cluster.NextURL()+"/wagering/transactions", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := cl.http.Do(req)
	if err != nil {
		return SubmitHTTPResponse{}, 0, "", err
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	var sub SubmitHTTPResponse
	_ = json.Unmarshal(bodyBytes, &sub)
	return sub, resp.StatusCode, string(bodyBytes), nil
}

func (cl *Client) GetProviderTransaction(t *testing.T, providerID, extID, token string) (domain.Status, domain.FailureCode, int, error) {
	req, _ := http.NewRequest("GET", cl.cluster.NextURL()+"/providers/"+providerID+"/wagering/transactions/"+extID, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := cl.http.Do(req)
	if err != nil {
		return "", "", 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", "", resp.StatusCode, nil
	}
	var tx struct {
		Status      domain.Status      `json:"status"`
		FailureCode domain.FailureCode `json:"failureCode"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tx); err != nil {
		return "", "", resp.StatusCode, err
	}
	return tx.Status, tx.FailureCode, resp.StatusCode, nil
}

type ReconcileResponse struct {
	WalletID          domain.ID    `json:"walletId"`
	StoredBalance     domain.Money `json:"storedBalance"`
	CalculatedBalance domain.Money `json:"calculatedBalance"`
	Difference        domain.Money `json:"difference"`
	Consistent        bool         `json:"consistent"`
	CheckedEntries    int          `json:"checkedEntries"`
}

func (cl *Client) Reconcile(t *testing.T, walletID domain.ID) ReconcileResponse {
	t.Helper()
	req, _ := http.NewRequest("POST", cl.cluster.NextURL()+"/wallets/"+walletID.String()+"/reconciliation", nil)
	req.Header.Set("Authorization", "Bearer "+TokenWalletInternal(t))

	resp, err := cl.http.Do(req)
	if err != nil {
		t.Fatalf("reconcile request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("reconcile failed status %d: %s", resp.StatusCode, string(body))
	}

	var rec ReconcileResponse
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		t.Fatalf("decode reconcile: %v", err)
	}
	return rec
}

// SQS Helpers
func (c *Cluster) SendSQS(ctx context.Context, envelope any, walletID, messageID string) error {
	raw, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(c.WagerQueue),
		MessageBody:            aws.String(string(raw)),
		MessageGroupId:         aws.String(walletID),
		MessageDeduplicationId: aws.String(messageID),
	})
	return err
}

func (cl *Client) SendSQS(ctx context.Context, envelope any, walletID, messageID string) error {
	return cl.cluster.SendSQS(ctx, envelope, walletID, messageID)
}

func (c *Cluster) ReceiveEvents(ctx context.Context, maxMessages int32, waitTime time.Duration) ([]types.Message, error) {
	out, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(c.EventsQueue),
		MaxNumberOfMessages: maxMessages,
		WaitTimeSeconds:     int32(waitTime.Seconds()),
	})
	if err != nil {
		return nil, err
	}
	return out.Messages, nil
}

func (cl *Client) ReceiveEvents(ctx context.Context, maxMessages int32, waitTime time.Duration) ([]types.Message, error) {
	return cl.cluster.ReceiveEvents(ctx, maxMessages, waitTime)
}

func (c *Cluster) CheckDLQ(ctx context.Context) (int, error) {
	out, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(c.DLQQueue),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
		},
	})
	if err != nil {
		return 0, err
	}
	var count int
	fmt.Sscanf(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)], "%d", &count)
	return count, nil
}

func (cl *Client) CheckDLQ(ctx context.Context) (int, error) {
	return cl.cluster.CheckDLQ(ctx)
}

func (c *Cluster) WaitForQueueDrained(ctx context.Context, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(c.WagerQueue),
			AttributeNames: []types.QueueAttributeName{
				types.QueueAttributeNameApproximateNumberOfMessages,
				types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			},
		})
		if err == nil {
			var vis, notVis int
			fmt.Sscanf(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)], "%d", &vis)
			fmt.Sscanf(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)], "%d", &notVis)
			if vis == 0 && notVis == 0 {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func SendSQSMessage(ctx context.Context, envelope any, walletID, messageID string) error {
	raw, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(defaultWagerQueue),
		MessageBody:            aws.String(string(raw)),
		MessageGroupId:         aws.String(walletID),
		MessageDeduplicationId: aws.String(messageID),
	})
	return err
}

func ReceiveEvents(ctx context.Context, maxMessages int32, waitTime time.Duration) ([]types.Message, error) {
	out, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(defaultEventsQueue),
		MaxNumberOfMessages: maxMessages,
		WaitTimeSeconds:     int32(waitTime.Seconds()),
	})
	if err != nil {
		return nil, err
	}
	return out.Messages, nil
}

func CheckDLQEmpty(ctx context.Context) (int, error) {
	out, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(defaultDLQQueue),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
		},
	})
	if err != nil {
		return 0, err
	}
	var count int
	fmt.Sscanf(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)], "%d", &count)
	return count, nil
}

func WaitForQueueDrained(ctx context.Context, queueURL string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl: aws.String(queueURL),
			AttributeNames: []types.QueueAttributeName{
				types.QueueAttributeNameApproximateNumberOfMessages,
				types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			},
		})
		if err == nil {
			var vis, notVis int
			fmt.Sscanf(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)], "%d", &vis)
			fmt.Sscanf(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)], "%d", &notVis)
			if vis == 0 && notVis == 0 {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// DB Assertions
func AssertDBInvariants(t *testing.T, cl *Client, walletID domain.ID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Check via reconciliation endpoint
	rec := cl.Reconcile(t, walletID)
	if !rec.Consistent {
		t.Fatalf("reconciliation reported divergence: stored=%s calc=%s diff=%s",
			rec.StoredBalance.Amount(), rec.CalculatedBalance.Amount(), rec.Difference.Amount())
	}
	if rec.StoredBalance.IsNegative() {
		t.Fatalf("stored balance is negative: %s", rec.StoredBalance.Amount())
	}

	// 2. Direct SQL snapshot query check
	const q = `
		SELECT w.balance_minor,
		       COALESCE(SUM(CASE WHEN l.direction = 'CREDIT' THEN l.amount_minor WHEN l.direction = 'DEBIT' THEN -l.amount_minor ELSE 0 END), 0) as calc_minor
		FROM wallets w
		LEFT JOIN wallet_ledger_entries l ON w.id = l.wallet_id
		WHERE w.id = $1
		GROUP BY w.id, w.balance_minor
	`
	var storedMinor, calcMinor int64
	err := globalPool.QueryRow(ctx, q, walletID.String()).Scan(&storedMinor, &calcMinor)
	if err != nil {
		t.Fatalf("direct DB invariant query failed: %v", err)
	}
	if storedMinor != calcMinor {
		t.Fatalf("direct DB balance divergence: stored=%d calculated=%d", storedMinor, calcMinor)
	}
	if storedMinor < 0 {
		t.Fatalf("direct DB balance is negative: %d", storedMinor)
	}
}
