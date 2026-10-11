package platform

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/patrickzets/Testejungle/internal/domain"
)

type LagSource interface {
	Lag(ctx context.Context, now time.Time) (time.Duration, error)
}

// Metrics implements app.Observer and exposes the Prometheus registry.
type Metrics struct {
	Registry *prometheus.Registry

	transactions *prometheus.CounterVec
	duplicates   prometheus.Counter
	conflicts    prometheus.Counter
	retries      prometheus.Counter
	divergences  prometheus.Counter
	sqsMessages  *prometheus.CounterVec
	httpRequests *prometheus.CounterVec
	httpLatency  *prometheus.HistogramVec
	processing   *prometheus.HistogramVec
	published    *prometheus.CounterVec
}

func NewMetrics() *Metrics {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		transactions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wagering_transactions_total", Help: "Transaction outcomes by kind and status."}, []string{"kind", "status"}),
		duplicates: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wagering_duplicate_submissions_total", Help: "Submissions answered from a persisted result."}),
		conflicts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wagering_idempotency_conflicts_total", Help: "Idempotency keys reused with a different payload."}),
		retries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wagering_concurrency_retries_total", Help: "SQL transactions retried after serialization failures or deadlocks."}),
		divergences: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wagering_reconciliation_divergences_total", Help: "Reconciliations that found a balance mismatch."}),
		sqsMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wagering_sqs_messages_total", Help: "Inbound SQS messages by result (processed, retried, dead_lettered, invalid)."}, []string{"result"}),
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wagering_http_requests_total", Help: "HTTP requests."}, []string{"method", "route", "status"}),
		httpLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "wagering_http_request_duration_seconds", Help: "HTTP latency.", Buckets: prometheus.DefBuckets}, []string{"route"}),
		processing: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "wagering_processing_duration_seconds", Help: "Message processing latency.", Buckets: prometheus.DefBuckets}, []string{"channel"}),
		published: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wagering_outbox_published_total", Help: "Outbox publication attempts by result."}, []string{"result"}),
	}
	m.Registry.MustRegister(m.transactions, m.duplicates, m.conflicts, m.retries, m.divergences,
		m.sqsMessages, m.httpRequests, m.httpLatency, m.processing, m.published)
	return m
}

func (m *Metrics) RegisterOutboxLag(src LagSource) {
	m.Registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "wagering_outbox_lag_seconds", Help: "Age of the oldest unpublished outbox event."},
		func() float64 {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			lag, err := src.Lag(ctx, time.Now().UTC())
			if err != nil {
				return -1
			}
			return lag.Seconds()
		}))
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

func (m *Metrics) TransactionOutcome(kind domain.Kind, status domain.Status, replay bool) {
	m.transactions.WithLabelValues(string(kind), string(status)).Inc()
	if replay {
		m.duplicates.Inc()
	}
}

func (m *Metrics) IdempotencyConflict()      { m.conflicts.Inc() }
func (m *Metrics) ConcurrencyRetry()         { m.retries.Inc() }
func (m *Metrics) ReconciliationDivergence() { m.divergences.Inc() }

func (m *Metrics) SQSMessage(result string)      { m.sqsMessages.WithLabelValues(result).Inc() }
func (m *Metrics) OutboxPublished(result string) { m.published.WithLabelValues(result).Inc() }
func (m *Metrics) ObserveProcessing(channel string, d time.Duration) {
	m.processing.WithLabelValues(channel).Observe(d.Seconds())
}

func (m *Metrics) ObserveHTTP(method, route string, status int, d time.Duration) {
	m.httpRequests.WithLabelValues(method, route, strconv.Itoa(status)).Inc()
	m.httpLatency.WithLabelValues(route).Observe(d.Seconds())
}
