package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/patrickzets/Testejungle/internal/app"
	"github.com/patrickzets/Testejungle/internal/auth"
	"github.com/patrickzets/Testejungle/internal/platform"
)

// HealthCheck is a named readiness probe.
type HealthCheck struct {
	Name  string
	Check func(ctx context.Context) error
}

type Server struct {
	svc      *app.Service
	verifier *auth.Verifier
	metrics  *platform.Metrics
	checks   []HealthCheck
	log      *slog.Logger
}

func NewServer(svc *app.Service, verifier *auth.Verifier, metrics *platform.Metrics, checks []HealthCheck, log *slog.Logger) *Server {
	return &Server{svc: svc, verifier: verifier, metrics: metrics, checks: checks, log: log}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", s.live)
	mux.HandleFunc("GET /health/ready", s.ready)
	mux.Handle("GET /metrics", s.metrics.Handler())

	mux.Handle("POST /wallets", s.protected(s.createWallet))
	mux.Handle("GET /wallets/{walletId}", s.protected(s.getWallet))
	mux.Handle("GET /wallets/{walletId}/ledger", s.protected(s.listLedger))
	mux.Handle("POST /wallets/{walletId}/reconciliation", s.protected(s.reconcile))

	mux.Handle("POST /wagering/transactions", s.protected(s.submitTransaction))
	mux.Handle("GET /wagering/transactions/{transactionId}", s.protected(s.getTransaction))
	mux.Handle("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", s.protected(s.getProviderTransaction))

	return s.recoverer(s.observe(mux))
}

type principalHandler func(w http.ResponseWriter, r *http.Request, p auth.Principal)

func (s *Server) protected(h principalHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := auth.BearerToken(r.Header.Get("Authorization"))
		if !ok {
			w.Header().Set("WWW-Authenticate", `Bearer realm="wagering"`)
			writeProblem(w, http.StatusUnauthorized, "UNAUTHENTICATED", "missing or malformed bearer token")
			return
		}
		p, err := s.verifier.Verify(r.Context(), token)
		if errors.Is(err, auth.ErrAuthUnavailable) {
			w.Header().Set("Retry-After", "1")
			writeProblem(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "identity provider unavailable, retry the same request")
			return
		}
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="wagering", error="invalid_token"`)
			writeProblem(w, http.StatusUnauthorized, "UNAUTHENTICATED", "invalid or expired token")
			return
		}
		if info, ok := r.Context().Value(requestInfoKey{}).(*requestInfo); ok {
			info.providerID = p.ProviderID
		}
		h(w, r.WithContext(auth.WithPrincipal(r.Context(), p)), p)
	})
}

type correlationKey struct{}

type requestInfoKey struct{}

// requestInfo is filled by inner middleware so the outer logger can report the caller.
type requestInfo struct{ providerID string }

func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(correlationKey{}).(string)
	return id
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		id := r.Header.Get("X-Correlation-Id")
		if id == "" || len(id) > 128 {
			id = newCorrelationID()
		}
		w.Header().Set("X-Correlation-Id", id)
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		info := &requestInfo{}
		ctx := context.WithValue(r.Context(), correlationKey{}, id)
		req := r.WithContext(context.WithValue(ctx, requestInfoKey{}, info))
		next.ServeHTTP(rec, req)

		route := req.Pattern
		if route == "" {
			route = "unmatched"
		}
		elapsed := time.Since(start)
		s.metrics.ObserveHTTP(r.Method, route, rec.status, elapsed)
		attrs := []any{"correlationId", id, "method", r.Method, "route", route, "status", rec.status, "durationMs", elapsed.Milliseconds()}

		if info.providerID != "" {
			attrs = append(attrs, "providerId", info.providerID)
		}
		s.log.Info("http request", attrs...)
	})
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.log.Error("panic in handler", "panic", rec, "stack", string(debug.Stack()))
				writeProblem(w, http.StatusInternalServerError, "INTERNAL_ERROR", "unexpected error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func newCorrelationID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func (s *Server) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "up"})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	results := make(map[string]string, len(s.checks))
	status := http.StatusOK
	for _, c := range s.checks {
		if err := c.Check(ctx); err != nil {
			results[c.Name] = "down"
			status = http.StatusServiceUnavailable
			s.log.Warn("readiness check failed", "check", c.Name, "error", err)
			continue
		}
		results[c.Name] = "up"
	}
	body := map[string]any{"status": "ready", "checks": results}
	if status != http.StatusOK {
		body["status"] = "not_ready"
	}
	writeJSON(w, status, body)
}
