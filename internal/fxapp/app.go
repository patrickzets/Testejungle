package fxapp

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/patrickkaua/wagering/internal/adapters/httpapi"
	"github.com/patrickkaua/wagering/internal/adapters/postgres"
	sqsadapter "github.com/patrickkaua/wagering/internal/adapters/sqs"
	"github.com/patrickkaua/wagering/internal/app"
	"github.com/patrickkaua/wagering/internal/auth"
	"github.com/patrickkaua/wagering/internal/platform"
)

// New composes the application. Options let tests replace or add components.
func New(cfg platform.Config, opts ...fx.Option) *fx.App {
	return fx.New(append(Options(cfg), opts...)...)
}

// Options returns the full composition so it can be validated without opening connections.
func Options(cfg platform.Config) []fx.Option {
	return []fx.Option{
		fx.Supply(cfg),
		fx.StartTimeout(60 * time.Second),
		fx.StopTimeout(cfg.ShutdownTimeout),
		fx.WithLogger(func(log *slog.Logger) fxevent.Logger {
			l := &fxevent.SlogLogger{Logger: log}
			l.UseLogLevel(slog.LevelDebug)
			return l
		}),
		InfraModule,
		DomainModule,
		TransportModule,
		fx.Invoke(registerLifecycle),
	}
}

var InfraModule = fx.Module("infra",
	fx.Provide(
		newLogger,
		platform.NewMetrics,
		newPool,
		newStore,
		newAWSClient,
	),
)

var DomainModule = fx.Module("domain",
	fx.Provide(
		newService,
		newOutboxRelay,
	),
)

var TransportModule = fx.Module("transport",
	fx.Provide(
		newVerifier,
		newHealthChecks,
		httpapi.NewServer,
		newHTTPServer,
		newPublisher,
		sqsadapter.NewConsumer,
	),
)

func newLogger(cfg platform.Config) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	return slog.New(h).With("instance", cfg.InstanceID)
}

func newPool(lc fx.Lifecycle, cfg platform.Config) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error {
		pool.Close()
		return nil
	}})
	return pool, nil
}

func newStore(pool *pgxpool.Pool, metrics *platform.Metrics) *postgres.Store {
	return postgres.NewStore(pool, metrics)
}

func newAWSClient(cfg platform.Config) (*sqs.Client, error) {
	return sqsadapter.NewClient(context.Background(), cfg)
}

func newPublisher(client *sqs.Client, cfg platform.Config) *sqsadapter.Publisher {
	return sqsadapter.NewPublisher(client, cfg.EventsQueueURL)
}

func newService(store *postgres.Store, metrics *platform.Metrics, cfg platform.Config) *app.Service {
	metrics.RegisterOutboxLag(store.Outbox())
	return app.NewService(app.Deps{
		Tx: store.Tx(), Wallets: store.Wallets(), Transactions: store.Transactions(), Ledger: store.Ledger(),
		Inbox: store.Inbox(), Outbox: store.Outbox(), Observer: metrics,
		Reference: app.ReferencePolicy{
			MaxAttempts: cfg.ReferenceMaxAttempts, TTL: cfg.ReferenceTTL,
			BaseDelay: cfg.ReferenceBaseDelay, MaxDelay: cfg.ReferenceMaxDelay,
		},
	})
}

func newOutboxRelay(store *postgres.Store, pub *sqsadapter.Publisher, cfg platform.Config, metrics *platform.Metrics) *app.OutboxRelay {
	return app.NewOutboxRelay(store.Outbox(), pub, nil, app.RelayConfig{
		Owner: cfg.InstanceID,
		OnPublish: func(ok bool) {
			if ok {
				metrics.OutboxPublished("success")
				return
			}
			metrics.OutboxPublished("error")
		},
	})
}

func newVerifier(cfg platform.Config) *auth.Verifier {
	return auth.NewVerifier(context.Background(), cfg.OIDCIssuerURL, cfg.OIDCJWKSURL, cfg.OIDCAudience)
}

func newHealthChecks(store *postgres.Store, client *sqs.Client, cfg platform.Config) []httpapi.HealthCheck {
	return []httpapi.HealthCheck{
		{Name: "postgres", Check: store.Ping},
		{Name: "sqs", Check: sqsadapter.QueueHealth(client, cfg.WagerQueueURL, cfg.EventsQueueURL)},
	}
}

func newHTTPServer(cfg platform.Config, srv *httpapi.Server) *http.Server {
	return &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

type lifecycleParams struct {
	fx.In

	LC       fx.Lifecycle
	Cfg      platform.Config
	Log      *slog.Logger
	Metrics  *platform.Metrics
	Svc      *app.Service
	Relay    *app.OutboxRelay
	Consumer *sqsadapter.Consumer
	HTTP     *http.Server
	Checks   []httpapi.HealthCheck
}

// registerLifecycle starts dependencies' consumers last and stops them first: HTTP and the SQS consumer stop
// accepting work, then the background workers drain, and the connection pool (registered earlier) closes last.
func registerLifecycle(p lifecycleParams) {
	resolver := platform.NewWorker("reference-resolver", p.Cfg.WorkerInterval, p.Log, p.Svc.ResolveNextPending)
	relay := platform.NewWorker("outbox-relay", p.Cfg.WorkerInterval/2, p.Log, p.Relay.RunOnce)
	var listener net.Listener

	p.LC.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			for _, c := range p.Checks {
				if err := c.Check(ctx); err != nil {
					return errors.Join(errors.New("dependency check failed: "+c.Name), err)
				}
			}
			var err error
			listener, err = net.Listen("tcp", p.HTTP.Addr)
			return err
		},
	})
	p.LC.Append(fx.Hook{
		OnStart: func(context.Context) error {
			resolver.Start()
			relay.Start()
			p.Consumer.Start()
			go func() {
				if err := p.HTTP.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
					p.Log.Error("http server stopped", "error", err)
				}
			}()
			p.Log.Info("service started", "addr", p.HTTP.Addr)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			p.Log.Info("shutting down")
			err := p.HTTP.Shutdown(ctx)
			err = errors.Join(err, p.Consumer.Stop(ctx), relay.Stop(ctx), resolver.Stop(ctx))
			return err
		},
	})
}
