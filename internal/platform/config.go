package platform

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr        string
	DatabaseURL     string
	AWSEndpointURL  string
	AWSRegion       string
	WagerQueueURL   string
	EventsQueueURL  string
	OIDCIssuerURL   string
	OIDCJWKSURL     string
	OIDCAudience    string
	InstanceID      string
	ShutdownTimeout time.Duration
	WorkerInterval  time.Duration
	ConsumerWorkers int
	MaxReceiveCount int
	SQSWaitSeconds  int

	ReferenceMaxAttempts int
	ReferenceTTL         time.Duration
	ReferenceBaseDelay   time.Duration
	ReferenceMaxDelay    time.Duration
}

func LoadConfig() (Config, error) {
	cfg := Config{
		HTTPAddr:        env("HTTP_ADDR", ":8080"),
		DatabaseURL:     os.Getenv("DATABASE_URL"),
		AWSEndpointURL:  os.Getenv("AWS_ENDPOINT_URL"),
		AWSRegion:       env("AWS_REGION", "us-east-1"),
		WagerQueueURL:   os.Getenv("WAGER_QUEUE_URL"),
		EventsQueueURL:  os.Getenv("EVENTS_QUEUE_URL"),
		OIDCIssuerURL:   os.Getenv("OIDC_ISSUER_URL"),
		OIDCJWKSURL:     os.Getenv("OIDC_JWKS_URL"),
		OIDCAudience:    env("OIDC_AUDIENCE", "wagering-api"),
		InstanceID:      env("INSTANCE_ID", hostname()),
		ShutdownTimeout: duration("SHUTDOWN_TIMEOUT", 25*time.Second),
		WorkerInterval:  duration("WORKER_INTERVAL", time.Second),
		ConsumerWorkers: integer("CONSUMER_WORKERS", 4),
		MaxReceiveCount: integer("MAX_RECEIVE_COUNT", 5),
		SQSWaitSeconds:  integer("SQS_WAIT_SECONDS", 10),

		ReferenceMaxAttempts: integer("REFERENCE_MAX_ATTEMPTS", 8),
		ReferenceTTL:         duration("REFERENCE_TTL", 10*time.Minute),
		ReferenceBaseDelay:   duration("REFERENCE_BASE_DELAY", 2*time.Second),
		ReferenceMaxDelay:    duration("REFERENCE_MAX_DELAY", 5*time.Minute),
	}
	return cfg, cfg.Validate()
}

func (c Config) Validate() error {
	var errs []error
	for name, v := range map[string]string{
		"DATABASE_URL": c.DatabaseURL, "WAGER_QUEUE_URL": c.WagerQueueURL,
		"EVENTS_QUEUE_URL": c.EventsQueueURL, "OIDC_ISSUER_URL": c.OIDCIssuerURL,
	} {
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", name))
		}
	}
	if c.ConsumerWorkers < 1 {
		errs = append(errs, errors.New("CONSUMER_WORKERS must be at least 1"))
	}
	return errors.Join(errs...)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func duration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func integer(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "instance"
	}
	return h
}
