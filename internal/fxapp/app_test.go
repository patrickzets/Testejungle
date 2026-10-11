package fxapp_test

import (
	"testing"
	"time"

	"go.uber.org/fx"

	"github.com/patrickzets/Testejungle/internal/fxapp"
	"github.com/patrickzets/Testejungle/internal/platform"
)

func TestCompositionGraphIsValid(t *testing.T) {
	cfg := platform.Config{
		HTTPAddr: ":0", DatabaseURL: "postgres://x", WagerQueueURL: "q", EventsQueueURL: "e",
		OIDCIssuerURL: "http://issuer", ShutdownTimeout: time.Second, ConsumerWorkers: 1,
	}
	if err := fx.ValidateApp(fxapp.Options(cfg)...); err != nil {
		t.Fatalf("invalid fx graph: %v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	if err := (platform.Config{}).Validate(); err == nil {
		t.Fatal("empty config must be rejected")
	}
}
