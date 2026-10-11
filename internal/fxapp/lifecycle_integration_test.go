//go:build integration

package fxapp_test

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/patrickzets/Testejungle/internal/fxapp"
	"github.com/patrickzets/Testejungle/internal/platform"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func TestStartServeAndGracefulStop(t *testing.T) {
	addr := freeAddr(t)
	t.Setenv("HTTP_ADDR", addr)
	t.Setenv("INSTANCE_ID", "lifecycle-test")
	cfg, err := platform.LoadConfig()
	if err != nil {
		t.Skipf("integration environment not configured: %v", err)
	}

	app := fxapp.New(cfg)
	startCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := app.Start(startCtx); err != nil {
		t.Fatalf("start: %v", err)
	}

	resp, err := http.Get("http://" + addr + "/health/ready")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("ready: %v %v", resp, err)
	}
	resp.Body.Close()

	stopCtx, stopCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer stopCancel()
	if err := app.Stop(stopCtx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if _, err := http.Get("http://" + addr + "/health/live"); err == nil {
		t.Fatal("server still accepting connections after stop")
	}
}

func TestStartFailsFastWhenDependencyIsDown(t *testing.T) {
	t.Setenv("HTTP_ADDR", freeAddr(t))
	cfg, err := platform.LoadConfig()
	if err != nil {
		t.Skipf("integration environment not configured: %v", err)
	}
	cfg.WagerQueueURL = "http://127.0.0.1:1/000000000000/missing.fifo"
	cfg.EventsQueueURL = cfg.WagerQueueURL

	app := fxapp.New(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := app.Start(ctx); err == nil {
		_ = app.Stop(ctx)
		t.Fatal("expected start to fail when SQS is unreachable")
	}
}
