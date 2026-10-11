package main

import (
	"log/slog"
	"os"

	"github.com/patrickkaua/wagering/internal/fxapp"
	"github.com/patrickkaua/wagering/internal/platform"
)

func main() {
	cfg, err := platform.LoadConfig()
	if err != nil {
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	fxapp.New(cfg).Run()
}
