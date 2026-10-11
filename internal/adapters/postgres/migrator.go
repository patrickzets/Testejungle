package postgres

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

// Migrator manages applying and rolling back database migrations.
type Migrator struct {
	sourceURL   string
	databaseURL string
}

// NewMigrator initializes a Migrator pointing to a directory of SQL migrations and a database URL.
func NewMigrator(migrationsDir, databaseURL string) (*Migrator, error) {
	absDir, err := filepath.Abs(migrationsDir)
	if err != nil {
		return nil, fmt.Errorf("resolve migrations dir: %w", err)
	}
	absDir = filepath.ToSlash(absDir)
	var sourceURL string
	if filepath.VolumeName(absDir) != "" {
		// Windows volume path: file://C:/path
		sourceURL = "file://" + strings.TrimPrefix(absDir, "/")
	} else {
		// Unix absolute path: file:///path
		if !strings.HasPrefix(absDir, "/") {
			absDir = "/" + absDir
		}
		sourceURL = "file://" + absDir
	}

	return &Migrator{
		sourceURL:   sourceURL,
		databaseURL: databaseURL,
	}, nil
}

// Up applies all pending database migrations.
func (m *Migrator) Up() error {
	mig, err := migrate.New(m.sourceURL, m.databaseURL)
	if err != nil {
		return fmt.Errorf("init migrator: %w", err)
	}
	defer func() {
		_, _ = mig.Close()
	}()

	if err := mig.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate up: %w", err)
	}
	return nil
}

// Down rolls back all applied database migrations.
func (m *Migrator) Down() error {
	mig, err := migrate.New(m.sourceURL, m.databaseURL)
	if err != nil {
		return fmt.Errorf("init migrator: %w", err)
	}
	defer func() {
		_, _ = mig.Close()
	}()

	if err := mig.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate down: %w", err)
	}
	return nil
}

// Steps applies n migration steps (positive up, negative down).
func (m *Migrator) Steps(n int) error {
	mig, err := migrate.New(m.sourceURL, m.databaseURL)
	if err != nil {
		return fmt.Errorf("init migrator: %w", err)
	}
	defer func() {
		_, _ = mig.Close()
	}()

	if err := mig.Steps(n); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate steps %d: %w", n, err)
	}
	return nil
}
