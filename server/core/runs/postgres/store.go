// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

// Package postgres implements durable run history in PostgreSQL.
package postgres

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/runatlantis/atlantis/server/core/runs"
)

const (
	defaultOperationTimeout = 10 * time.Second
	migrationLockKey        = int64(0x41544c52554e53)
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// Config configures a PostgreSQL run-history Store.
type Config struct {
	URL              string
	MaxOpenConns     int
	MaxIdleConns     int
	ConnMaxLifetime  time.Duration
	OperationTimeout time.Duration
}

// Store implements runs.Store using database/sql. PostgreSQL driver types do
// not cross the runs.Store seam.
type Store struct {
	db               *sql.DB
	operationTimeout time.Duration
}

// New connects to PostgreSQL, validates connectivity, and applies embedded
// migrations before returning a Store.
func New(ctx context.Context, cfg Config) (*Store, error) {
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, fmt.Errorf("PostgreSQL URL is required")
	}
	db, err := sql.Open("pgx", cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("opening PostgreSQL run store: %w", err)
	}
	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}

	store := newStore(db, cfg.OperationTimeout)
	opCtx, cancel := store.operationContext(ctx)
	defer cancel()
	if err := db.PingContext(opCtx); err != nil {
		db.Close() // nolint: errcheck
		return nil, fmt.Errorf("connecting to PostgreSQL run store: %w", err)
	}
	if err := store.applyMigrations(opCtx); err != nil {
		db.Close() // nolint: errcheck
		return nil, err
	}
	return store, nil
}

func newStore(db *sql.DB, operationTimeout time.Duration) *Store {
	if operationTimeout <= 0 {
		operationTimeout = defaultOperationTimeout
	}
	return &Store{db: db, operationTimeout: operationTimeout}
}

// Ping verifies current PostgreSQL connectivity.
func (s *Store) Ping(ctx context.Context) error {
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()
	return s.db.PingContext(opCtx)
}

// Close releases the PostgreSQL connection pool.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) operationContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, s.operationTimeout)
}

func (s *Store) applyMigrations(ctx context.Context) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquiring PostgreSQL migration connection: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", migrationLockKey); err != nil {
		return fmt.Errorf("locking PostgreSQL run store migrations: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), s.operationTimeout)
		defer cancel()
		conn.ExecContext(unlockCtx, "SELECT pg_advisory_unlock($1)", migrationLockKey) // nolint: errcheck
	}()

	if _, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS atlantis_run_store_migrations (
        version bigint PRIMARY KEY,
        name text NOT NULL,
        applied_at timestamptz NOT NULL DEFAULT now()
    )`); err != nil {
		return fmt.Errorf("creating PostgreSQL run store migration table: %w", err)
	}

	migrations, err := loadMigrations()
	if err != nil {
		return err
	}
	for _, migration := range migrations {
		var applied bool
		if err := conn.QueryRowContext(ctx,
			"SELECT EXISTS (SELECT 1 FROM atlantis_run_store_migrations WHERE version = $1)",
			migration.version,
		).Scan(&applied); err != nil {
			return fmt.Errorf("checking PostgreSQL run store migration %d: %w", migration.version, err)
		}
		if applied {
			continue
		}

		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("starting PostgreSQL run store migration %d: %w", migration.version, err)
		}
		if _, err := tx.ExecContext(ctx, migration.sql); err != nil {
			tx.Rollback() // nolint: errcheck
			return fmt.Errorf("applying PostgreSQL run store migration %d (%s): %w", migration.version, migration.name, err)
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO atlantis_run_store_migrations (version, name) VALUES ($1, $2)",
			migration.version, migration.name,
		); err != nil {
			tx.Rollback() // nolint: errcheck
			return fmt.Errorf("recording PostgreSQL run store migration %d: %w", migration.version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("committing PostgreSQL run store migration %d: %w", migration.version, err)
		}
	}
	return nil
}

type migration struct {
	version int64
	name    string
	sql     string
}

func loadMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return nil, fmt.Errorf("reading embedded PostgreSQL run store migrations: %w", err)
	}
	migrations := make([]migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		prefix, _, ok := strings.Cut(entry.Name(), "_")
		if !ok {
			return nil, fmt.Errorf("migration filename %q has no numeric prefix", entry.Name())
		}
		version, err := strconv.ParseInt(prefix, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parsing migration filename %q: %w", entry.Name(), err)
		}
		contents, err := migrationFiles.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return nil, fmt.Errorf("reading migration %q: %w", entry.Name(), err)
		}
		migrations = append(migrations, migration{version: version, name: entry.Name(), sql: string(contents)})
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].version < migrations[j].version })
	for i := 1; i < len(migrations); i++ {
		if migrations[i-1].version == migrations[i].version {
			return nil, fmt.Errorf("duplicate migration version %d", migrations[i].version)
		}
	}
	if len(migrations) == 0 {
		return nil, errors.New("no embedded PostgreSQL run store migrations")
	}
	return migrations, nil
}

func normalizeTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}

func normalizeTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	normalized := normalizeTime(*value)
	return &normalized
}

var _ runs.Store = (*Store)(nil)
