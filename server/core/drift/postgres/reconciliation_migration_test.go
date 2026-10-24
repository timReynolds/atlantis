// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	runspostgres "github.com/runatlantis/atlantis/server/core/runs/postgres"
	"github.com/stretchr/testify/require"
)

// The drift fix reserves migration 9 so existing later stack migrations retain
// their identities. A database upgraded from the drift-only branch must still
// apply migrations 4–8 when it subsequently upgrades to the full stack.
func TestDriftReconciliationMigrationUpgrade(t *testing.T) {
	testURL := os.Getenv("ATLANTIS_POSTGRES_TEST_URL")
	if testing.Short() || testURL == "" {
		t.Skip("PostgreSQL conformance test requires ATLANTIS_POSTGRES_TEST_URL and non-short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	storeURL, _, cleanup := newIsolatedDatabase(t, ctx, testURL)
	defer cleanup()
	db, err := sql.Open("pgx", storeURL)
	require.NoError(t, err)
	defer db.Close()
	_, err = db.ExecContext(ctx, `CREATE TABLE atlantis_run_store_migrations (
version bigint PRIMARY KEY, name text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`)
	require.NoError(t, err)
	files, err := filepath.Glob("../../runs/postgres/migrations/*.sql")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, file := range files {
		name := filepath.Base(file)
		prefix, _, _ := strings.Cut(name, "_")
		version, err := strconv.Atoi(prefix)
		require.NoError(t, err)
		if version > 3 && version != 9 {
			continue
		}
		source, err := os.ReadFile(file)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, string(source))
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "INSERT INTO atlantis_run_store_migrations(version, name) VALUES ($1, $2)", version, name)
		require.NoError(t, err)
	}
	var count int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM atlantis_run_store_migrations").Scan(&count))
	require.Equal(t, 4, count)
	store, err := runspostgres.New(ctx, runspostgres.Config{URL: storeURL})
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, db.QueryRowContext(ctx, "SELECT COUNT(*) FROM atlantis_run_store_migrations").Scan(&count))
	require.Equal(t, len(files), count, "every unapplied migration must run even when a higher version was applied first")
}
