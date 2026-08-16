// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/runatlantis/atlantis/server/core/drift"
	driftpostgres "github.com/runatlantis/atlantis/server/core/drift/postgres"
	runspostgres "github.com/runatlantis/atlantis/server/core/runs/postgres"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/stretchr/testify/require"
)

// TestStorageConformance exercises restart durability and the existing
// drift.Storage semantics against a real PostgreSQL server. It is opt-in so
// unit-test environments do not require a database.
func TestStorageConformance(t *testing.T) {
	if testing.Short() {
		t.Skip("PostgreSQL conformance test is disabled in short mode")
	}
	testURL := os.Getenv("ATLANTIS_POSTGRES_TEST_URL")
	if testURL == "" {
		t.Skip("ATLANTIS_POSTGRES_TEST_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	storeURL, admin, cleanup := newIsolatedDatabase(t, ctx, testURL)
	defer cleanup()
	runStore, err := runspostgres.New(ctx, runspostgres.Config{URL: storeURL, OperationTimeout: 5 * time.Second})
	require.NoError(t, err)
	storage := driftpostgres.New(runStore.Database(), 5*time.Second)

	checkedAt := time.Now().UTC().Truncate(time.Microsecond)
	first := models.ProjectDrift{
		ProjectName: "network", Path: "terraform/network", Workspace: "production",
		Ref: "main", BaseBranch: "main", ResolvedCommit: "deadbeef", DetectionID: "detection-1",
		Drift: models.DriftSummary{
			HasDrift: true, ToAdd: 1, ToChange: 2, ToDestroy: 3, ToImport: 4, ToForget: 5,
			Summary: "drift detected", ChangesOutside: true,
		},
		PlanOutput: "must never persist", LastChecked: checkedAt,
	}
	partial := models.ProjectDrift{
		ProjectName: "database", Path: "terraform/database", Workspace: "production",
		Ref: "main", BaseBranch: "main", ResolvedCommit: "deadbeef", DetectionID: "detection-1",
		LastChecked: checkedAt.Add(time.Second), Error: "provider unavailable",
	}
	require.NoError(t, storage.Store("example/infrastructure", first))
	require.NoError(t, storage.Store("example/infrastructure", partial))
	require.NoError(t, storage.Store("another/repository", models.ProjectDrift{
		ProjectName: "other", Ref: "main", LastChecked: checkedAt,
	}))

	results, err := storage.Get("example/infrastructure", drift.GetOptions{Ref: "main", BaseBranch: "main"})
	require.NoError(t, err)
	require.Len(t, results, 2, "partial project outcomes must survive independently")
	byName := indexByProject(results)
	require.Equal(t, "", byName["network"].PlanOutput)
	require.Equal(t, first.Drift, byName["network"].Drift)
	require.Equal(t, partial.Error, byName["database"].Error)

	updated := first
	updated.Drift = models.DriftSummary{HasDrift: false, Summary: "no changes"}
	updated.LastChecked = checkedAt.Add(2 * time.Second)
	require.NoError(t, storage.Store("example/infrastructure", updated))
	results, err = storage.Get("example/infrastructure", drift.GetOptions{ProjectName: "network"})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.False(t, results[0].Drift.HasDrift, "same identity must overwrite latest state")

	all, err := storage.GetAll()
	require.NoError(t, err)
	require.Len(t, all["example/infrastructure"], 2)
	require.Len(t, all["another/repository"], 1)

	// Closing and reopening the owning RunStore simulates an Atlantis restart.
	require.NoError(t, runStore.Close())
	runStore, err = runspostgres.New(ctx, runspostgres.Config{URL: storeURL, OperationTimeout: 5 * time.Second})
	require.NoError(t, err)
	storage = driftpostgres.New(runStore.Database(), 5*time.Second)
	results, err = storage.Get("example/infrastructure", drift.GetOptions{})
	require.NoError(t, err)
	require.Len(t, results, 2)

	require.NoError(t, storage.DeleteMatching("example/infrastructure", drift.GetOptions{
		ProjectName: "network", Path: "terraform/network", Workspace: "production",
		Ref: "main", BaseBranch: "main", Exact: true,
	}))
	require.NoError(t, storage.Delete("example/infrastructure", "database"))
	results, err = storage.Get("example/infrastructure", drift.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, results)
	require.NoError(t, runStore.Close())
	require.NoError(t, admin.PingContext(ctx))
}

func indexByProject(results []models.ProjectDrift) map[string]models.ProjectDrift {
	indexed := make(map[string]models.ProjectDrift, len(results))
	for _, result := range results {
		indexed[result.ProjectName] = result
	}
	return indexed
}

func newIsolatedDatabase(t *testing.T, ctx context.Context, rawURL string) (string, *sql.DB, func()) {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	require.NoError(t, err)
	require.NotEmpty(t, parsed.Scheme, "ATLANTIS_POSTGRES_TEST_URL must be a PostgreSQL URL")
	schema := fmt.Sprintf("atlantis_drift_test_%d", time.Now().UnixNano())

	admin, err := sql.Open("pgx", rawURL)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(ctx))
	_, err = admin.ExecContext(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)

	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	cleanup := func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := admin.ExecContext(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE")
		require.NoError(t, err)
		require.NoError(t, admin.Close())
	}
	return parsed.String(), admin, cleanup
}
