// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/core/drift"
	driftpostgres "github.com/runatlantis/atlantis/server/core/drift/postgres"
	runspostgres "github.com/runatlantis/atlantis/server/core/runs/postgres"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/stretchr/testify/require"
)

const reconciliationRepository = "github.com/example/infrastructure"

func TestFullDetectionRetainsNewLatestStatus(t *testing.T) {
	ctx, store, _ := newReconciliationStore(t)
	history := driftpostgres.NewHistoryStore(store.Database(), 5*time.Second)
	latest := driftpostgres.New(store.Database(), 5*time.Second)
	startedAt := time.Now().UTC().Truncate(time.Microsecond)
	record := reconciliationRecord(t, startedAt, "retained")

	// An absent project from the same generation must be removed, while a
	// newer targeted result and other repository/ref/base scopes survive.
	seed := reconciliationRecord(t, startedAt, "removed", "newer", "other-ref", "other-base", "other-repo")
	seed.Projects[1].Project.LastChecked = startedAt.Add(time.Hour)
	seed.Projects[2].Project.Ref = "release"
	seed.Projects[3].Project.BaseBranch = "release"
	for i, project := range seed.Projects {
		repository := reconciliationRepository
		if i == 4 {
			repository = "github.com/other/repository"
		}
		require.NoError(t, latest.Store(repository, project.Project))
	}
	require.NoError(t, history.RecordDetectionWithLatest(ctx, reconciliationRepository, record, true))
	_, historical, err := history.GetDetection(ctx, record.Run.ID, drift.HistoryPageRequest{})
	require.NoError(t, err)
	require.Len(t, historical.Projects, 1)
	current, err := latest.Get(reconciliationRepository, drift.GetOptions{})
	require.NoError(t, err)
	indexed := indexByProject(current)
	require.Len(t, indexed, 4)
	require.Equal(t, record.Run.ID, indexed["retained"].DetectionID)
	require.Equal(t, startedAt, indexed["retained"].LastChecked)
	require.Contains(t, indexed, "newer")
	require.Contains(t, indexed, "other-ref")
	require.Contains(t, indexed, "other-base")
	other, err := latest.Get("github.com/other/repository", drift.GetOptions{})
	require.NoError(t, err)
	require.Len(t, other, 1)
}

func TestOlderDetectionCannotResurrectReconciledProject(t *testing.T) {
	ctx, store, storeURL := newReconciliationStore(t)
	history := driftpostgres.NewHistoryStore(store.Database(), 5*time.Second)
	latest := driftpostgres.New(store.Database(), 5*time.Second)
	now := time.Now().UTC().Truncate(time.Microsecond)
	old := reconciliationRecord(t, now.Add(-time.Hour), "removed", "previously-unseen")
	require.NoError(t, latest.Store(reconciliationRepository, old.Projects[0].Project))
	// A successful empty full scan removes every project in its scope.
	require.NoError(t, history.RecordDetectionWithLatest(ctx, reconciliationRepository, reconciliationRecord(t, now), true))

	// A separate pool also sees the durable generation fence. Both an older
	// targeted detection and an older full detection must still record history.
	replica, err := runspostgres.New(ctx, runspostgres.Config{URL: storeURL})
	require.NoError(t, err)
	defer replica.Close()
	history = driftpostgres.NewHistoryStore(replica.Database(), 5*time.Second)
	for _, full := range []bool{false, true} {
		stale := reconciliationRecord(t, old.Run.StartedAt, "removed", "previously-unseen")
		require.NoError(t, history.RecordDetectionWithLatest(ctx, reconciliationRepository, stale, full))
		_, historical, err := history.GetDetection(ctx, stale.Run.ID, drift.HistoryPageRequest{})
		require.NoError(t, err)
		require.Len(t, historical.Projects, 2)
	}
	// Direct latest-status writers must obey the same fence.
	require.NoError(t, latest.Store(reconciliationRepository, old.Projects[1].Project))
	current, err := latest.Get(reconciliationRepository, drift.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, current)

	// New detections can reintroduce projects, and older full detections cannot
	// remove these newer results or move the reconciliation watermark backward.
	fresh := reconciliationRecord(t, now.Add(time.Hour), "fresh")
	require.NoError(t, history.RecordDetectionWithLatest(ctx, reconciliationRepository, fresh, false))
	require.NoError(t, history.RecordDetectionWithLatest(ctx, reconciliationRepository, reconciliationRecord(t, now.Add(-time.Minute)), true))
	require.NoError(t, latest.Store(reconciliationRepository, old.Projects[0].Project))
	current, err = latest.Get(reconciliationRepository, drift.GetOptions{})
	require.NoError(t, err)
	require.Len(t, current, 1)
	require.Equal(t, "fresh", current[0].ProjectName)
}

func TestIncompleteDetectionDoesNotReconcile(t *testing.T) {
	for _, status := range []drift.DetectionStatus{drift.DetectionStatusPartial, drift.DetectionStatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			ctx, store, _ := newReconciliationStore(t)
			history := driftpostgres.NewHistoryStore(store.Database(), 5*time.Second)
			latest := driftpostgres.New(store.Database(), 5*time.Second)
			now := time.Now().UTC().Truncate(time.Microsecond)
			initial := reconciliationRecord(t, now.Add(-time.Hour), "retained", "failed")
			require.NoError(t, history.RecordDetectionWithLatest(ctx, reconciliationRepository, initial, true))
			failed := reconciliationRecord(t, now, "failed")
			failed.Run.Status = status
			failed.Run.FailedProjects = 1
			failed.Projects[0].Project.Error = "planning error"
			failed.Projects[0].Outcome = drift.DetectionOutcomeFailed
			require.NoError(t, history.RecordDetectionWithLatest(ctx, reconciliationRepository, failed, true))
			current, err := latest.Get(reconciliationRepository, drift.GetOptions{})
			require.NoError(t, err)
			indexed := indexByProject(current)
			require.Len(t, indexed, 2)
			require.Equal(t, &initial.Run.StartedAt, indexed["failed"].LastSuccessfulChecked)
			// The incomplete run must not advance the full-scan generation fence.
			older := reconciliationRecord(t, now.Add(-time.Minute), "late")
			require.NoError(t, history.RecordDetectionWithLatest(ctx, reconciliationRepository, older, false))
			current, err = latest.Get(reconciliationRepository, drift.GetOptions{})
			require.NoError(t, err)
			require.Len(t, current, 3)
		})
	}
}

func TestReconciliationRollsBackWithHistory(t *testing.T) {
	ctx, store, _ := newReconciliationStore(t)
	history := driftpostgres.NewHistoryStore(store.Database(), 5*time.Second)
	latest := driftpostgres.New(store.Database(), 5*time.Second)
	now := time.Now().UTC().Truncate(time.Microsecond)
	initial := reconciliationRecord(t, now.Add(-time.Hour), "retained")
	require.NoError(t, history.RecordDetectionWithLatest(ctx, reconciliationRepository, initial, true))
	duplicate := reconciliationRecord(t, now)
	duplicate.Run.ID = initial.Run.ID
	require.Error(t, history.RecordDetectionWithLatest(ctx, reconciliationRepository, duplicate, true))
	older := reconciliationRecord(t, now.Add(-time.Minute), "late")
	require.NoError(t, history.RecordDetectionWithLatest(ctx, reconciliationRepository, older, false))
	current, err := latest.Get(reconciliationRepository, drift.GetOptions{})
	require.NoError(t, err)
	require.Len(t, current, 2, "failed history transaction must not remove status or advance the fence")
}

func TestReconciliationFencesConcurrentStaleInsert(t *testing.T) {
	ctx, store, storeURL := newReconciliationStore(t)
	history := driftpostgres.NewHistoryStore(store.Database(), 10*time.Second)
	latest := driftpostgres.New(store.Database(), 10*time.Second)
	now := time.Now().UTC().Truncate(time.Microsecond)
	stale := reconciliationRecord(t, now.Add(-time.Hour), "removed", "previously-unseen")
	require.NoError(t, latest.Store(reconciliationRepository, stale.Projects[0].Project))
	replica, err := runspostgres.New(ctx, runspostgres.Config{URL: storeURL})
	require.NoError(t, err)
	defer replica.Close()
	otherHistory := driftpostgres.NewHistoryStore(replica.Database(), 10*time.Second)

	// Hold the full scan after its DELETE but before commit. The older writer
	// must wait for that transaction and then observe its generation floor.
	gate, err := store.Database().BeginTx(ctx, nil)
	require.NoError(t, err)
	defer gate.Rollback()
	var gatePID int
	require.NoError(t, gate.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&gatePID))
	_, err = gate.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1)", gatePID)
	require.NoError(t, err)
	_, err = store.Database().ExecContext(ctx, fmt.Sprintf(`
CREATE FUNCTION block_drift_delete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(%d);
    RETURN OLD;
END $$;
CREATE TRIGGER block_drift_delete AFTER DELETE ON drift_status
FOR EACH ROW EXECUTE FUNCTION block_drift_delete()`, gatePID))
	require.NoError(t, err)
	fullDone := make(chan error, 1)
	full := reconciliationRecord(t, now)
	go func() {
		fullDone <- history.RecordDetectionWithLatest(ctx, reconciliationRepository, full, true)
	}()
	fullPID := waitForBlockedBackend(t, ctx, store.Database(), gatePID)
	staleDone := make(chan error, 1)
	go func() {
		staleDone <- otherHistory.RecordDetectionWithLatest(ctx, reconciliationRepository, stale, false)
	}()
	waitForBlockedBackend(t, ctx, store.Database(), fullPID)
	require.NoError(t, gate.Commit())
	require.NoError(t, <-fullDone)
	require.NoError(t, <-staleDone)
	current, err := latest.Get(reconciliationRepository, drift.GetOptions{})
	require.NoError(t, err)
	require.Empty(t, current)
	_, historical, err := history.GetDetection(ctx, stale.Run.ID, drift.HistoryPageRequest{})
	require.NoError(t, err)
	require.Len(t, historical.Projects, 2)
}

func waitForBlockedBackend(t *testing.T, ctx context.Context, db *sql.DB, blockingPID int) int {
	t.Helper()
	var blockedPID int
	require.Eventually(t, func() bool {
		return db.QueryRowContext(ctx, `SELECT pid FROM pg_stat_activity
WHERE $1 = ANY(pg_blocking_pids(pid)) LIMIT 1`, blockingPID).Scan(&blockedPID) == nil
	}, 5*time.Second, 10*time.Millisecond, "expected PostgreSQL transaction to block")
	return blockedPID
}

func newReconciliationStore(t *testing.T) (context.Context, *runspostgres.Store, string) {
	t.Helper()
	testURL := os.Getenv("ATLANTIS_POSTGRES_TEST_URL")
	if testing.Short() || testURL == "" {
		t.Skip("PostgreSQL conformance test requires ATLANTIS_POSTGRES_TEST_URL and non-short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	storeURL, _, cleanup := newIsolatedDatabase(t, ctx, testURL)
	t.Cleanup(cleanup)
	store, err := runspostgres.New(ctx, runspostgres.Config{URL: storeURL})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return ctx, store, storeURL
}

func reconciliationRecord(t *testing.T, startedAt time.Time, names ...string) drift.DetectionRecord {
	t.Helper()
	id := string(mustHistoryID(t))
	record := drift.DetectionRecord{Run: drift.DetectionRun{
		ID: id, Repository: reconciliationRepository, DisplayRepository: "example/infrastructure",
		Ref: "main", BaseBranch: "main", ResolvedCommit: "commit-" + id,
		Status: drift.DetectionStatusSucceeded, StartedAt: startedAt,
		CompletedAt: startedAt.Add(time.Minute), TotalProjects: len(names), ProjectsWithDrift: len(names),
	}}
	for i, name := range names {
		record.Projects = append(record.Projects, drift.DetectionProject{DetectionID: id, Ordinal: i,
			Project: models.ProjectDrift{
				ProjectName: name, Path: "terraform/" + name, Workspace: "production",
				Ref: "main", BaseBranch: "main", ResolvedCommit: record.Run.ResolvedCommit, DetectionID: id,
				Drift: models.DriftSummary{HasDrift: true, ToAdd: 1},
				// Match DetectDrift: LastChecked is the detection generation start.
				LastChecked: startedAt,
			}, Outcome: drift.DetectionOutcomeDrifted})
	}
	return record
}
