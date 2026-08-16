// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/stretchr/testify/require"
)

const (
	testRunID        = runs.ID("0198a0df-85f1-7d83-a60b-2e57b725c62a")
	testProjectRunID = runs.ID("0198a0df-85f1-7d83-a60b-2e57b725c62b")
	testAttemptID    = runs.ID("0198a0df-85f1-7d83-a60b-2e57b725c62c")
	testInstanceID   = runs.ID("0198a0df-85f1-7d83-a60b-2e57b725c62d")
)

var testTime = time.Date(2026, 8, 16, 10, 0, 0, 123456000, time.UTC)

func TestLoadMigrations(t *testing.T) {
	migrations, err := loadMigrations()
	require.NoError(t, err)
	require.NotEmpty(t, migrations)
	require.Equal(t, int64(1), migrations[0].version)
	require.Contains(t, migrations[0].sql, "CREATE TABLE run_output_chunks")
	require.Equal(t, int64(4), migrations[3].version)
	require.Contains(t, migrations[3].sql, "CREATE TABLE run_attempts")
	require.Contains(t, migrations[3].sql, "run_attempts_one_active_concurrency_key_idx")
	require.Contains(t, migrations[3].sql, "ON run_attempts (deployment_id, concurrency_key)")
	require.Contains(t, migrations[3].sql, "status = 'unknown' AND reconciled_at IS NULL")
	require.Equal(t, int64(5), migrations[len(migrations)-1].version)
	require.Contains(t, migrations[len(migrations)-1].sql, "'unknown'")
}

func TestRegisterExecutionInstance(t *testing.T) {
	store, mock := newMockStore(t)
	instance := runs.ExecutionInstance{
		ID: testInstanceID, ReplicaID: "atlantis-0", DeploymentID: "prod-eu",
		AdvertiseURL: "http://atlantis-0.atlantis:4141", StartedAt: testTime,
		HeartbeatAt: testTime, Version: "1.0.0", Commit: "abc123",
		Metadata: runs.Metadata(`{"zone":"eu-west-2a"}`),
	}
	mock.ExpectExec("INSERT INTO execution_instances").
		WithArgs(
			instance.ID, instance.ReplicaID, instance.DeploymentID, instance.AdvertiseURL,
			instance.StartedAt, instance.HeartbeatAt, nil, instance.Version, instance.Commit,
			[]byte(`{"zone":"eu-west-2a"}`),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	require.NoError(t, store.RegisterInstance(context.Background(), instance))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateAttemptMapsActiveConcurrencyConflict(t *testing.T) {
	store, mock := newMockStore(t)
	attempt := claimedAttempt()
	mock.ExpectExec("INSERT INTO run_attempts").
		WithArgs(
			attempt.ID, attempt.RunID, attempt.InstanceID, attempt.DeploymentID, attempt.ConcurrencyKey,
			attempt.OwnershipClaimID, attempt.Status, attempt.ClaimedAt, nil,
			attempt.HeartbeatAt, nil, nil, attempt.FailureReason, nil,
			attempt.ReconciledBy, attempt.ReconciliationSummary, []byte(`{}`),
		).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT .* FROM run_attempts WHERE id = \\$1").
		WithArgs(testAttemptID).WillReturnError(sql.ErrNoRows)

	err := store.CreateAttempt(context.Background(), attempt)
	require.ErrorIs(t, err, runs.ErrConflict)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCompleteAttemptUnknownRequiresRecordedSideEffect(t *testing.T) {
	store, mock := newMockStore(t)
	completedAt := testTime.Add(time.Minute)
	mock.ExpectExec("UPDATE run_attempts").
		WithArgs(
			testAttemptID, runs.AttemptUnknown, completedAt, "worker heartbeat lost",
			runs.AttemptSucceeded, runs.AttemptFailed, runs.AttemptRunning,
			runs.AttemptInterrupted, runs.AttemptClaimed, runs.AttemptUnknown,
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	require.NoError(t, store.CompleteAttempt(context.Background(), runs.AttemptCompletion{
		ID: testAttemptID, Status: runs.AttemptUnknown, CompletedAt: completedAt,
		FailureReason: "worker heartbeat lost",
	}))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateRun(t *testing.T) {
	store, mock := newMockStore(t)
	run := pendingRun()
	mock.ExpectExec("INSERT INTO runs").
		WithArgs(
			run.ID, run.Repository, int64(42), run.Command, run.Trigger, run.Actor,
			run.BaseRef, run.HeadRef, run.HeadSHA, run.Status, run.CreatedAt,
			nil, nil, []byte(`{"environment":"prod","team":"platform"}`),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))

	require.NoError(t, store.CreateRun(context.Background(), run))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestStartRunReplayIsIdempotent(t *testing.T) {
	store, mock := newMockStore(t)
	mock.ExpectExec("UPDATE runs").
		WithArgs(testRunID, runs.StatusRunning, testTime, runs.StatusPending).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT status, started_at FROM runs WHERE id = $1")).
		WithArgs(testRunID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "started_at"}).AddRow(runs.StatusRunning, testTime))

	require.NoError(t, store.StartRun(context.Background(), testRunID, testTime))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCompleteRunRejectsDifferentReplay(t *testing.T) {
	store, mock := newMockStore(t)
	completedAt := testTime.Add(time.Minute)
	mock.ExpectExec("UPDATE runs").
		WithArgs(testRunID, runs.StatusSucceeded, completedAt, runs.StatusRunning).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT status, completed_at FROM runs WHERE id = $1")).
		WithArgs(testRunID).
		WillReturnRows(sqlmock.NewRows([]string{"status", "completed_at"}).AddRow(runs.StatusFailed, completedAt))

	err := store.CompleteRun(context.Background(), runs.RunCompletion{
		ID: testRunID, Status: runs.StatusSucceeded, CompletedAt: completedAt,
	})
	require.ErrorIs(t, err, runs.ErrConflict)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAppendOutputRollsBackConflictingReplay(t *testing.T) {
	store, mock := newMockStore(t)
	chunk := runs.OutputChunk{
		ProjectRunID: testProjectRunID, Sequence: 7, Stream: runs.OutputStdout,
		Content: "terraform output\n", CreatedAt: testTime,
	}
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO run_output_chunks").
		WithArgs(chunk.ProjectRunID, chunk.Sequence, chunk.Stream, chunk.Content, chunk.CreatedAt).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery("SELECT stream, content, created_at").
		WithArgs(chunk.ProjectRunID, chunk.Sequence).
		WillReturnRows(sqlmock.NewRows([]string{"stream", "content", "created_at"}).
			AddRow(runs.OutputStdout, "different output\n", testTime))
	mock.ExpectRollback()

	err := store.AppendOutput(context.Background(), []runs.OutputChunk{chunk})
	require.ErrorIs(t, err, runs.ErrConflict)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestListRunsUsesKeysetCursor(t *testing.T) {
	store, mock := newMockStore(t)
	cursor, err := encodeCursor(timeCursor{CreatedAt: testTime, ID: testRunID})
	require.NoError(t, err)
	query := regexp.QuoteMeta("SELECT "+runColumns+" FROM runs") +
		".*" + regexp.QuoteMeta("(created_at, id) < ($4, $5)") + ".*" +
		regexp.QuoteMeta("ORDER BY created_at DESC, id DESC LIMIT $6")
	mock.ExpectQuery(query).
		WithArgs("org/repo", runs.CommandPlan, runs.StatusSucceeded, testTime, testRunID, 3).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "repository", "pull_number", "command", "trigger", "actor",
			"base_ref", "head_ref", "head_sha", "status", "created_at",
			"started_at", "completed_at", "metadata",
		}))

	page, err := store.ListRuns(context.Background(), runs.RunFilter{
		Repository: "org/repo", Commands: []runs.Command{runs.CommandPlan},
		Statuses: []runs.Status{runs.StatusSucceeded},
	}, runs.PageRequest{Limit: 2, Cursor: cursor})
	require.NoError(t, err)
	require.Empty(t, page.Runs)
	require.Empty(t, page.NextCursor)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestApplyRetentionKeepsCutoffsIndependent(t *testing.T) {
	store, mock := newMockStore(t)
	outputBefore := testTime
	auditBefore := testTime.Add(time.Hour)
	runBefore := testTime.Add(2 * time.Hour)
	driftBefore := testTime.Add(3 * time.Hour)
	mock.ExpectExec("WITH retained_output AS").
		WithArgs(outputBefore, retentionBatchSize).WillReturnResult(sqlmock.NewResult(0, retentionBatchSize))
	mock.ExpectExec("WITH retained_output AS").
		WithArgs(outputBefore, retentionBatchSize).WillReturnResult(sqlmock.NewResult(0, 4))
	mock.ExpectExec("WITH retained_events AS").
		WithArgs(auditBefore, retentionBatchSize).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(regexp.QuoteMeta("WHERE drift_status.identity_hash = retained_drift.identity_hash\n          AND drift_status.last_checked < $1")).
		WithArgs(driftBefore, retentionBatchSize).WillReturnResult(sqlmock.NewResult(0, 6))
	runArgs := []driver.Value{runBefore, runs.StatusSucceeded, runs.StatusFailed, runs.StatusPartial, runs.StatusCancelled, runs.StatusSkipped, runs.StatusUnknown, retentionBatchSize}
	mock.ExpectExec("(?s)WITH retained_output AS.*run_attempts.*reconciled_at IS NULL").
		WithArgs(runArgs...).WillReturnResult(sqlmock.NewResult(0, 8))
	mock.ExpectExec("(?s)WITH retained_projects AS.*run_attempts.*reconciled_at IS NULL").
		WithArgs(runArgs...).WillReturnResult(sqlmock.NewResult(0, 5))
	mock.ExpectExec("(?s)WITH retained_events AS.*run_attempts.*reconciled_at IS NULL").
		WithArgs(runArgs...).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec("(?s)WITH retained_runs AS.*run_attempts.*reconciled_at IS NULL").
		WithArgs(runArgs...).
		WillReturnResult(sqlmock.NewResult(0, retentionBatchSize))
	mock.ExpectExec("(?s)WITH retained_runs AS.*run_attempts.*reconciled_at IS NULL").
		WithArgs(runArgs...).
		WillReturnResult(sqlmock.NewResult(0, 1))

	result, err := store.ApplyRetention(context.Background(), runs.RetentionPolicy{
		RunMetadataBefore: &runBefore, OutputBefore: &outputBefore,
		AuditEventsBefore: &auditBefore, DriftStatusBefore: &driftBefore,
	})
	require.NoError(t, err)
	require.Equal(t, runs.RetentionResult{
		RunsDeleted: retentionBatchSize + 1, ProjectRunsDeleted: 5,
		OutputChunksDeleted: retentionBatchSize + 12, AuditEventsDeleted: 2, DriftStatusesDeleted: 6,
	}, result)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestConfigureConnectionPoolHonorsZeroMaxIdleConnections(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	require.Positive(t, db.Stats().Idle)
	mock.ExpectClose()
	configureConnectionPool(db, Config{MaxIdleConns: 0})
	require.Zero(t, db.Stats().Idle)
	require.NoError(t, mock.ExpectationsWereMet())
	require.NoError(t, db.Close())
}

func TestGetRunMapsNotFound(t *testing.T) {
	store, mock := newMockStore(t)
	mock.ExpectQuery("SELECT .* FROM runs WHERE id = \\$1").WithArgs(testRunID).WillReturnError(sql.ErrNoRows)
	_, err := store.GetRun(context.Background(), testRunID)
	require.ErrorIs(t, err, runs.ErrNotFound)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPageLimitRejectsUnboundedReads(t *testing.T) {
	_, err := pageLimit(maximumPageLimit + 1)
	require.Error(t, err)
}

func TestAppendOutputValidatesBeforeOpeningTransaction(t *testing.T) {
	store, mock := newMockStore(t)
	err := store.AppendOutput(context.Background(), []runs.OutputChunk{{
		ProjectRunID: testProjectRunID, Stream: runs.OutputStdout,
		Content: string(make([]byte, maxOutputChunkBytes+1)), CreatedAt: testTime,
	}})
	require.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMapLookupError(t *testing.T) {
	err := mapLookupError("lookup", sql.ErrNoRows)
	require.True(t, errors.Is(err, runs.ErrNotFound))
}

func newMockStore(t *testing.T) (*Store, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
	})
	return newStore(db, time.Second), mock
}

func pendingRun() runs.Run {
	pull := 42
	return runs.Run{
		ID: testRunID, Repository: "org/repo", PullNumber: &pull,
		Command: runs.CommandPlan, Trigger: runs.TriggerComment, Actor: "octocat",
		BaseRef: "main", HeadRef: "feature", HeadSHA: "abc123",
		Status: runs.StatusPending, CreatedAt: testTime,
		Metadata: runs.Metadata(`{"team":"platform","environment":"prod"}`),
	}
}

func claimedAttempt() runs.RunAttempt {
	return runs.RunAttempt{
		ID: testAttemptID, RunID: testRunID, InstanceID: testInstanceID,
		DeploymentID:   "prod-eu",
		ConcurrencyKey: "sha256:pull-ownership-key", OwnershipClaimID: "claim-1",
		Status: runs.AttemptClaimed, ClaimedAt: testTime, HeartbeatAt: testTime,
	}
}
