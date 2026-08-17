// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/stretchr/testify/require"
)

func TestAttemptIsLiveUntilBothHeartbeatsExpire(t *testing.T) {
	cutoff := testTime
	request := runs.AttemptTakeoverRequest{HeartbeatBefore: cutoff}
	attempt := claimedAttempt()
	instance := runs.ExecutionInstance{HeartbeatAt: cutoff.Add(-time.Second)}

	attempt.HeartbeatAt = cutoff
	require.True(t, attemptIsLive(attempt, instance, request))
	attempt.HeartbeatAt = cutoff.Add(-time.Second)
	instance.HeartbeatAt = cutoff
	require.True(t, attemptIsLive(attempt, instance, request))
	instance.HeartbeatAt = cutoff.Add(-time.Second)
	require.False(t, attemptIsLive(attempt, instance, request))
	instance.StoppedAt = &cutoff
	require.False(t, attemptIsLive(attempt, instance, request))
}

func TestAttemptTakeoverRequiresDeploymentScope(t *testing.T) {
	pull := 42
	request := runs.AttemptTakeoverRequest{
		ConcurrencyKey: "sha256:key", OwnershipClaimID: "claim-2",
		HeartbeatBefore: testTime, RecoveredAt: testTime, Repository: "org/repo",
		PullNumber: &pull, Command: runs.CommandPlan, Trigger: runs.TriggerComment,
		HeadSHA: "abc123",
	}
	require.ErrorContains(t, request.Validate(), "deployment ID")
	request.DeploymentID = "prod-eu"
	require.NoError(t, request.Validate())
}

func TestTakeoverClassificationSeparatesPlanFromPossibleSideEffect(t *testing.T) {
	attempt := claimedAttempt()
	status, reason := takeoverClassification(attempt, "new-claim")
	require.Equal(t, runs.AttemptInterrupted, status)
	require.Contains(t, reason, "before an infrastructure side effect")

	attempt.SideEffectStartedAt = &testTime
	status, reason = takeoverClassification(attempt, "new-claim")
	require.Equal(t, runs.AttemptUnknown, status)
	require.Contains(t, reason, "may have completed")
}

func TestTerminalRunAttemptClassificationRepairsOldCompletionOrdering(t *testing.T) {
	startedAt := testTime
	completedAt := startedAt.Add(time.Minute)
	attempt := claimedAttempt()
	attempt.Status = runs.AttemptRunning
	attempt.StartedAt = &startedAt
	run := runs.Run{Status: runs.StatusSucceeded, CompletedAt: &completedAt}

	status, reason := terminalRunAttemptClassification(attempt, run)
	require.Equal(t, runs.AttemptSucceeded, status)
	require.Empty(t, reason)

	run.Status = runs.StatusUnknown
	attempt.SideEffectStartedAt = &startedAt
	status, reason = terminalRunAttemptClassification(attempt, run)
	require.Equal(t, runs.AttemptUnknown, status)
	require.Contains(t, reason, "unknown infrastructure outcome")

	attempt.StartedAt = nil
	attempt.SideEffectStartedAt = nil
	status, reason = terminalRunAttemptClassification(attempt, run)
	require.Equal(t, runs.AttemptInterrupted, status)
	require.Contains(t, reason, "before the execution attempt started")
}

func TestRecoveredAttemptCompletionDoesNotPrecedeLifecycle(t *testing.T) {
	startedAt := testTime.Add(time.Minute)
	sideEffectAt := startedAt.Add(time.Minute)
	attempt := claimedAttempt()
	attempt.StartedAt = &startedAt
	attempt.SideEffectStartedAt = &sideEffectAt

	require.Equal(t, sideEffectAt, recoveredAttemptCompletionTime(attempt, testTime.Add(-time.Minute)))
}

func TestRunMatchesPlanRetryRequiresExactLogicalOperation(t *testing.T) {
	pull := 42
	startedAt := testTime
	run := runs.Run{
		ID: testRunID, Repository: "org/repo", PullNumber: &pull,
		Command: runs.CommandPlan, Trigger: runs.TriggerComment, Actor: "operator",
		BaseRef: "main", HeadRef: "feature", HeadSHA: "abc123",
		Status: runs.StatusRunning, CreatedAt: startedAt, StartedAt: &startedAt,
	}
	request := runs.AttemptTakeoverRequest{
		Repository: run.Repository, PullNumber: &pull, Command: run.Command,
		Trigger: run.Trigger, Actor: run.Actor, BaseRef: run.BaseRef,
		HeadRef: run.HeadRef, HeadSHA: run.HeadSHA,
	}
	require.True(t, runMatchesPlanRetry(run, request))

	request.HeadSHA = "new-commit"
	require.False(t, runMatchesPlanRetry(run, request))
	request.HeadSHA = run.HeadSHA
	request.Command = runs.CommandApply
	require.False(t, runMatchesPlanRetry(run, request))
}

func TestCompleteRecoveredProjectsIncludesRollingUpgradeRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	mock.ExpectExec(`(?s)WHERE \(attempt_id = \$1 OR \(attempt_id IS NULL AND run_id = \$2\)\)`).
		WithArgs(
			testAttemptID, testRunID, runs.StatusFailed, testTime, "replica disappeared",
			runs.StatusPending, runs.StatusRunning,
		).
		WillReturnResult(sqlmock.NewResult(0, 2))

	require.NoError(t, completeRecoveredProjects(
		context.Background(), tx, testAttemptID, testRunID, runs.StatusFailed, testTime, "replica disappeared",
	))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRecoveredProjectStatusPreservesUnknownApplyOutcome(t *testing.T) {
	require.Equal(t, runs.StatusUnknown, recoveredProjectStatus(runs.AttemptUnknown))
	require.Equal(t, runs.StatusFailed, recoveredProjectStatus(runs.AttemptInterrupted))
}

func TestUnknownRecoveryLookupIsDeploymentScoped(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	mock.ExpectQuery(`(?s)WHERE deployment_id = \$1 AND concurrency_key = \$2 AND status = \$3`).
		WithArgs("prod-eu", "sha256:key", runs.AttemptUnknown).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	attempt, err := latestUnreconciledUnknown(context.Background(), tx, "prod-eu", "sha256:key")

	require.NoError(t, err)
	require.Nil(t, attempt)
	require.NoError(t, mock.ExpectationsWereMet())
}
