// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
)

// PrepareAttemptTakeover classifies an attempt left active by an older Redis
// ownership claim. Both the attempt and its process heartbeat must be stale;
// this intentionally prevents a Redis partition alone from authorizing
// overlapping Terraform execution.
func (s *Store) PrepareAttemptTakeover(ctx context.Context, request runs.AttemptTakeoverRequest) (runs.AttemptTakeoverResult, error) {
	if err := request.Validate(); err != nil {
		return runs.AttemptTakeoverResult{}, fmt.Errorf("validating attempt takeover: %w", err)
	}
	request.HeartbeatBefore = normalizeTime(request.HeartbeatBefore)
	request.RecoveredAt = normalizeTime(request.RecoveredAt)
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()

	tx, err := s.db.BeginTx(opCtx, nil)
	if err != nil {
		return runs.AttemptTakeoverResult{}, fmt.Errorf("beginning attempt takeover: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := lockAttemptAdmission(opCtx, tx, request.DeploymentID, request.ConcurrencyKey); err != nil {
		return runs.AttemptTakeoverResult{}, err
	}

	result, err := prepareAttemptTakeover(opCtx, tx, request)
	if err != nil {
		return runs.AttemptTakeoverResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return runs.AttemptTakeoverResult{}, fmt.Errorf("committing attempt takeover: %w", err)
	}
	return result, nil
}

func prepareAttemptTakeover(ctx context.Context, tx *sql.Tx, request runs.AttemptTakeoverRequest) (runs.AttemptTakeoverResult, error) {
	var result runs.AttemptTakeoverResult
	var activeID runs.ID
	err := tx.QueryRowContext(ctx, `SELECT id FROM run_attempts
		WHERE deployment_id = $1 AND concurrency_key = $2 AND status IN ($3, $4)
		ORDER BY claimed_at DESC, id DESC LIMIT 1 FOR UPDATE`,
		request.DeploymentID, request.ConcurrencyKey, runs.AttemptClaimed, runs.AttemptRunning,
	).Scan(&activeID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return result, fmt.Errorf("locking active execution attempt: %w", err)
	}
	if err == nil {
		attempt, err := scanRunAttempt(tx.QueryRowContext(ctx,
			"SELECT "+runAttemptColumns+" FROM run_attempts WHERE id = $1", activeID))
		if err != nil {
			return result, err
		}
		instance, err := scanExecutionInstance(tx.QueryRowContext(ctx,
			"SELECT "+executionInstanceColumns+" FROM execution_instances WHERE id = $1", attempt.InstanceID))
		if err != nil {
			return result, err
		}
		run, err := scanRun(tx.QueryRowContext(ctx,
			"SELECT "+runColumns+" FROM runs WHERE id = $1 FOR UPDATE", attempt.RunID))
		if err != nil {
			return result, err
		}
		if run.CompletedAt != nil {
			status, reason := terminalRunAttemptClassification(attempt, run)
			completedAt := recoveredAttemptCompletionTime(attempt, maxTime(request.RecoveredAt, *run.CompletedAt))
			if err := completeRecoveredAttempt(ctx, tx, attempt, status, completedAt, reason); err != nil {
				return result, err
			}
			if err := completeRecoveredProjects(ctx, tx, attempt.ID, attempt.RunID, recoveredProjectStatus(status), completedAt, reason); err != nil {
				return result, err
			}
			attempt.Status = status
			attempt.CompletedAt = &completedAt
			attempt.HeartbeatAt = maxTime(attempt.HeartbeatAt, completedAt)
			attempt.FailureReason = reason
			result.RecoveredAttempt = &attempt
		} else if attempt.OwnershipClaimID == request.OwnershipClaimID || attemptIsLive(attempt, instance, request) {
			result.ActiveAttempt = &attempt
			return result, nil
		} else {
			status, reason := takeoverClassification(attempt, request.OwnershipClaimID)
			completedAt := recoveredAttemptCompletionTime(attempt, request.RecoveredAt)
			if err := completeRecoveredAttempt(ctx, tx, attempt, status, completedAt, reason); err != nil {
				return result, err
			}
			if err := completeRecoveredProjects(ctx, tx, attempt.ID, attempt.RunID, recoveredProjectStatus(status), completedAt, reason); err != nil {
				return result, err
			}

			attempt.Status = status
			attempt.CompletedAt = &completedAt
			attempt.HeartbeatAt = maxTime(attempt.HeartbeatAt, completedAt)
			attempt.FailureReason = reason
			result.RecoveredAttempt = &attempt
			if status == runs.AttemptInterrupted && runMatchesPlanRetry(run, request) {
				result.RetryRun = &run
			} else {
				runStatus := runs.StatusFailed
				if status == runs.AttemptUnknown {
					runStatus = runs.StatusUnknown
				}
				if err := completeRecoveredRun(ctx, tx, run, runStatus, completedAt); err != nil {
					return result, err
				}
			}
		}
	}

	unknown, err := latestUnreconciledUnknown(ctx, tx, request.DeploymentID, request.ConcurrencyKey)
	if err != nil {
		return result, err
	}
	result.UnreconciledUnknown = unknown
	return result, nil
}

func recoveredAttemptCompletionTime(attempt runs.RunAttempt, recoveredAt time.Time) time.Time {
	completedAt := maxTime(recoveredAt, attempt.ClaimedAt)
	if attempt.StartedAt != nil {
		completedAt = maxTime(completedAt, *attempt.StartedAt)
	}
	if attempt.SideEffectStartedAt != nil {
		completedAt = maxTime(completedAt, *attempt.SideEffectStartedAt)
	}
	return completedAt
}

func terminalRunAttemptClassification(attempt runs.RunAttempt, run runs.Run) (runs.AttemptStatus, string) {
	if attempt.StartedAt == nil {
		return runs.AttemptInterrupted, "logical run completed before the execution attempt started"
	}
	switch run.Status {
	case runs.StatusSucceeded, runs.StatusSkipped:
		return runs.AttemptSucceeded, ""
	case runs.StatusUnknown:
		if attempt.SideEffectStartedAt != nil {
			return runs.AttemptUnknown, "logical run completed with an unknown infrastructure outcome"
		}
		return runs.AttemptInterrupted, "logical run completed as unknown before an infrastructure side effect was recorded"
	default:
		return runs.AttemptFailed, "logical run completed with status " + string(run.Status)
	}
}

func completeRecoveredAttempt(
	ctx context.Context,
	tx *sql.Tx,
	attempt runs.RunAttempt,
	status runs.AttemptStatus,
	completedAt time.Time,
	reason string,
) error {
	updated, err := tx.ExecContext(ctx, `UPDATE run_attempts
        SET status = $2, completed_at = $3, heartbeat_at = GREATEST(heartbeat_at, $3),
            failure_reason = $4
        WHERE id = $1 AND status IN ($5, $6) AND completed_at IS NULL`,
		attempt.ID, status, completedAt, reason, runs.AttemptClaimed, runs.AttemptRunning)
	if err != nil {
		return fmt.Errorf("classifying stale execution attempt: %w", err)
	}
	count, err := rowsAffected(updated)
	if err != nil {
		return fmt.Errorf("checking stale execution attempt classification: %w", err)
	}
	if count != 1 {
		return fmt.Errorf("classifying stale execution attempt %s: %w", attempt.ID, runs.ErrConflict)
	}
	return nil
}

func attemptIsLive(attempt runs.RunAttempt, instance runs.ExecutionInstance, request runs.AttemptTakeoverRequest) bool {
	if !attempt.HeartbeatAt.Before(request.HeartbeatBefore) {
		return true
	}
	return instance.StoppedAt == nil && !instance.HeartbeatAt.Before(request.HeartbeatBefore)
}

func takeoverClassification(attempt runs.RunAttempt, newClaimID string) (runs.AttemptStatus, string) {
	if attempt.SideEffectStartedAt != nil {
		return runs.AttemptUnknown, fmt.Sprintf(
			"ownership moved to claim %q after attempt and instance heartbeats expired; infrastructure side effect may have completed",
			newClaimID,
		)
	}
	return runs.AttemptInterrupted, fmt.Sprintf(
		"ownership moved to claim %q after attempt and instance heartbeats expired before an infrastructure side effect",
		newClaimID,
	)
}

func recoveredProjectStatus(attemptStatus runs.AttemptStatus) runs.Status {
	if attemptStatus == runs.AttemptUnknown {
		return runs.StatusUnknown
	}
	return runs.StatusFailed
}

func completeRecoveredProjects(ctx context.Context, tx *sql.Tx, attemptID, runID runs.ID, status runs.Status, completedAt time.Time, reason string) error {
	_, err := tx.ExecContext(ctx, `UPDATE project_runs
        SET status = $3, started_at = COALESCE(started_at, $4), completed_at = $4,
            error_summary = CASE WHEN error_summary = '' THEN $5 ELSE error_summary END
        WHERE (attempt_id = $1 OR (attempt_id IS NULL AND run_id = $2))
          AND status IN ($6, $7)`,
		attemptID, runID, status, completedAt, reason,
		runs.StatusPending, runs.StatusRunning)
	if err != nil {
		return fmt.Errorf("completing project results for stale attempt: %w", err)
	}
	return nil
}

func runMatchesPlanRetry(run runs.Run, request runs.AttemptTakeoverRequest) bool {
	if run.Command != runs.CommandPlan || request.Command != runs.CommandPlan {
		return false
	}
	return run.Repository == request.Repository && equalPullNumber(run.PullNumber, request.PullNumber) &&
		run.Trigger == request.Trigger && run.Actor == request.Actor && run.BaseRef == request.BaseRef &&
		run.HeadRef == request.HeadRef && run.HeadSHA == request.HeadSHA
}

func equalPullNumber(left, right *int) bool {
	return left != nil && right != nil && *left == *right
}

func completeRecoveredRun(ctx context.Context, tx *sql.Tx, run runs.Run, status runs.Status, completedAt time.Time) error {
	result, err := tx.ExecContext(ctx, `UPDATE runs
        SET status = $2, completed_at = $3
        WHERE id = $1 AND status = $4 AND completed_at IS NULL`,
		run.ID, status, completedAt, runs.StatusRunning)
	if err != nil {
		return fmt.Errorf("completing run for stale attempt: %w", err)
	}
	updated, err := rowsAffected(result)
	if err != nil {
		return fmt.Errorf("checking completed run for stale attempt: %w", err)
	}
	if updated != 1 {
		return fmt.Errorf("completing run %s for stale attempt from status %q: %w", run.ID, run.Status, runs.ErrConflict)
	}
	return nil
}

func latestUnreconciledUnknown(ctx context.Context, tx *sql.Tx, deploymentID, concurrencyKey string) (*runs.RunAttempt, error) {
	attempt, err := scanRunAttempt(tx.QueryRowContext(ctx, `SELECT `+runAttemptColumns+` FROM run_attempts
		WHERE deployment_id = $1 AND concurrency_key = $2 AND status = $3 AND reconciled_at IS NULL
		ORDER BY completed_at DESC, id DESC LIMIT 1`, deploymentID, concurrencyKey, runs.AttemptUnknown))
	if errors.Is(err, runs.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading unreconciled unknown attempt: %w", err)
	}
	return &attempt, nil
}

func maxTime(left, right time.Time) time.Time {
	if right.After(left) {
		return right
	}
	return left
}
