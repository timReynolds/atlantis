// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
)

const executionInstanceColumns = `id, replica_id, deployment_id, advertise_url,
    started_at, heartbeat_at, stopped_at, version, commit_sha, metadata`

const runAttemptColumns = `id, run_id, instance_id, concurrency_key,
    ownership_claim_id, status, claimed_at, started_at, heartbeat_at,
    side_effect_started_at, completed_at, failure_reason, reconciled_at,
    reconciled_by, reconciliation_summary, metadata`

// RegisterInstance durably records one Atlantis process lifetime. Replaying the
// same process identity is idempotent; changing its identity fields conflicts.
func (s *Store) RegisterInstance(ctx context.Context, instance runs.ExecutionInstance) error {
	if err := instance.Validate(); err != nil {
		return fmt.Errorf("validating execution instance: %w", err)
	}
	instance = normalizeExecutionInstance(instance)
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()

	result, err := s.db.ExecContext(opCtx, `INSERT INTO execution_instances (
        id, replica_id, deployment_id, advertise_url, started_at, heartbeat_at,
        stopped_at, version, commit_sha, metadata
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
    ON CONFLICT DO NOTHING`,
		instance.ID, instance.ReplicaID, instance.DeploymentID, instance.AdvertiseURL,
		instance.StartedAt, instance.HeartbeatAt, instance.StoppedAt, instance.Version,
		instance.Commit, []byte(instance.Metadata),
	)
	if err != nil {
		return fmt.Errorf("registering execution instance: %w", err)
	}
	inserted, err := rowsAffected(result)
	if err != nil {
		return fmt.Errorf("checking registered execution instance: %w", err)
	}
	if inserted == 1 {
		return nil
	}

	existing, err := s.getInstance(opCtx, instance.ID)
	if err != nil {
		if errors.Is(err, runs.ErrNotFound) {
			return fmt.Errorf("registering execution instance: %w", runs.ErrConflict)
		}
		return err
	}
	if !instanceRegistrationMatches(existing, instance) {
		return fmt.Errorf("registering execution instance %s: %w", instance.ID, runs.ErrConflict)
	}
	return nil
}

// HeartbeatInstance advances a live process heartbeat monotonically.
func (s *Store) HeartbeatInstance(ctx context.Context, id runs.ID, heartbeatAt time.Time) error {
	if err := validateExecutionTimestamp(id, heartbeatAt, "instance heartbeat"); err != nil {
		return err
	}
	heartbeatAt = normalizeTime(heartbeatAt)
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()

	result, err := s.db.ExecContext(opCtx, `UPDATE execution_instances
        SET heartbeat_at = $2
        WHERE id = $1 AND stopped_at IS NULL AND heartbeat_at <= $2`, id, heartbeatAt)
	if err != nil {
		return fmt.Errorf("heartbeating execution instance: %w", err)
	}
	updated, err := rowsAffected(result)
	if err != nil {
		return fmt.Errorf("checking execution instance heartbeat: %w", err)
	}
	if updated == 1 {
		return nil
	}
	existing, err := s.getInstance(opCtx, id)
	if err != nil {
		return err
	}
	if existing.StoppedAt == nil && !existing.HeartbeatAt.Before(heartbeatAt) {
		return nil
	}
	return fmt.Errorf("heartbeating execution instance %s after stop: %w", id, runs.ErrConflict)
}

// StopInstance records a graceful process stop without relying on it for crash
// correctness. The final heartbeat is advanced to at least the stop time.
func (s *Store) StopInstance(ctx context.Context, id runs.ID, stoppedAt time.Time) error {
	if err := validateExecutionTimestamp(id, stoppedAt, "instance stop"); err != nil {
		return err
	}
	stoppedAt = normalizeTime(stoppedAt)
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()

	result, err := s.db.ExecContext(opCtx, `UPDATE execution_instances
        SET stopped_at = $2, heartbeat_at = GREATEST(heartbeat_at, $2)
        WHERE id = $1 AND stopped_at IS NULL`, id, stoppedAt)
	if err != nil {
		return fmt.Errorf("stopping execution instance: %w", err)
	}
	updated, err := rowsAffected(result)
	if err != nil {
		return fmt.Errorf("checking stopped execution instance: %w", err)
	}
	if updated == 1 {
		return nil
	}
	existing, err := s.getInstance(opCtx, id)
	if err != nil {
		return err
	}
	if existing.StoppedAt != nil && existing.StoppedAt.Equal(stoppedAt) {
		return nil
	}
	return fmt.Errorf("stopping execution instance %s: %w", id, runs.ErrConflict)
}

// CreateAttempt records admission under a Redis ownership claim. PostgreSQL's
// partial unique index provides a durable second fence against overlapping
// active attempts; it does not replace the Redis lease.
func (s *Store) CreateAttempt(ctx context.Context, attempt runs.RunAttempt) error {
	if err := attempt.Validate(); err != nil {
		return fmt.Errorf("validating run attempt: %w", err)
	}
	if attempt.Status != runs.AttemptClaimed {
		return fmt.Errorf("creating run attempt requires claimed status")
	}
	attempt = normalizeRunAttempt(attempt)
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()

	result, err := s.db.ExecContext(opCtx, `INSERT INTO run_attempts (
        id, run_id, instance_id, concurrency_key, ownership_claim_id, status,
        claimed_at, started_at, heartbeat_at, side_effect_started_at,
        completed_at, failure_reason, reconciled_at, reconciled_by,
        reconciliation_summary, metadata
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
    ON CONFLICT DO NOTHING`,
		attempt.ID, attempt.RunID, attempt.InstanceID, attempt.ConcurrencyKey,
		attempt.OwnershipClaimID, attempt.Status, attempt.ClaimedAt, attempt.StartedAt,
		attempt.HeartbeatAt, attempt.SideEffectStartedAt, attempt.CompletedAt,
		attempt.FailureReason, attempt.ReconciledAt, attempt.ReconciledBy,
		attempt.ReconciliationSummary, []byte(attempt.Metadata),
	)
	if err != nil {
		return fmt.Errorf("creating run attempt: %w", err)
	}
	inserted, err := rowsAffected(result)
	if err != nil {
		return fmt.Errorf("checking created run attempt: %w", err)
	}
	if inserted == 1 {
		return nil
	}

	existing, err := s.getAttempt(opCtx, attempt.ID)
	if err != nil {
		if errors.Is(err, runs.ErrNotFound) {
			return fmt.Errorf("creating run attempt for concurrency key %q: %w", attempt.ConcurrencyKey, runs.ErrConflict)
		}
		return err
	}
	if !attemptAdmissionMatches(existing, attempt) {
		return fmt.Errorf("creating run attempt %s: %w", attempt.ID, runs.ErrConflict)
	}
	return nil
}

func (s *Store) StartAttempt(ctx context.Context, id runs.ID, startedAt time.Time) error {
	if err := validateExecutionTimestamp(id, startedAt, "attempt start"); err != nil {
		return err
	}
	startedAt = normalizeTime(startedAt)
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()

	result, err := s.db.ExecContext(opCtx, `UPDATE run_attempts
        SET status = $2, started_at = $3, heartbeat_at = GREATEST(heartbeat_at, $3)
        WHERE id = $1 AND status = $4 AND started_at IS NULL`,
		id, runs.AttemptRunning, startedAt, runs.AttemptClaimed)
	if err != nil {
		return fmt.Errorf("starting run attempt: %w", err)
	}
	updated, err := rowsAffected(result)
	if err != nil {
		return fmt.Errorf("checking started run attempt: %w", err)
	}
	if updated == 1 {
		return nil
	}
	existing, err := s.getAttempt(opCtx, id)
	if err != nil {
		return err
	}
	if existing.Status == runs.AttemptRunning && existing.StartedAt != nil && existing.StartedAt.Equal(startedAt) {
		return nil
	}
	return fmt.Errorf("starting run attempt %s from status %q: %w", id, existing.Status, runs.ErrConflict)
}

func (s *Store) HeartbeatAttempt(ctx context.Context, id runs.ID, heartbeatAt time.Time) error {
	if err := validateExecutionTimestamp(id, heartbeatAt, "attempt heartbeat"); err != nil {
		return err
	}
	heartbeatAt = normalizeTime(heartbeatAt)
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()

	result, err := s.db.ExecContext(opCtx, `UPDATE run_attempts
        SET heartbeat_at = $2
        WHERE id = $1 AND status IN ($3, $4) AND heartbeat_at <= $2`,
		id, heartbeatAt, runs.AttemptClaimed, runs.AttemptRunning)
	if err != nil {
		return fmt.Errorf("heartbeating run attempt: %w", err)
	}
	updated, err := rowsAffected(result)
	if err != nil {
		return fmt.Errorf("checking run attempt heartbeat: %w", err)
	}
	if updated == 1 {
		return nil
	}
	existing, err := s.getAttempt(opCtx, id)
	if err != nil {
		return err
	}
	if (existing.Status == runs.AttemptClaimed || existing.Status == runs.AttemptRunning) && !existing.HeartbeatAt.Before(heartbeatAt) {
		return nil
	}
	return fmt.Errorf("heartbeating run attempt %s in status %q: %w", id, existing.Status, runs.ErrConflict)
}

// MarkAttemptSideEffectStarted is the fail-closed boundary immediately before
// Terraform apply or another infrastructure mutation. A process must not start
// the side effect if this durable write fails.
func (s *Store) MarkAttemptSideEffectStarted(ctx context.Context, id runs.ID, startedAt time.Time) error {
	if err := validateExecutionTimestamp(id, startedAt, "attempt side effect start"); err != nil {
		return err
	}
	startedAt = normalizeTime(startedAt)
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()

	result, err := s.db.ExecContext(opCtx, `UPDATE run_attempts
        SET side_effect_started_at = $2, heartbeat_at = GREATEST(heartbeat_at, $2)
        WHERE id = $1 AND status = $3 AND side_effect_started_at IS NULL`,
		id, startedAt, runs.AttemptRunning)
	if err != nil {
		return fmt.Errorf("marking run attempt side effect: %w", err)
	}
	updated, err := rowsAffected(result)
	if err != nil {
		return fmt.Errorf("checking run attempt side effect: %w", err)
	}
	if updated == 1 {
		return nil
	}
	existing, err := s.getAttempt(opCtx, id)
	if err != nil {
		return err
	}
	if existing.Status == runs.AttemptRunning && existing.SideEffectStartedAt != nil && existing.SideEffectStartedAt.Equal(startedAt) {
		return nil
	}
	return fmt.Errorf("marking run attempt %s side effect in status %q: %w", id, existing.Status, runs.ErrConflict)
}

func (s *Store) CompleteAttempt(ctx context.Context, completion runs.AttemptCompletion) error {
	if err := completion.Validate(); err != nil {
		return fmt.Errorf("validating run attempt completion: %w", err)
	}
	completion.CompletedAt = normalizeTime(completion.CompletedAt)
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()

	result, err := s.db.ExecContext(opCtx, `UPDATE run_attempts
        SET status = $2, completed_at = $3, failure_reason = $4,
            heartbeat_at = GREATEST(heartbeat_at, $3)
        WHERE id = $1 AND completed_at IS NULL AND (
            ($2 IN ($5, $6) AND status = $7) OR
            ($2 = $8 AND status IN ($9, $7) AND side_effect_started_at IS NULL) OR
            ($2 = $10 AND status = $7 AND side_effect_started_at IS NOT NULL)
        )`,
		completion.ID, completion.Status, completion.CompletedAt, completion.FailureReason,
		runs.AttemptSucceeded, runs.AttemptFailed, runs.AttemptRunning,
		runs.AttemptInterrupted, runs.AttemptClaimed, runs.AttemptUnknown)
	if err != nil {
		return fmt.Errorf("completing run attempt: %w", err)
	}
	updated, err := rowsAffected(result)
	if err != nil {
		return fmt.Errorf("checking completed run attempt: %w", err)
	}
	if updated == 1 {
		return nil
	}
	existing, err := s.getAttempt(opCtx, completion.ID)
	if err != nil {
		return err
	}
	if existing.Status == completion.Status && existing.CompletedAt != nil &&
		existing.CompletedAt.Equal(completion.CompletedAt) && existing.FailureReason == completion.FailureReason {
		return nil
	}
	return fmt.Errorf("completing run attempt %s from status %q: %w", completion.ID, existing.Status, runs.ErrConflict)
}

func (s *Store) ReconcileAttempt(ctx context.Context, reconciliation runs.AttemptReconciliation) error {
	if err := reconciliation.Validate(); err != nil {
		return fmt.Errorf("validating run attempt reconciliation: %w", err)
	}
	reconciliation.At = normalizeTime(reconciliation.At)
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()

	result, err := s.db.ExecContext(opCtx, `UPDATE run_attempts
        SET reconciled_at = $2, reconciled_by = $3, reconciliation_summary = $4
        WHERE id = $1 AND status = $5 AND reconciled_at IS NULL`,
		reconciliation.ID, reconciliation.At, reconciliation.Actor,
		reconciliation.Summary, runs.AttemptUnknown)
	if err != nil {
		return fmt.Errorf("reconciling run attempt: %w", err)
	}
	updated, err := rowsAffected(result)
	if err != nil {
		return fmt.Errorf("checking reconciled run attempt: %w", err)
	}
	if updated == 1 {
		return nil
	}
	existing, err := s.getAttempt(opCtx, reconciliation.ID)
	if err != nil {
		return err
	}
	if existing.Status == runs.AttemptUnknown && existing.ReconciledAt != nil &&
		existing.ReconciledAt.Equal(reconciliation.At) &&
		existing.ReconciledBy == reconciliation.Actor &&
		existing.ReconciliationSummary == reconciliation.Summary {
		return nil
	}
	return fmt.Errorf("reconciling run attempt %s in status %q: %w", reconciliation.ID, existing.Status, runs.ErrConflict)
}

func (s *Store) GetInstance(ctx context.Context, id runs.ID) (runs.ExecutionInstance, error) {
	if _, err := runs.ParseID(string(id)); err != nil {
		return runs.ExecutionInstance{}, fmt.Errorf("validating execution instance ID: %w", err)
	}
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()
	return s.getInstance(opCtx, id)
}

func (s *Store) getInstance(ctx context.Context, id runs.ID) (runs.ExecutionInstance, error) {
	return scanExecutionInstance(s.db.QueryRowContext(ctx,
		"SELECT "+executionInstanceColumns+" FROM execution_instances WHERE id = $1", id))
}

func (s *Store) GetAttempt(ctx context.Context, id runs.ID) (runs.RunAttempt, error) {
	if _, err := runs.ParseID(string(id)); err != nil {
		return runs.RunAttempt{}, fmt.Errorf("validating run attempt ID: %w", err)
	}
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()
	return s.getAttempt(opCtx, id)
}

func (s *Store) getAttempt(ctx context.Context, id runs.ID) (runs.RunAttempt, error) {
	return scanRunAttempt(s.db.QueryRowContext(ctx,
		"SELECT "+runAttemptColumns+" FROM run_attempts WHERE id = $1", id))
}

func (s *Store) ListRunAttempts(ctx context.Context, runID runs.ID, page runs.PageRequest) (runs.AttemptPage, error) {
	if _, err := runs.ParseID(string(runID)); err != nil {
		return runs.AttemptPage{}, fmt.Errorf("validating run ID: %w", err)
	}
	limit, err := pageLimit(page.Limit)
	if err != nil {
		return runs.AttemptPage{}, err
	}
	query := "SELECT " + runAttemptColumns + " FROM run_attempts WHERE run_id = $1"
	args := []any{runID}
	if page.Cursor != "" {
		cursor, err := decodeTimeCursor(page.Cursor)
		if err != nil {
			return runs.AttemptPage{}, fmt.Errorf("decoding run attempt page cursor: %w", err)
		}
		args = append(args, cursor.CreatedAt, cursor.ID)
		query += " AND (claimed_at, id) < ($2, $3)"
	}
	args = append(args, limit+1)
	query += fmt.Sprintf(" ORDER BY claimed_at DESC, id DESC LIMIT $%d", len(args))

	opCtx, cancel := s.operationContext(ctx)
	defer cancel()
	rows, err := s.db.QueryContext(opCtx, query, args...)
	if err != nil {
		return runs.AttemptPage{}, fmt.Errorf("listing run attempts: %w", err)
	}
	defer rows.Close()

	result := runs.AttemptPage{Attempts: make([]runs.RunAttempt, 0, limit)}
	for rows.Next() {
		attempt, err := scanRunAttempt(rows)
		if err != nil {
			return runs.AttemptPage{}, err
		}
		result.Attempts = append(result.Attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		return runs.AttemptPage{}, fmt.Errorf("iterating run attempts: %w", err)
	}
	if len(result.Attempts) > limit {
		last := result.Attempts[limit-1]
		result.NextCursor, err = encodeCursor(timeCursor{CreatedAt: last.ClaimedAt, ID: last.ID})
		if err != nil {
			return runs.AttemptPage{}, fmt.Errorf("encoding run attempt page cursor: %w", err)
		}
		result.Attempts = result.Attempts[:limit]
	}
	return result, nil
}

func scanExecutionInstance(row rowScanner) (runs.ExecutionInstance, error) {
	var instance runs.ExecutionInstance
	var stoppedAt sql.NullTime
	var metadata []byte
	if err := row.Scan(
		&instance.ID, &instance.ReplicaID, &instance.DeploymentID, &instance.AdvertiseURL,
		&instance.StartedAt, &instance.HeartbeatAt, &stoppedAt, &instance.Version,
		&instance.Commit, &metadata,
	); err != nil {
		return runs.ExecutionInstance{}, mapLookupError("scanning execution instance", err)
	}
	instance.StartedAt = normalizeTime(instance.StartedAt)
	instance.HeartbeatAt = normalizeTime(instance.HeartbeatAt)
	instance.StoppedAt = timeFromNull(stoppedAt)
	instance.Metadata = normalizeMetadata(metadata)
	return instance, nil
}

func scanRunAttempt(row rowScanner) (runs.RunAttempt, error) {
	var attempt runs.RunAttempt
	var startedAt, sideEffectStartedAt, completedAt, reconciledAt sql.NullTime
	var metadata []byte
	if err := row.Scan(
		&attempt.ID, &attempt.RunID, &attempt.InstanceID, &attempt.ConcurrencyKey,
		&attempt.OwnershipClaimID, &attempt.Status, &attempt.ClaimedAt, &startedAt,
		&attempt.HeartbeatAt, &sideEffectStartedAt, &completedAt, &attempt.FailureReason,
		&reconciledAt, &attempt.ReconciledBy, &attempt.ReconciliationSummary, &metadata,
	); err != nil {
		return runs.RunAttempt{}, mapLookupError("scanning run attempt", err)
	}
	attempt.ClaimedAt = normalizeTime(attempt.ClaimedAt)
	attempt.StartedAt = timeFromNull(startedAt)
	attempt.HeartbeatAt = normalizeTime(attempt.HeartbeatAt)
	attempt.SideEffectStartedAt = timeFromNull(sideEffectStartedAt)
	attempt.CompletedAt = timeFromNull(completedAt)
	attempt.ReconciledAt = timeFromNull(reconciledAt)
	attempt.Metadata = normalizeMetadata(metadata)
	return attempt, nil
}

func validateExecutionTimestamp(id runs.ID, timestamp time.Time, field string) error {
	if _, err := runs.ParseID(string(id)); err != nil {
		return fmt.Errorf("validating %s ID: %w", field, err)
	}
	if timestamp.IsZero() {
		return fmt.Errorf("%s time is required", field)
	}
	return nil
}

func normalizeExecutionInstance(instance runs.ExecutionInstance) runs.ExecutionInstance {
	instance.StartedAt = normalizeTime(instance.StartedAt)
	instance.HeartbeatAt = normalizeTime(instance.HeartbeatAt)
	instance.StoppedAt = normalizeTimePtr(instance.StoppedAt)
	instance.Metadata = normalizeMetadata(instance.Metadata)
	return instance
}

func normalizeRunAttempt(attempt runs.RunAttempt) runs.RunAttempt {
	attempt.ClaimedAt = normalizeTime(attempt.ClaimedAt)
	attempt.StartedAt = normalizeTimePtr(attempt.StartedAt)
	attempt.HeartbeatAt = normalizeTime(attempt.HeartbeatAt)
	attempt.SideEffectStartedAt = normalizeTimePtr(attempt.SideEffectStartedAt)
	attempt.CompletedAt = normalizeTimePtr(attempt.CompletedAt)
	attempt.ReconciledAt = normalizeTimePtr(attempt.ReconciledAt)
	attempt.Metadata = normalizeMetadata(attempt.Metadata)
	return attempt
}

func instanceRegistrationMatches(existing, registered runs.ExecutionInstance) bool {
	return existing.ID == registered.ID &&
		existing.ReplicaID == registered.ReplicaID &&
		existing.DeploymentID == registered.DeploymentID &&
		existing.AdvertiseURL == registered.AdvertiseURL &&
		existing.StartedAt.Equal(registered.StartedAt) &&
		!existing.HeartbeatAt.Before(registered.HeartbeatAt) &&
		existing.Version == registered.Version &&
		existing.Commit == registered.Commit &&
		reflect.DeepEqual(existing.Metadata, registered.Metadata)
}

func attemptAdmissionMatches(existing, admitted runs.RunAttempt) bool {
	return existing.ID == admitted.ID &&
		existing.RunID == admitted.RunID &&
		existing.InstanceID == admitted.InstanceID &&
		existing.ConcurrencyKey == admitted.ConcurrencyKey &&
		existing.OwnershipClaimID == admitted.OwnershipClaimID &&
		existing.ClaimedAt.Equal(admitted.ClaimedAt) &&
		!existing.HeartbeatAt.Before(admitted.HeartbeatAt) &&
		reflect.DeepEqual(existing.Metadata, admitted.Metadata)
}
