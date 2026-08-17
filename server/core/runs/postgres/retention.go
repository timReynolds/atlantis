// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
)

const retentionBatchSize = 1000

func (s *Store) ApplyRetention(ctx context.Context, policy runs.RetentionPolicy) (runs.RetentionResult, error) {
	if policy.RunMetadataBefore == nil && policy.OutputBefore == nil && policy.AuditEventsBefore == nil && policy.DriftStatusBefore == nil {
		return runs.RetentionResult{}, nil
	}
	var result runs.RetentionResult
	if policy.OutputBefore != nil {
		deleted, err := s.deleteBeforeInBatches(ctx, `WITH retained_output AS (
            SELECT id FROM run_output_chunks
            WHERE created_at < $1
            ORDER BY created_at, id
            LIMIT $2
        )
        DELETE FROM run_output_chunks
        USING retained_output
        WHERE run_output_chunks.id = retained_output.id`,
			normalizeTime(*policy.OutputBefore),
		)
		if err != nil {
			return runs.RetentionResult{}, fmt.Errorf("deleting retained run output: %w", err)
		}
		result.OutputChunksDeleted += deleted
	}
	if policy.AuditEventsBefore != nil {
		deleted, err := s.deleteBeforeInBatches(ctx, `WITH retained_events AS (
            SELECT id FROM audit_events
            WHERE created_at < $1
            ORDER BY created_at, id
            LIMIT $2
        )
        DELETE FROM audit_events
        USING retained_events
        WHERE audit_events.id = retained_events.id`,
			normalizeTime(*policy.AuditEventsBefore),
		)
		if err != nil {
			return runs.RetentionResult{}, fmt.Errorf("deleting retained audit events: %w", err)
		}
		result.AuditEventsDeleted = deleted
	}
	if policy.DriftStatusBefore != nil {
		deleted, err := s.deleteBeforeInBatches(ctx, `WITH retained_drift AS (
            SELECT identity_hash FROM drift_status
            WHERE last_checked < $1
            ORDER BY last_checked, identity_hash
            LIMIT $2
        )
        DELETE FROM drift_status
        USING retained_drift
        WHERE drift_status.identity_hash = retained_drift.identity_hash
          AND drift_status.last_checked < $1`,
			normalizeTime(*policy.DriftStatusBefore),
		)
		if err != nil {
			return runs.RetentionResult{}, fmt.Errorf("deleting retained drift status: %w", err)
		}
		result.DriftStatusesDeleted = deleted
	}
	if policy.RunMetadataBefore != nil {
		deleted, err := s.deleteRunMetadataInBatches(ctx, normalizeTime(*policy.RunMetadataBefore))
		if err != nil {
			return runs.RetentionResult{}, fmt.Errorf("deleting retained runs: %w", err)
		}
		result.RunsDeleted = deleted.RunsDeleted
		result.ProjectRunsDeleted = deleted.ProjectRunsDeleted
		result.OutputChunksDeleted += deleted.OutputChunksDeleted
	}
	return result, nil
}

func (s *Store) deleteBeforeInBatches(ctx context.Context, query string, cutoff time.Time) (int64, error) {
	var total int64
	for {
		opCtx, cancel := s.operationContext(ctx)
		result, err := s.db.ExecContext(opCtx, query, cutoff, retentionBatchSize)
		cancel()
		if err != nil {
			return 0, err
		}
		deleted, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		total += deleted
		if deleted < retentionBatchSize {
			return total, nil
		}
	}
}

func (s *Store) deleteRunMetadataInBatches(ctx context.Context, cutoff time.Time) (runs.RetentionResult, error) {
	terminal := []runs.Status{
		runs.StatusSucceeded, runs.StatusFailed, runs.StatusPartial,
		runs.StatusCancelled, runs.StatusSkipped,
	}
	output, err := s.deleteRunChildrenInBatches(ctx, `WITH retained_output AS (
            SELECT output.id
            FROM run_output_chunks output
            JOIN project_runs project ON project.id = output.project_run_id
            JOIN runs run ON run.id = project.run_id
            WHERE run.completed_at < $1
              AND run.status IN ($2, $3, $4, $5, $6)
              AND NOT EXISTS (
                  SELECT 1 FROM run_attempts attempt
                  WHERE attempt.run_id = run.id
                    AND (
                      attempt.status IN ('claimed', 'running')
                      OR (attempt.status = 'unknown' AND attempt.reconciled_at IS NULL)
                    )
              )
            ORDER BY run.completed_at, run.id, project.id, output.id
            LIMIT $7
        )
        DELETE FROM run_output_chunks
        USING retained_output
        WHERE run_output_chunks.id = retained_output.id`, cutoff, terminal)
	if err != nil {
		return runs.RetentionResult{}, err
	}
	projects, err := s.deleteRunChildrenInBatches(ctx, `WITH retained_projects AS (
            SELECT project.id
            FROM project_runs project
            JOIN runs run ON run.id = project.run_id
            WHERE run.completed_at < $1
              AND run.status IN ($2, $3, $4, $5, $6)
              AND NOT EXISTS (
                  SELECT 1 FROM run_attempts attempt
                  WHERE attempt.run_id = run.id
                    AND (
                      attempt.status IN ('claimed', 'running')
                      OR (attempt.status = 'unknown' AND attempt.reconciled_at IS NULL)
                    )
              )
              AND NOT EXISTS (
                  SELECT 1 FROM run_output_chunks output
                  WHERE output.project_run_id = project.id
              )
            ORDER BY run.completed_at, run.id, project.id
            LIMIT $7
        )
        DELETE FROM project_runs
        USING retained_projects
        WHERE project_runs.id = retained_projects.id`, cutoff, terminal)
	if err != nil {
		return runs.RetentionResult{}, err
	}
	if _, err := s.deleteRunChildrenInBatches(ctx, `WITH retained_events AS (
            SELECT event.id
            FROM audit_events event
            JOIN runs run ON run.id = event.run_id
            WHERE run.completed_at < $1
              AND run.status IN ($2, $3, $4, $5, $6)
              AND NOT EXISTS (
                  SELECT 1 FROM run_attempts attempt
                  WHERE attempt.run_id = run.id
                    AND (
                      attempt.status IN ('claimed', 'running')
                      OR (attempt.status = 'unknown' AND attempt.reconciled_at IS NULL)
                    )
              )
            ORDER BY run.completed_at, run.id, event.id
            LIMIT $7
        )
        UPDATE audit_events
        SET run_id = NULL
        FROM retained_events
        WHERE audit_events.id = retained_events.id`, cutoff, terminal); err != nil {
		return runs.RetentionResult{}, err
	}
	deletedRuns, err := s.deleteRunChildrenInBatches(ctx, `WITH retained_runs AS (
            SELECT run.id
            FROM runs run
            WHERE run.completed_at < $1
              AND run.status IN ($2, $3, $4, $5, $6)
              AND NOT EXISTS (
                  SELECT 1 FROM run_attempts attempt
                  WHERE attempt.run_id = run.id
                    AND (
                      attempt.status IN ('claimed', 'running')
                      OR (attempt.status = 'unknown' AND attempt.reconciled_at IS NULL)
                    )
              )
              AND NOT EXISTS (
                  SELECT 1 FROM project_runs project WHERE project.run_id = run.id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM audit_events event WHERE event.run_id = run.id
              )
            ORDER BY run.completed_at, run.id
            LIMIT $7
        )
        DELETE FROM runs
        USING retained_runs
        WHERE runs.id = retained_runs.id`, cutoff, terminal)
	if err != nil {
		return runs.RetentionResult{}, err
	}
	return runs.RetentionResult{
		RunsDeleted: deletedRuns, ProjectRunsDeleted: projects, OutputChunksDeleted: output,
	}, nil
}

func (s *Store) deleteRunChildrenInBatches(ctx context.Context, query string, cutoff time.Time, terminal []runs.Status) (int64, error) {
	var total int64
	for {
		opCtx, cancel := s.operationContext(ctx)
		result, err := s.db.ExecContext(opCtx, query,
			cutoff, terminal[0], terminal[1], terminal[2], terminal[3], terminal[4], retentionBatchSize,
		)
		cancel()
		if err != nil {
			return 0, err
		}
		deleted, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		total += deleted
		if deleted < retentionBatchSize {
			return total, nil
		}
	}
}
