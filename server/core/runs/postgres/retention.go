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
	if policy.RunMetadataBefore == nil && policy.OutputBefore == nil && policy.AuditEventsBefore == nil {
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
	var total runs.RetentionResult
	for {
		opCtx, cancel := s.operationContext(ctx)
		var batch runs.RetentionResult
		err := s.db.QueryRowContext(opCtx, `WITH retained_runs AS (
            SELECT id FROM runs
            WHERE completed_at < $1
              AND status IN ($2, $3, $4, $5, $6)
            ORDER BY completed_at, id
            LIMIT $7
        ), retained_counts AS (
            SELECT count(DISTINCT project_runs.id) AS project_runs,
                   count(run_output_chunks.id) AS output_chunks
            FROM retained_runs
            LEFT JOIN project_runs ON project_runs.run_id = retained_runs.id
            LEFT JOIN run_output_chunks ON run_output_chunks.project_run_id = project_runs.id
        ), deleted_runs AS (
            DELETE FROM runs
            USING retained_runs
            WHERE runs.id = retained_runs.id
            RETURNING runs.id
        )
        SELECT (SELECT count(*) FROM deleted_runs), project_runs, output_chunks
        FROM retained_counts`,
			cutoff, terminal[0], terminal[1], terminal[2], terminal[3], terminal[4], retentionBatchSize,
		).Scan(&batch.RunsDeleted, &batch.ProjectRunsDeleted, &batch.OutputChunksDeleted)
		cancel()
		if err != nil {
			return runs.RetentionResult{}, err
		}
		total.RunsDeleted += batch.RunsDeleted
		total.ProjectRunsDeleted += batch.ProjectRunsDeleted
		total.OutputChunksDeleted += batch.OutputChunksDeleted
		if batch.RunsDeleted < retentionBatchSize {
			return total, nil
		}
	}
}
