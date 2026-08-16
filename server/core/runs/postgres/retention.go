// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/runatlantis/atlantis/server/core/runs"
)

func (s *Store) ApplyRetention(ctx context.Context, policy runs.RetentionPolicy) (runs.RetentionResult, error) {
	if policy.RunMetadataBefore == nil && policy.OutputBefore == nil && policy.AuditEventsBefore == nil {
		return runs.RetentionResult{}, nil
	}
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()
	tx, err := s.db.BeginTx(opCtx, nil)
	if err != nil {
		return runs.RetentionResult{}, fmt.Errorf("starting run history retention: %w", err)
	}
	defer tx.Rollback() // nolint: errcheck

	var result runs.RetentionResult
	if policy.OutputBefore != nil {
		deleted, err := deleteBefore(opCtx, tx,
			"DELETE FROM run_output_chunks WHERE created_at < $1",
			normalizeTime(*policy.OutputBefore),
		)
		if err != nil {
			return runs.RetentionResult{}, fmt.Errorf("deleting retained run output: %w", err)
		}
		result.OutputChunksDeleted += deleted
	}
	if policy.AuditEventsBefore != nil {
		deleted, err := deleteBefore(opCtx, tx,
			"DELETE FROM audit_events WHERE created_at < $1",
			normalizeTime(*policy.AuditEventsBefore),
		)
		if err != nil {
			return runs.RetentionResult{}, fmt.Errorf("deleting retained audit events: %w", err)
		}
		result.AuditEventsDeleted = deleted
	}
	if policy.RunMetadataBefore != nil {
		cutoff := normalizeTime(*policy.RunMetadataBefore)
		terminal := []runs.Status{
			runs.StatusSucceeded, runs.StatusFailed, runs.StatusPartial,
			runs.StatusCancelled, runs.StatusSkipped,
		}
		var projectRunsDeleted, outputChunksDeleted int64
		if err := tx.QueryRowContext(opCtx, `SELECT
            count(DISTINCT project_runs.id), count(run_output_chunks.id)
            FROM runs
            LEFT JOIN project_runs ON project_runs.run_id = runs.id
            LEFT JOIN run_output_chunks ON run_output_chunks.project_run_id = project_runs.id
            WHERE runs.completed_at < $1
              AND runs.status IN ($2, $3, $4, $5, $6)`,
			cutoff, terminal[0], terminal[1], terminal[2], terminal[3], terminal[4],
		).Scan(&projectRunsDeleted, &outputChunksDeleted); err != nil {
			return runs.RetentionResult{}, fmt.Errorf("counting retained run records: %w", err)
		}
		result.ProjectRunsDeleted = projectRunsDeleted
		result.OutputChunksDeleted += outputChunksDeleted
		deleted, err := deleteBefore(opCtx, tx, `DELETE FROM runs
            WHERE completed_at < $1 AND status IN ($2, $3, $4, $5, $6)`,
			cutoff, terminal[0], terminal[1], terminal[2], terminal[3], terminal[4],
		)
		if err != nil {
			return runs.RetentionResult{}, fmt.Errorf("deleting retained runs: %w", err)
		}
		result.RunsDeleted = deleted
	}
	if err := tx.Commit(); err != nil {
		return runs.RetentionResult{}, fmt.Errorf("committing run history retention: %w", err)
	}
	return result, nil
}

func deleteBefore(ctx context.Context, tx *sql.Tx, query string, args ...any) (int64, error) {
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
