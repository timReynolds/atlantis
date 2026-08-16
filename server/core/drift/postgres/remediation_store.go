// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/runatlantis/atlantis/server/core/drift"
	"github.com/runatlantis/atlantis/server/events/models"
)

// RemediationStore persists remediation summaries and project outcomes. Raw
// plan/apply output stays in the permissioned generic RunStore output tables.
type RemediationStore struct {
	db               *sql.DB
	operationTimeout time.Duration
}

// NewRemediationStore creates a remediation result adapter over an existing
// PostgreSQL pool. The caller retains ownership of db.
func NewRemediationStore(db *sql.DB, operationTimeout time.Duration) *RemediationStore {
	if operationTimeout <= 0 {
		operationTimeout = defaultOperationTimeout
	}
	return &RemediationStore{db: db, operationTimeout: operationTimeout}
}

// Put atomically upserts a remediation and replaces its current project list.
func (s *RemediationStore) Put(result *models.RemediationResult) error {
	if result == nil {
		return fmt.Errorf("remediation result is required")
	}
	ctx, cancel := s.operationContext()
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting PostgreSQL remediation transaction: %w", err)
	}
	defer tx.Rollback() // nolint: errcheck

	var runID any
	if result.RunID != "" {
		runID = result.RunID
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO drift_remediations (
    id, run_id, repository, storage_repository, ref, base_branch, action, status,
    started_at, completed_at, total_projects, success_count, failure_count, error
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
ON CONFLICT (id) DO UPDATE SET
    run_id = EXCLUDED.run_id,
    repository = EXCLUDED.repository,
    storage_repository = EXCLUDED.storage_repository,
    ref = EXCLUDED.ref,
    base_branch = EXCLUDED.base_branch,
    action = EXCLUDED.action,
    status = EXCLUDED.status,
    started_at = EXCLUDED.started_at,
    completed_at = EXCLUDED.completed_at,
    total_projects = EXCLUDED.total_projects,
    success_count = EXCLUDED.success_count,
    failure_count = EXCLUDED.failure_count,
    error = EXCLUDED.error`,
		result.ID, runID, result.Repository, remediationRepositoryKey(result), result.Ref, result.BaseBranch,
		result.Action, result.Status, normalizeTime(result.StartedAt), normalizeTimePtr(result.CompletedAt),
		result.TotalProjects, result.SuccessCount, result.FailureCount, result.Error,
	)
	if err != nil {
		return fmt.Errorf("upserting PostgreSQL remediation: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM drift_remediation_projects WHERE remediation_id = $1", result.ID); err != nil {
		return fmt.Errorf("replacing PostgreSQL remediation projects: %w", err)
	}
	for ordinal, project := range result.Projects {
		before, err := marshalOptionalSummary(project.DriftBefore)
		if err != nil {
			return err
		}
		after, err := marshalOptionalSummary(project.DriftAfter)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO drift_remediation_projects (
    remediation_id, ordinal, project_name, directory, workspace, status, error,
    drift_before, drift_after
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
			result.ID, ordinal, project.ProjectName, project.Path, project.Workspace,
			project.Status, project.Error, before, after,
		); err != nil {
			return fmt.Errorf("inserting PostgreSQL remediation project: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing PostgreSQL remediation: %w", err)
	}
	return nil
}

// GetResult retrieves one remediation and its project summaries.
func (s *RemediationStore) GetResult(id string) (*models.RemediationResult, error) {
	ctx, cancel := s.operationContext()
	defer cancel()
	result := &models.RemediationResult{}
	var runID sql.NullString
	err := s.db.QueryRowContext(ctx, `
SELECT id, run_id, repository, storage_repository, ref, base_branch, action, status,
       started_at, completed_at, total_projects, success_count, failure_count, error
FROM drift_remediations WHERE id = $1`, id).Scan(
		&result.ID, &runID, &result.Repository, &result.StorageRepository, &result.Ref, &result.BaseBranch,
		&result.Action, &result.Status, &result.StartedAt, &result.CompletedAt,
		&result.TotalProjects, &result.SuccessCount, &result.FailureCount, &result.Error,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("remediation result not found: %s", id)
	}
	if err != nil {
		return nil, fmt.Errorf("querying PostgreSQL remediation: %w", err)
	}
	if runID.Valid {
		result.RunID = runID.String
	}
	result.StartedAt = normalizeTime(result.StartedAt)
	result.CompletedAt = normalizeTimePtr(result.CompletedAt)
	rows, err := s.db.QueryContext(ctx, `
SELECT project_name, directory, workspace, status, error, drift_before, drift_after
FROM drift_remediation_projects WHERE remediation_id = $1 ORDER BY ordinal`, id)
	if err != nil {
		return nil, fmt.Errorf("querying PostgreSQL remediation projects: %w", err)
	}
	defer rows.Close()
	result.Projects = make([]models.ProjectRemediationResult, 0)
	for rows.Next() {
		var project models.ProjectRemediationResult
		var before, after []byte
		if err := rows.Scan(
			&project.ProjectName, &project.Path, &project.Workspace, &project.Status,
			&project.Error, &before, &after,
		); err != nil {
			return nil, fmt.Errorf("scanning PostgreSQL remediation project: %w", err)
		}
		if err := unmarshalOptionalSummary(before, &project.DriftBefore); err != nil {
			return nil, err
		}
		if err := unmarshalOptionalSummary(after, &project.DriftAfter); err != nil {
			return nil, err
		}
		result.Projects = append(result.Projects, project)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating PostgreSQL remediation projects: %w", err)
	}
	return result, nil
}

// ListResults returns remediation results newest first for one repository key.
func (s *RemediationStore) ListResults(repository string, limit int) ([]*models.RemediationResult, error) {
	ctx, cancel := s.operationContext()
	defer cancel()
	query := "SELECT id FROM drift_remediations WHERE storage_repository = $1 ORDER BY started_at DESC, id DESC"
	args := []any{repository}
	if limit > 0 {
		query += " LIMIT $2"
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("listing PostgreSQL remediations: %w", err)
	}
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scanning PostgreSQL remediation ID: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close() // nolint: errcheck
		return nil, fmt.Errorf("iterating PostgreSQL remediation IDs: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("closing PostgreSQL remediation list: %w", err)
	}
	results := make([]*models.RemediationResult, 0, len(ids))
	for _, id := range ids {
		result, err := s.GetResult(id)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func (s *RemediationStore) operationContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.operationTimeout)
}

func remediationRepositoryKey(result *models.RemediationResult) string {
	if result.StorageRepository != "" {
		return result.StorageRepository
	}
	return result.Repository
}

func marshalOptionalSummary(summary *models.DriftSummary) (any, error) {
	if summary == nil {
		return nil, nil
	}
	value, err := json.Marshal(summary)
	if err != nil {
		return nil, fmt.Errorf("marshaling remediation drift summary: %w", err)
	}
	return value, nil
}

func unmarshalOptionalSummary(value []byte, destination **models.DriftSummary) error {
	if len(value) == 0 {
		return nil
	}
	var summary models.DriftSummary
	if err := json.Unmarshal(value, &summary); err != nil {
		return fmt.Errorf("unmarshaling remediation drift summary: %w", err)
	}
	*destination = &summary
	return nil
}

func normalizeTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	normalized := normalizeTime(*value)
	return &normalized
}

var _ drift.RemediationResultStore = (*RemediationStore)(nil)
