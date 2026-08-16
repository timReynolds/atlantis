// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/runatlantis/atlantis/server/core/runs"
)

// RecordProjectPlanArtifact stores the expected object identity before S3
// upload. Replays are idempotent, while a different object or workflow for the
// same ProjectRun conflicts.
func (s *Store) RecordProjectPlanArtifact(ctx context.Context, update runs.ProjectPlanArtifactUpdate) error {
	if err := update.Validate(); err != nil {
		return fmt.Errorf("validating project plan artifact: %w", err)
	}
	update.Artifact.CreatedAt = normalizeTime(update.Artifact.CreatedAt)
	update.Artifact.ExpiresAt = normalizeTimePtr(update.Artifact.ExpiresAt)
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()

	result, err := s.db.ExecContext(opCtx, `UPDATE project_runs SET
        plan_artifact_key = $2, plan_artifact_checksum = $3,
        plan_artifact_created_at = $4, plan_artifact_expires_at = $5,
        plan_repo_config_version = $6, plan_workflow_checksum = $7
        WHERE id = $1 AND plan_artifact_key IS NULL`,
		update.ProjectRunID, update.Artifact.Key, update.Artifact.Checksum,
		update.Artifact.CreatedAt, update.Artifact.ExpiresAt,
		update.Identity.RepoConfigVersion, update.Identity.WorkflowChecksum)
	if err != nil {
		return fmt.Errorf("recording project plan artifact: %w", err)
	}
	updated, err := rowsAffected(result)
	if err != nil {
		return fmt.Errorf("checking recorded project plan artifact: %w", err)
	}
	if updated == 1 {
		return nil
	}
	existing, err := findPlanArtifactByProjectRun(opCtx, s.db, update.ProjectRunID)
	if err != nil {
		return err
	}
	if existing.ProjectRunID == update.ProjectRunID &&
		reflect.DeepEqual(existing.Artifact, update.Artifact) && existing.Identity == update.Identity {
		return nil
	}
	return fmt.Errorf("recording project plan artifact for %s: %w", update.ProjectRunID, runs.ErrConflict)
}

// FindPlanArtifact returns the newest independent PostgreSQL expectation for
// an exact project at an exact pull-request commit.
func (s *Store) FindPlanArtifact(ctx context.Context, lookup runs.PlanArtifactLookup) (runs.PlanArtifactExpectation, error) {
	if err := lookup.Validate(); err != nil {
		return runs.PlanArtifactExpectation{}, fmt.Errorf("validating plan artifact lookup: %w", err)
	}
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()
	if lookup.PullNumber < 0 {
		return scanPlanArtifactExpectation(s.db.QueryRowContext(opCtx, `SELECT
                r.id, p.id, p.plan_artifact_key, p.plan_artifact_checksum,
                p.plan_artifact_created_at, p.plan_artifact_expires_at,
                p.plan_repo_config_version, p.plan_workflow_checksum
            FROM project_runs p JOIN runs r ON r.id = p.run_id
            WHERE p.id = $1 AND r.repository = $2 AND r.pull_number IS NULL
              AND r.head_sha = $3 AND r.command IN ($4, $5)
              AND p.project_name = $6 AND p.directory = $7 AND p.workspace = $8
              AND p.plan_artifact_key IS NOT NULL
              AND p.plan_repo_config_version IS NOT NULL`,
			lookup.ProjectRunID, lookup.Repository, lookup.HeadSHA,
			runs.CommandPlan, runs.CommandApply, lookup.ProjectName, lookup.Directory, lookup.Workspace))
	}

	return scanPlanArtifactExpectation(s.db.QueryRowContext(opCtx, `SELECT
            r.id, p.id, p.plan_artifact_key, p.plan_artifact_checksum,
            p.plan_artifact_created_at, p.plan_artifact_expires_at,
            p.plan_repo_config_version, p.plan_workflow_checksum
        FROM project_runs p JOIN runs r ON r.id = p.run_id
        WHERE r.repository = $1 AND r.pull_number = $2 AND r.head_sha = $3
          AND r.command IN ($4, $5) AND p.project_name = $6 AND p.directory = $7
          AND p.workspace = $8 AND p.plan_artifact_key IS NOT NULL
          AND p.plan_repo_config_version IS NOT NULL
        ORDER BY p.plan_artifact_created_at DESC, p.id DESC LIMIT 1`,
		lookup.Repository, int64(lookup.PullNumber), lookup.HeadSHA,
		runs.CommandPlan, runs.CommandApply, lookup.ProjectName, lookup.Directory, lookup.Workspace))
}

func findPlanArtifactByProjectRun(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, projectRunID runs.ID) (runs.PlanArtifactExpectation, error) {
	return scanPlanArtifactExpectation(queryer.QueryRowContext(ctx, `SELECT
            p.run_id, p.id, p.plan_artifact_key, p.plan_artifact_checksum,
            p.plan_artifact_created_at, p.plan_artifact_expires_at,
            p.plan_repo_config_version, p.plan_workflow_checksum
        FROM project_runs p WHERE p.id = $1 AND p.plan_artifact_key IS NOT NULL`, projectRunID))
}

func scanPlanArtifactExpectation(row rowScanner) (runs.PlanArtifactExpectation, error) {
	var result runs.PlanArtifactExpectation
	var expiresAt sql.NullTime
	if err := row.Scan(
		&result.RunID, &result.ProjectRunID, &result.Artifact.Key,
		&result.Artifact.Checksum, &result.Artifact.CreatedAt, &expiresAt,
		&result.Identity.RepoConfigVersion, &result.Identity.WorkflowChecksum,
	); err != nil {
		return runs.PlanArtifactExpectation{}, mapLookupError("scanning plan artifact expectation", err)
	}
	result.Artifact.CreatedAt = normalizeTime(result.Artifact.CreatedAt)
	result.Artifact.ExpiresAt = timeFromNull(expiresAt)
	if err := (runs.ProjectPlanArtifactUpdate{
		ProjectRunID: result.ProjectRunID, Artifact: result.Artifact, Identity: result.Identity,
	}).Validate(); err != nil {
		return runs.PlanArtifactExpectation{}, fmt.Errorf("validating stored plan artifact expectation: %w", err)
	}
	return result, nil
}
