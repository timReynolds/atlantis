// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/runatlantis/atlantis/server/core/runs"
)

// RecordProjectPlanArtifact stores the expected object identity before S3
// upload. A later successful plan step for the same ProjectRun (for example a
// custom apply-stage workflow that re-plans before applying) replaces the
// prior expectation rather than conflicting with it, since both writes
// originate from the same authorized execution for this exact ProjectRun.
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
        WHERE id = $1`,
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
	if updated == 0 {
		return fmt.Errorf("recording project plan artifact for %s: %w", update.ProjectRunID, runs.ErrNotFound)
	}
	return nil
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
		// The synthetic (no-PR) path already has the caller's exact ProjectRun
		// identity, which uniquely pins the row. Do not additionally require
		// r.head_sha to match: for a symbolic ref such as "main", the Run may
		// have persisted the literal ref while the checkout resolved a
		// different commit, and comparing them here would reject an artifact
		// that was correctly recorded against this exact ProjectRun.
		return scanPlanArtifactExpectation(s.db.QueryRowContext(opCtx, `SELECT
                r.id, p.id, p.plan_artifact_key, p.plan_artifact_checksum,
                p.plan_artifact_created_at, p.plan_artifact_expires_at,
                p.plan_repo_config_version, p.plan_workflow_checksum
            FROM project_runs p JOIN runs r ON r.id = p.run_id
            WHERE p.id = $1 AND r.repository = $2 AND r.pull_number IS NULL
              AND r.command IN ($3, $4, $5)
              AND p.project_name = $6 AND p.directory = $7 AND p.workspace = $8
              AND p.plan_artifact_key IS NOT NULL
              AND p.plan_repo_config_version IS NOT NULL`,
			lookup.ProjectRunID, lookup.Repository,
			runs.CommandPlan, runs.CommandApply, runs.CommandDriftRemediation,
			lookup.ProjectName, lookup.Directory, lookup.Workspace))
	}

	return scanPlanArtifactExpectation(s.db.QueryRowContext(opCtx, `SELECT
            r.id, p.id, p.plan_artifact_key, p.plan_artifact_checksum,
            p.plan_artifact_created_at, p.plan_artifact_expires_at,
            p.plan_repo_config_version, p.plan_workflow_checksum
        FROM project_runs p JOIN runs r ON r.id = p.run_id
        WHERE r.repository = $1 AND r.pull_number = $2 AND r.head_sha = $3
          AND r.command IN ($4, $5, $6) AND p.project_name = $7 AND p.directory = $8
          AND p.workspace = $9 AND p.plan_artifact_key IS NOT NULL
          AND p.plan_repo_config_version IS NOT NULL
        ORDER BY p.plan_artifact_created_at DESC, p.id DESC LIMIT 1`,
		lookup.Repository, int64(lookup.PullNumber), lookup.HeadSHA,
		runs.CommandPlan, runs.CommandApply, runs.CommandDriftRemediation,
		lookup.ProjectName, lookup.Directory, lookup.Workspace))
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
