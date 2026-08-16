// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/runatlantis/atlantis/server/core/drift"
	"github.com/runatlantis/atlantis/server/events/models"
)

const defaultHistoryPageLimit = 100

// HistoryStore persists append-only detection history and serves bounded drift
// UI queries over the authoritative latest-state table.
type HistoryStore struct {
	db               *sql.DB
	operationTimeout time.Duration
}

// NewHistoryStore creates a drift history adapter over an existing PostgreSQL
// pool. The caller retains ownership of db.
func NewHistoryStore(db *sql.DB, operationTimeout time.Duration) *HistoryStore {
	if operationTimeout <= 0 {
		operationTimeout = defaultOperationTimeout
	}
	return &HistoryStore{db: db, operationTimeout: operationTimeout}
}

// RecordDetection atomically inserts one immutable detection and all project
// outcomes. Artifact references are copied from the correlated generic Run.
func (s *HistoryStore) RecordDetection(ctx context.Context, record drift.DetectionRecord) error {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("starting PostgreSQL drift history transaction: %w", err)
	}
	defer tx.Rollback() // nolint: errcheck

	var runID any
	if record.Run.RunID != "" {
		runID = record.Run.RunID
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO drift_detection_runs (
    id, run_id, repository, display_repository, ref, base_branch,
    resolved_commit, status, started_at, completed_at, total_projects,
    projects_with_drift, failed_projects, locked_projects, skipped_projects
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`,
		record.Run.ID, runID, record.Run.Repository, record.Run.DisplayRepository,
		record.Run.Ref, record.Run.BaseBranch, record.Run.ResolvedCommit, record.Run.Status,
		normalizeTime(record.Run.StartedAt), normalizeTime(record.Run.CompletedAt),
		record.Run.TotalProjects, record.Run.ProjectsWithDrift, record.Run.FailedProjects,
		record.Run.LockedProjects, record.Run.SkippedProjects,
	)
	if err != nil {
		return fmt.Errorf("inserting PostgreSQL drift detection: %w", err)
	}
	for _, project := range record.Projects {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO drift_detection_results (
    detection_id, ordinal, project_name, directory, workspace, ref, base_branch,
    resolved_commit, outcome, has_drift, additions, changes, destructions,
    imports, forgets, summary, changes_outside, error, last_checked,
    last_successful_checked, plan_artifact_key, plan_artifact_checksum
)
SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
       $14, $15, $16, $17, $18, $19, $20,
       COALESCE($21, artifact.plan_artifact_key),
       COALESCE($22, artifact.plan_artifact_checksum)
FROM (SELECT 1) AS seed
LEFT JOIN LATERAL (
    SELECT plan_artifact_key, plan_artifact_checksum
    FROM project_runs
    WHERE run_id = $23 AND project_name = $3 AND directory = $4 AND workspace = $5
    LIMIT 1
) AS artifact ON true`,
			record.Run.ID, project.Ordinal, project.Project.ProjectName, project.Project.Path,
			project.Project.Workspace, project.Project.Ref, project.Project.BaseBranch,
			project.Project.ResolvedCommit, project.Outcome, project.Project.Drift.HasDrift,
			project.Project.Drift.ToAdd, project.Project.Drift.ToChange,
			project.Project.Drift.ToDestroy, project.Project.Drift.ToImport,
			project.Project.Drift.ToForget, project.Project.Drift.Summary,
			project.Project.Drift.ChangesOutside, project.Project.Error,
			normalizeTime(project.Project.LastChecked), successfulCheckedAt(project.Project),
			nullString(project.ArtifactKey),
			nullString(project.ArtifactChecksum), runID,
		); err != nil {
			return fmt.Errorf("inserting PostgreSQL drift detection project: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing PostgreSQL drift history: %w", err)
	}
	return nil
}

// ListCurrentStatus returns a bounded page from the authoritative latest-state
// table, enriched with the latest matching remediation attempt.
func (s *HistoryStore) ListCurrentStatus(ctx context.Context, filter drift.CurrentStatusFilter, page drift.HistoryPageRequest) (drift.CurrentStatusPage, error) {
	limit, offset := normalizeHistoryPage(page)
	where := []string{"1 = 1"}
	args := make([]any, 0, 8)
	appendFilter := func(predicate string, value any) {
		args = append(args, value)
		where = append(where, fmt.Sprintf(predicate, len(args)))
	}
	if filter.Repository != "" {
		args = append(args, filter.Repository)
		where = append(where, fmt.Sprintf("(ds.repository = $%d OR ds.repository LIKE '%%/' || $%d)", len(args), len(args)))
	}
	if filter.Ref != "" {
		appendFilter("ds.ref = $%d", filter.Ref)
	}
	if filter.Project != "" {
		appendFilter("ds.project_name = $%d", filter.Project)
	}
	if filter.Directory != "" {
		appendFilter("ds.directory = $%d", filter.Directory)
	}
	if filter.Workspace != "" {
		appendFilter("ds.workspace = $%d", filter.Workspace)
	}
	if filter.Outcome != "" {
		appendFilter(currentOutcomeSQL+" = $%d", filter.Outcome)
	}
	args = append(args, limit+1, offset)
	query := `
SELECT ds.repository, ds.project_name, ds.directory, ds.workspace, ds.ref,
       ds.base_branch, ds.resolved_commit, ds.detection_id, ds.has_drift,
       ds.additions, ds.changes, ds.destructions, ds.imports, ds.forgets,
       ds.summary, ds.changes_outside, ds.error, ds.last_checked,
       ds.last_successful_checked,
       ` + currentOutcomeSQL + ` AS outcome,
       remediation.id, remediation.status, remediation.started_at
FROM drift_status ds
LEFT JOIN LATERAL (
    SELECT r.id, r.status, r.started_at
    FROM drift_remediations r
    JOIN drift_remediation_projects p ON p.remediation_id = r.id
    WHERE r.storage_repository = ds.repository AND r.ref = ds.ref
      AND r.base_branch = ds.base_branch
      AND p.project_name = ds.project_name AND p.directory = ds.directory
      AND p.workspace = ds.workspace
    ORDER BY r.started_at DESC, r.id DESC
    LIMIT 1
) AS remediation ON true
WHERE ` + strings.Join(where, " AND ") + `
ORDER BY ds.last_successful_checked ASC NULLS FIRST, ds.last_checked ASC,
         ds.repository, ds.project_name, ds.directory, ds.workspace
LIMIT $` + fmt.Sprint(len(args)-1) + ` OFFSET $` + fmt.Sprint(len(args))

	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return drift.CurrentStatusPage{}, fmt.Errorf("querying PostgreSQL current drift status: %w", err)
	}
	defer rows.Close()
	projects := make([]drift.CurrentProjectStatus, 0, limit+1)
	for rows.Next() {
		var current drift.CurrentProjectStatus
		var remediationID, remediationStatus sql.NullString
		var remediationAt sql.NullTime
		var lastSuccessful sql.NullTime
		if err := rows.Scan(
			&current.Repository, &current.Project.ProjectName, &current.Project.Path,
			&current.Project.Workspace, &current.Project.Ref, &current.Project.BaseBranch,
			&current.Project.ResolvedCommit, &current.Project.DetectionID,
			&current.Project.Drift.HasDrift, &current.Project.Drift.ToAdd,
			&current.Project.Drift.ToChange, &current.Project.Drift.ToDestroy,
			&current.Project.Drift.ToImport, &current.Project.Drift.ToForget,
			&current.Project.Drift.Summary, &current.Project.Drift.ChangesOutside,
			&current.Project.Error, &current.Project.LastChecked, &lastSuccessful,
			&current.Outcome,
			&remediationID, &remediationStatus, &remediationAt,
		); err != nil {
			return drift.CurrentStatusPage{}, fmt.Errorf("scanning PostgreSQL current drift status: %w", err)
		}
		current.Project.LastChecked = normalizeTime(current.Project.LastChecked)
		if lastSuccessful.Valid {
			checked := normalizeTime(lastSuccessful.Time)
			current.Project.LastSuccessfulChecked = &checked
		}
		if remediationID.Valid {
			current.LastRemediationID = remediationID.String
			current.LastRemediationStatus = models.RemediationStatus(remediationStatus.String)
			at := normalizeTime(remediationAt.Time)
			current.LastRemediationAt = &at
		}
		projects = append(projects, current)
	}
	if err := rows.Err(); err != nil {
		return drift.CurrentStatusPage{}, fmt.Errorf("iterating PostgreSQL current drift status: %w", err)
	}
	hasMore := len(projects) > limit
	if hasMore {
		projects = projects[:limit]
	}
	return drift.CurrentStatusPage{Projects: projects, HasMore: hasMore}, nil
}

// ListDetections returns newest-first append-only detection runs.
func (s *HistoryStore) ListDetections(ctx context.Context, repository string, page drift.HistoryPageRequest) (drift.DetectionPage, error) {
	limit, offset := normalizeHistoryPage(page)
	query := detectionRunSelect
	args := make([]any, 0, 3)
	if repository != "" {
		query += " WHERE repository = $1 OR display_repository = $1 OR repository LIKE '%/' || $1"
		args = append(args, repository)
	}
	args = append(args, limit+1, offset)
	query += fmt.Sprintf(" ORDER BY completed_at DESC, id DESC LIMIT $%d OFFSET $%d", len(args)-1, len(args))

	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return drift.DetectionPage{}, fmt.Errorf("listing PostgreSQL drift detections: %w", err)
	}
	defer rows.Close()
	runs := make([]drift.DetectionRun, 0, limit+1)
	for rows.Next() {
		var run drift.DetectionRun
		if err := scanDetectionRun(rows, &run); err != nil {
			return drift.DetectionPage{}, err
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return drift.DetectionPage{}, fmt.Errorf("iterating PostgreSQL drift detections: %w", err)
	}
	hasMore := len(runs) > limit
	if hasMore {
		runs = runs[:limit]
	}
	return drift.DetectionPage{Runs: runs, HasMore: hasMore}, nil
}

// GetDetection returns one detection and a bounded project page.
func (s *HistoryStore) GetDetection(ctx context.Context, id string, page drift.HistoryPageRequest) (drift.DetectionRun, drift.DetectionPage, error) {
	ctx, cancel := s.operationContext(ctx)
	defer cancel()
	var run drift.DetectionRun
	err := scanDetectionRun(s.db.QueryRowContext(ctx, detectionRunSelect+" WHERE id = $1", id), &run)
	if errors.Is(err, sql.ErrNoRows) {
		return drift.DetectionRun{}, drift.DetectionPage{}, drift.ErrHistoryNotFound
	}
	if err != nil {
		return drift.DetectionRun{}, drift.DetectionPage{}, err
	}
	limit, offset := normalizeHistoryPage(page)
	rows, err := s.db.QueryContext(ctx, `
SELECT detection_id, ordinal, project_name, directory, workspace, ref,
       base_branch, resolved_commit, outcome, has_drift, additions, changes,
       destructions, imports, forgets, summary, changes_outside, error,
       last_checked, last_successful_checked, plan_artifact_key, plan_artifact_checksum
FROM drift_detection_results
WHERE detection_id = $1 ORDER BY ordinal LIMIT $2 OFFSET $3`, id, limit+1, offset)
	if err != nil {
		return drift.DetectionRun{}, drift.DetectionPage{}, fmt.Errorf("querying PostgreSQL drift detection projects: %w", err)
	}
	defer rows.Close()
	projects := make([]drift.DetectionProject, 0, limit+1)
	for rows.Next() {
		var project drift.DetectionProject
		var artifactKey, artifactChecksum sql.NullString
		var lastSuccessful sql.NullTime
		if err := rows.Scan(
			&project.DetectionID, &project.Ordinal, &project.Project.ProjectName,
			&project.Project.Path, &project.Project.Workspace, &project.Project.Ref,
			&project.Project.BaseBranch, &project.Project.ResolvedCommit, &project.Outcome,
			&project.Project.Drift.HasDrift, &project.Project.Drift.ToAdd,
			&project.Project.Drift.ToChange, &project.Project.Drift.ToDestroy,
			&project.Project.Drift.ToImport, &project.Project.Drift.ToForget,
			&project.Project.Drift.Summary, &project.Project.Drift.ChangesOutside,
			&project.Project.Error, &project.Project.LastChecked, &lastSuccessful,
			&artifactKey,
			&artifactChecksum,
		); err != nil {
			return drift.DetectionRun{}, drift.DetectionPage{}, fmt.Errorf("scanning PostgreSQL drift detection project: %w", err)
		}
		project.Project.LastChecked = normalizeTime(project.Project.LastChecked)
		if lastSuccessful.Valid {
			checked := normalizeTime(lastSuccessful.Time)
			project.Project.LastSuccessfulChecked = &checked
		}
		project.ArtifactKey = artifactKey.String
		project.ArtifactChecksum = artifactChecksum.String
		projects = append(projects, project)
	}
	if err := rows.Err(); err != nil {
		return drift.DetectionRun{}, drift.DetectionPage{}, fmt.Errorf("iterating PostgreSQL drift detection projects: %w", err)
	}
	hasMore := len(projects) > limit
	if hasMore {
		projects = projects[:limit]
	}
	return run, drift.DetectionPage{Projects: projects, HasMore: hasMore}, nil
}

const currentOutcomeSQL = `CASE
    WHEN ds.error <> '' AND (lower(ds.error) LIKE '%locked%' OR lower(ds.error) LIKE '% lock%' OR lower(ds.error) LIKE 'lock%') THEN 'locked'
    WHEN ds.error <> '' AND lower(ds.error) LIKE '%skip%' THEN 'skipped'
    WHEN ds.error <> '' THEN 'failed'
    WHEN ds.has_drift THEN 'drifted'
    ELSE 'clean'
END`

const detectionRunSelect = `
SELECT id, run_id, repository, display_repository, ref, base_branch,
       resolved_commit, status, started_at, completed_at, total_projects,
       projects_with_drift, failed_projects, locked_projects, skipped_projects
FROM drift_detection_runs`

func scanDetectionRun(scanner rowScanner, run *drift.DetectionRun) error {
	var runID sql.NullString
	if err := scanner.Scan(
		&run.ID, &runID, &run.Repository, &run.DisplayRepository, &run.Ref,
		&run.BaseBranch, &run.ResolvedCommit, &run.Status, &run.StartedAt,
		&run.CompletedAt, &run.TotalProjects, &run.ProjectsWithDrift,
		&run.FailedProjects, &run.LockedProjects, &run.SkippedProjects,
	); err != nil {
		return err
	}
	run.RunID = runID.String
	run.StartedAt = normalizeTime(run.StartedAt)
	run.CompletedAt = normalizeTime(run.CompletedAt)
	return nil
}

func normalizeHistoryPage(page drift.HistoryPageRequest) (int, int) {
	limit := page.Limit
	if limit <= 0 || limit > defaultHistoryPageLimit {
		limit = defaultHistoryPageLimit
	}
	offset := page.Offset
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}

func (s *HistoryStore) operationContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, s.operationTimeout)
}

func nullString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

var _ drift.HistoryStore = (*HistoryStore)(nil)
