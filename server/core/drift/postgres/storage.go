// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

// Package postgres implements durable drift status storage in PostgreSQL.
package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/runatlantis/atlantis/server/core/drift"
	"github.com/runatlantis/atlantis/server/events/models"
)

const defaultOperationTimeout = 10 * time.Second

// Storage implements drift.Storage using the PostgreSQL pool and migrations
// owned by the configured RunStore.
type Storage struct {
	db               *sql.DB
	operationTimeout time.Duration
}

// New creates a drift status adapter over an existing PostgreSQL pool. The
// caller retains ownership of db.
func New(db *sql.DB, operationTimeout time.Duration) *Storage {
	if operationTimeout <= 0 {
		operationTimeout = defaultOperationTimeout
	}
	return &Storage{db: db, operationTimeout: operationTimeout}
}

// Store saves the latest drift result for one complete project identity.
// PlanOutput is intentionally absent from both the query and schema.
func (s *Storage) Store(repository string, projectDrift models.ProjectDrift) error {
	ctx, cancel := s.operationContext()
	defer cancel()
	return storeDriftStatus(ctx, s.db, repository, projectDrift)
}

type driftStatusQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func storeDriftStatus(ctx context.Context, querier driftStatusQuerier, repository string, projectDrift models.ProjectDrift) error {
	var stored bool
	err := querier.QueryRowContext(ctx, `
WITH stored AS (
  INSERT INTO drift_status (
    identity_hash, repository_hash,
    repository, project_name, directory, workspace, ref, base_branch,
    resolved_commit, detection_id, has_drift, additions, changes,
    destructions, imports, forgets, summary, changes_outside, error, last_checked,
    last_successful_checked
  ) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
  )
  ON CONFLICT (identity_hash)
  DO UPDATE SET
    resolved_commit = EXCLUDED.resolved_commit,
    detection_id = EXCLUDED.detection_id,
    has_drift = EXCLUDED.has_drift,
    additions = EXCLUDED.additions,
    changes = EXCLUDED.changes,
    destructions = EXCLUDED.destructions,
    imports = EXCLUDED.imports,
    forgets = EXCLUDED.forgets,
    summary = EXCLUDED.summary,
    changes_outside = EXCLUDED.changes_outside,
    error = EXCLUDED.error,
    last_checked = EXCLUDED.last_checked,
    last_successful_checked = COALESCE(EXCLUDED.last_successful_checked, drift_status.last_successful_checked)
  WHERE drift_status.repository = EXCLUDED.repository
    AND drift_status.project_name = EXCLUDED.project_name
    AND drift_status.directory = EXCLUDED.directory
    AND drift_status.workspace = EXCLUDED.workspace
    AND drift_status.ref = EXCLUDED.ref
    AND drift_status.base_branch = EXCLUDED.base_branch
    AND drift_status.last_checked <= EXCLUDED.last_checked
  RETURNING TRUE
)
SELECT TRUE FROM stored`,
		driftIdentityDigest(repository, projectDrift),
		digestValues(repository),
		repository,
		projectDrift.ProjectName,
		projectDrift.Path,
		projectDrift.Workspace,
		projectDrift.Ref,
		projectDrift.BaseBranch,
		projectDrift.ResolvedCommit,
		projectDrift.DetectionID,
		projectDrift.Drift.HasDrift,
		projectDrift.Drift.ToAdd,
		projectDrift.Drift.ToChange,
		projectDrift.Drift.ToDestroy,
		projectDrift.Drift.ToImport,
		projectDrift.Drift.ToForget,
		projectDrift.Drift.Summary,
		projectDrift.Drift.ChangesOutside,
		projectDrift.Error,
		normalizeTime(projectDrift.LastChecked),
		successfulCheckedAt(projectDrift),
	).Scan(&stored)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("storing PostgreSQL drift status: %w", err)
	}
	if stored {
		return nil
	}
	var identityMatches bool
	err = querier.QueryRowContext(ctx, `SELECT EXISTS (
    SELECT 1 FROM drift_status
    WHERE identity_hash = $1
      AND repository = $2
      AND project_name = $3
      AND directory = $4
      AND workspace = $5
      AND ref = $6
      AND base_branch = $7
)`,
		driftIdentityDigest(repository, projectDrift), repository,
		projectDrift.ProjectName, projectDrift.Path, projectDrift.Workspace,
		projectDrift.Ref, projectDrift.BaseBranch,
	).Scan(&identityMatches)
	if err != nil {
		return fmt.Errorf("checking PostgreSQL drift status identity: %w", err)
	}
	if !identityMatches {
		return fmt.Errorf("storing PostgreSQL drift status: identity digest collision")
	}
	return nil
}

// Get retrieves the latest drift results for a repository.
func (s *Storage) Get(repository string, opts drift.GetOptions) ([]models.ProjectDrift, error) {
	query, args := buildSelect(repository, opts)
	ctx, cancel := s.operationContext()
	defer cancel()
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying PostgreSQL drift status: %w", err)
	}
	defer rows.Close()

	results := make([]models.ProjectDrift, 0)
	for rows.Next() {
		var projectDrift models.ProjectDrift
		if err := scanProjectDrift(rows, &projectDrift); err != nil {
			return nil, fmt.Errorf("scanning PostgreSQL drift status: %w", err)
		}
		results = append(results, projectDrift)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating PostgreSQL drift status: %w", err)
	}
	return results, nil
}

// Delete removes all repository drift status or every identity with the given
// project name.
func (s *Storage) Delete(repository string, projectName string) error {
	query := "DELETE FROM drift_status WHERE repository_hash = $1 AND repository = $2"
	args := []any{digestValues(repository), repository}
	if projectName != "" {
		query += " AND project_name = $3"
		args = append(args, projectName)
	}
	return s.execDelete(query, args)
}

// DeleteMatching removes drift status matching a non-empty filter.
func (s *Storage) DeleteMatching(repository string, opts drift.GetOptions) error {
	if opts == (drift.GetOptions{}) {
		return fmt.Errorf("at least one drift delete filter is required")
	}
	where, args := buildWhere(repository, opts)
	return s.execDelete("DELETE FROM drift_status "+where, args)
}

// DeleteObserved removes one exact identity only if the detection version is
// unchanged since reconciliation read it.
func (s *Storage) DeleteObserved(repository string, observed models.ProjectDrift) error {
	where, args := buildWhere(repository, drift.GetOptions{
		ProjectName: observed.ProjectName, Path: observed.Path, Workspace: observed.Workspace,
		Ref: observed.Ref, BaseBranch: observed.BaseBranch, Exact: true,
	})
	args = append(args, normalizeTime(observed.LastChecked), observed.DetectionID)
	query := fmt.Sprintf("DELETE FROM drift_status %s AND last_checked = $%d AND detection_id = $%d", where, len(args)-1, len(args))
	return s.execDelete(query, args)
}

// GetAll retrieves the latest drift results grouped by repository.
func (s *Storage) GetAll() (map[string][]models.ProjectDrift, error) {
	ctx, cancel := s.operationContext()
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `
SELECT repository, project_name, directory, workspace, ref, base_branch,
       resolved_commit, detection_id, has_drift, additions, changes,
       destructions, imports, forgets, summary, changes_outside, error, last_checked,
       last_successful_checked
FROM drift_status
ORDER BY repository, last_checked DESC, project_name, directory, workspace`)
	if err != nil {
		return nil, fmt.Errorf("querying all PostgreSQL drift status: %w", err)
	}
	defer rows.Close()

	results := make(map[string][]models.ProjectDrift)
	for rows.Next() {
		var repository string
		var projectDrift models.ProjectDrift
		var lastSuccessful sql.NullTime
		if err := rows.Scan(
			&repository,
			&projectDrift.ProjectName,
			&projectDrift.Path,
			&projectDrift.Workspace,
			&projectDrift.Ref,
			&projectDrift.BaseBranch,
			&projectDrift.ResolvedCommit,
			&projectDrift.DetectionID,
			&projectDrift.Drift.HasDrift,
			&projectDrift.Drift.ToAdd,
			&projectDrift.Drift.ToChange,
			&projectDrift.Drift.ToDestroy,
			&projectDrift.Drift.ToImport,
			&projectDrift.Drift.ToForget,
			&projectDrift.Drift.Summary,
			&projectDrift.Drift.ChangesOutside,
			&projectDrift.Error,
			&projectDrift.LastChecked,
			&lastSuccessful,
		); err != nil {
			return nil, fmt.Errorf("scanning all PostgreSQL drift status: %w", err)
		}
		projectDrift.LastChecked = normalizeTime(projectDrift.LastChecked)
		if lastSuccessful.Valid {
			checked := normalizeTime(lastSuccessful.Time)
			projectDrift.LastSuccessfulChecked = &checked
		}
		results[repository] = append(results[repository], projectDrift)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating all PostgreSQL drift status: %w", err)
	}
	return results, nil
}

func (s *Storage) execDelete(query string, args []any) error {
	ctx, cancel := s.operationContext()
	defer cancel()
	if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("deleting PostgreSQL drift status: %w", err)
	}
	return nil
}

func (s *Storage) operationContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.operationTimeout)
}

func buildSelect(repository string, opts drift.GetOptions) (string, []any) {
	where, args := buildWhere(repository, opts)
	return `
SELECT project_name, directory, workspace, ref, base_branch,
       resolved_commit, detection_id, has_drift, additions, changes,
       destructions, imports, forgets, summary, changes_outside, error, last_checked,
       last_successful_checked
FROM drift_status ` + where + `
ORDER BY last_checked DESC, project_name, directory, workspace`, args
}

func buildWhere(repository string, opts drift.GetOptions) (string, []any) {
	clauses := []string{"WHERE repository_hash = $1", "repository = $2"}
	args := []any{digestValues(repository), repository}
	appendFilter := func(column string, value any) {
		args = append(args, value)
		clauses = append(clauses, fmt.Sprintf("%s = $%d", column, len(args)))
	}
	if opts.Exact {
		appendFilter("project_name", opts.ProjectName)
		appendFilter("directory", opts.Path)
		appendFilter("workspace", opts.Workspace)
		appendFilter("ref", opts.Ref)
		appendFilter("base_branch", opts.BaseBranch)
	} else {
		if opts.ProjectName != "" {
			appendFilter("project_name", opts.ProjectName)
		}
		if opts.Path != "" {
			appendFilter("directory", opts.Path)
		}
		if opts.Workspace != "" {
			appendFilter("workspace", opts.Workspace)
		}
		if opts.Ref != "" {
			appendFilter("ref", opts.Ref)
		}
		if opts.BaseBranch != "" {
			appendFilter("base_branch", opts.BaseBranch)
		}
	}
	if opts.MaxAge > 0 {
		args = append(args, normalizeTime(time.Now().Add(-opts.MaxAge)))
		clauses = append(clauses, fmt.Sprintf("last_checked >= $%d", len(args)))
	}
	return strings.Join(clauses, " AND "), args
}

func driftIdentityDigest(repository string, projectDrift models.ProjectDrift) []byte {
	return digestValues(
		repository, projectDrift.ProjectName, projectDrift.Path,
		projectDrift.Workspace, projectDrift.Ref, projectDrift.BaseBranch,
	)
}

func digestValues(values ...string) []byte {
	digest := sha256.New()
	var size [8]byte
	for _, value := range values {
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = digest.Write(size[:])
		_, _ = digest.Write([]byte(value))
	}
	return digest.Sum(nil)
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanProjectDrift(row rowScanner, projectDrift *models.ProjectDrift) error {
	var lastSuccessful sql.NullTime
	if err := row.Scan(
		&projectDrift.ProjectName,
		&projectDrift.Path,
		&projectDrift.Workspace,
		&projectDrift.Ref,
		&projectDrift.BaseBranch,
		&projectDrift.ResolvedCommit,
		&projectDrift.DetectionID,
		&projectDrift.Drift.HasDrift,
		&projectDrift.Drift.ToAdd,
		&projectDrift.Drift.ToChange,
		&projectDrift.Drift.ToDestroy,
		&projectDrift.Drift.ToImport,
		&projectDrift.Drift.ToForget,
		&projectDrift.Drift.Summary,
		&projectDrift.Drift.ChangesOutside,
		&projectDrift.Error,
		&projectDrift.LastChecked,
		&lastSuccessful,
	); err != nil {
		return err
	}
	projectDrift.LastChecked = normalizeTime(projectDrift.LastChecked)
	if lastSuccessful.Valid {
		checked := normalizeTime(lastSuccessful.Time)
		projectDrift.LastSuccessfulChecked = &checked
	}
	return nil
}

func successfulCheckedAt(projectDrift models.ProjectDrift) any {
	if projectDrift.Error == "" {
		return normalizeTime(projectDrift.LastChecked)
	}
	if projectDrift.LastSuccessfulChecked != nil {
		return normalizeTime(*projectDrift.LastSuccessfulChecked)
	}
	return nil
}

func normalizeTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}

var _ drift.Storage = (*Storage)(nil)
