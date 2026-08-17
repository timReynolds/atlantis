// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
)

const (
	maxOutputChunkBytes = 256 * 1024
	maxOutputBatchSize  = 100
)

func (s *Store) CreateRun(ctx context.Context, run runs.Run) error {
	if err := run.Validate(); err != nil {
		return fmt.Errorf("validating run: %w", err)
	}
	run = normalizeRun(run)
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()

	result, err := s.db.ExecContext(opCtx, `INSERT INTO runs (
        id, repository, pull_number, command, trigger, actor, base_ref, head_ref,
        head_sha, status, created_at, started_at, completed_at, metadata
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
    ON CONFLICT DO NOTHING`,
		run.ID, run.Repository, intPointerValue(run.PullNumber), run.Command, run.Trigger,
		run.Actor, run.BaseRef, run.HeadRef, run.HeadSHA, run.Status, run.CreatedAt,
		run.StartedAt, run.CompletedAt, []byte(run.Metadata),
	)
	if err != nil {
		return fmt.Errorf("creating run: %w", err)
	}
	inserted, err := rowsAffected(result)
	if err != nil {
		return fmt.Errorf("checking created run: %w", err)
	}
	if inserted == 1 {
		return nil
	}

	existing, err := s.getRun(opCtx, run.ID)
	if err != nil {
		if errors.Is(err, runs.ErrNotFound) {
			return fmt.Errorf("creating run: %w", runs.ErrConflict)
		}
		return err
	}
	if !reflect.DeepEqual(existing, run) {
		return fmt.Errorf("creating run %s: %w", run.ID, runs.ErrConflict)
	}
	return nil
}

func (s *Store) StartRun(ctx context.Context, id runs.ID, startedAt time.Time) error {
	if _, err := runs.ParseID(string(id)); err != nil {
		return fmt.Errorf("validating run ID: %w", err)
	}
	if startedAt.IsZero() {
		return fmt.Errorf("run start time is required")
	}
	startedAt = normalizeTime(startedAt)
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()

	result, err := s.db.ExecContext(opCtx, `UPDATE runs
        SET status = $2, started_at = $3
        WHERE id = $1 AND status = $4 AND started_at IS NULL`,
		id, runs.StatusRunning, startedAt, runs.StatusPending,
	)
	if err != nil {
		return fmt.Errorf("starting run: %w", err)
	}
	updated, err := rowsAffected(result)
	if err != nil {
		return fmt.Errorf("checking started run: %w", err)
	}
	if updated == 1 {
		return nil
	}

	var status runs.Status
	var existing sql.NullTime
	if err := s.db.QueryRowContext(opCtx, "SELECT status, started_at FROM runs WHERE id = $1", id).Scan(&status, &existing); err != nil {
		return mapLookupError("reading run after start", err)
	}
	if status == runs.StatusRunning && existing.Valid && normalizeTime(existing.Time).Equal(startedAt) {
		return nil
	}
	return fmt.Errorf("starting run %s from status %q: %w", id, status, runs.ErrConflict)
}

func (s *Store) CompleteRun(ctx context.Context, completion runs.RunCompletion) error {
	if err := completion.Validate(); err != nil {
		return fmt.Errorf("validating run completion: %w", err)
	}
	completion.CompletedAt = normalizeTime(completion.CompletedAt)
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()

	result, err := s.db.ExecContext(opCtx, `UPDATE runs
        SET status = $2, completed_at = $3
        WHERE id = $1 AND status = $4 AND started_at IS NOT NULL AND completed_at IS NULL`,
		completion.ID, completion.Status, completion.CompletedAt, runs.StatusRunning,
	)
	if err != nil {
		return fmt.Errorf("completing run: %w", err)
	}
	updated, err := rowsAffected(result)
	if err != nil {
		return fmt.Errorf("checking completed run: %w", err)
	}
	if updated == 1 {
		return nil
	}

	var status runs.Status
	var existing sql.NullTime
	if err := s.db.QueryRowContext(opCtx, "SELECT status, completed_at FROM runs WHERE id = $1", completion.ID).Scan(&status, &existing); err != nil {
		return mapLookupError("reading run after completion", err)
	}
	if status == completion.Status && existing.Valid && normalizeTime(existing.Time).Equal(completion.CompletedAt) {
		return nil
	}
	return fmt.Errorf("completing run %s from status %q: %w", completion.ID, status, runs.ErrConflict)
}

func (s *Store) CreateProjectRun(ctx context.Context, projectRun runs.ProjectRun) error {
	if err := projectRun.Validate(); err != nil {
		return fmt.Errorf("validating project run: %w", err)
	}
	projectRun = normalizeProjectRun(projectRun)
	artifact := artifactValues(projectRun.PlanArtifact)
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()

	result, err := s.db.ExecContext(opCtx, `INSERT INTO project_runs (
        id, run_id, attempt_id, project_name, directory, workspace, status, additions, changes,
        destructions, imports, forgets, started_at, completed_at, error_summary,
        plan_artifact_key, plan_artifact_checksum, plan_artifact_created_at,
        plan_artifact_expires_at, metadata
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
        $14, $15, $16, $17, $18, $19, $20)
    ON CONFLICT DO NOTHING`,
		projectRun.ID, projectRun.RunID, projectRun.AttemptID, projectRun.ProjectName,
		projectRun.Directory, projectRun.Workspace, projectRun.Status, projectRun.Additions,
		projectRun.Changes, projectRun.Destructions, projectRun.Imports, projectRun.Forgets,
		projectRun.StartedAt, projectRun.CompletedAt, projectRun.ErrorSummary, artifact.key,
		artifact.checksum, artifact.createdAt, artifact.expiresAt, []byte(projectRun.Metadata),
	)
	if err != nil {
		return fmt.Errorf("creating project run: %w", err)
	}
	inserted, err := rowsAffected(result)
	if err != nil {
		return fmt.Errorf("checking created project run: %w", err)
	}
	if inserted == 1 {
		return nil
	}

	existing, err := s.getProjectRun(opCtx, projectRun.ID)
	if err != nil {
		if errors.Is(err, runs.ErrNotFound) {
			return fmt.Errorf("creating project run: %w", runs.ErrConflict)
		}
		return err
	}
	if !reflect.DeepEqual(existing, projectRun) {
		return fmt.Errorf("creating project run %s: %w", projectRun.ID, runs.ErrConflict)
	}
	return nil
}

func (s *Store) StartProjectRun(ctx context.Context, id runs.ID, startedAt time.Time) error {
	if _, err := runs.ParseID(string(id)); err != nil {
		return fmt.Errorf("validating project run ID: %w", err)
	}
	if startedAt.IsZero() {
		return fmt.Errorf("project run start time is required")
	}
	startedAt = normalizeTime(startedAt)
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()

	result, err := s.db.ExecContext(opCtx, `UPDATE project_runs
        SET status = $2, started_at = $3
        WHERE id = $1 AND status = $4 AND started_at IS NULL`,
		id, runs.StatusRunning, startedAt, runs.StatusPending,
	)
	if err != nil {
		return fmt.Errorf("starting project run: %w", err)
	}
	updated, err := rowsAffected(result)
	if err != nil {
		return fmt.Errorf("checking started project run: %w", err)
	}
	if updated == 1 {
		return nil
	}

	var status runs.Status
	var existing sql.NullTime
	if err := s.db.QueryRowContext(opCtx, "SELECT status, started_at FROM project_runs WHERE id = $1", id).Scan(&status, &existing); err != nil {
		return mapLookupError("reading project run after start", err)
	}
	if status == runs.StatusRunning && existing.Valid && normalizeTime(existing.Time).Equal(startedAt) {
		return nil
	}
	return fmt.Errorf("starting project run %s from status %q: %w", id, status, runs.ErrConflict)
}

func (s *Store) CompleteProjectRun(ctx context.Context, completion runs.ProjectRunCompletion) error {
	if err := completion.Validate(); err != nil {
		return fmt.Errorf("validating project run completion: %w", err)
	}
	completion = normalizeProjectRunCompletion(completion)
	artifact := artifactValues(completion.PlanArtifact)
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()

	result, err := s.db.ExecContext(opCtx, `UPDATE project_runs
        SET status = $2, additions = $3, changes = $4, destructions = $5, imports = $6,
            forgets = $7, completed_at = $8, error_summary = $9,
            plan_artifact_key = $10, plan_artifact_checksum = $11,
            plan_artifact_created_at = $12, plan_artifact_expires_at = $13,
            metadata = $14
        WHERE id = $1
          AND ((status = $15 AND started_at IS NOT NULL) OR
               (status = $16 AND $2 IN ($17, $18)))
          AND completed_at IS NULL`,
		completion.ID, completion.Status, completion.Additions, completion.Changes,
		completion.Destructions, completion.Imports, completion.Forgets, completion.CompletedAt,
		completion.ErrorSummary, artifact.key, artifact.checksum, artifact.createdAt,
		artifact.expiresAt, []byte(completion.Metadata), runs.StatusRunning, runs.StatusPending, runs.StatusSkipped,
		runs.StatusCancelled,
	)
	if err != nil {
		return fmt.Errorf("completing project run: %w", err)
	}
	updated, err := rowsAffected(result)
	if err != nil {
		return fmt.Errorf("checking completed project run: %w", err)
	}
	if updated == 1 {
		return nil
	}

	existing, err := s.getProjectRun(opCtx, completion.ID)
	if err != nil {
		return err
	}
	if projectCompletionMatches(existing, completion) {
		return nil
	}
	return fmt.Errorf("completing project run %s from status %q: %w", completion.ID, existing.Status, runs.ErrConflict)
}

func (s *Store) AppendOutput(ctx context.Context, chunks []runs.OutputChunk) error {
	if len(chunks) == 0 {
		return nil
	}
	if len(chunks) > maxOutputBatchSize {
		return fmt.Errorf("output batch exceeds %d chunks", maxOutputBatchSize)
	}
	normalized := make([]runs.OutputChunk, len(chunks))
	seen := make(map[int64]runs.OutputChunk, len(chunks))
	projectRunID := chunks[0].ProjectRunID
	for i, chunk := range chunks {
		if err := chunk.Validate(); err != nil {
			return fmt.Errorf("validating output chunk %d: %w", i, err)
		}
		if len([]byte(chunk.Content)) > maxOutputChunkBytes {
			return fmt.Errorf("output chunk %d exceeds %d bytes", i, maxOutputChunkBytes)
		}
		if chunk.ProjectRunID != projectRunID {
			return fmt.Errorf("output batch contains multiple project runs")
		}
		chunk.CreatedAt = normalizeTime(chunk.CreatedAt)
		if previous, ok := seen[chunk.Sequence]; ok && previous != chunk {
			return fmt.Errorf("output sequence %d appears with different data: %w", chunk.Sequence, runs.ErrConflict)
		}
		seen[chunk.Sequence] = chunk
		normalized[i] = chunk
	}

	opCtx, cancel := s.operationContext(ctx)
	defer cancel()
	tx, err := s.db.BeginTx(opCtx, nil)
	if err != nil {
		return fmt.Errorf("starting output batch: %w", err)
	}
	defer tx.Rollback() // nolint: errcheck
	for _, chunk := range normalized {
		result, err := tx.ExecContext(opCtx, `INSERT INTO run_output_chunks (
            project_run_id, sequence, stream, content, created_at
        ) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`,
			chunk.ProjectRunID, chunk.Sequence, chunk.Stream, chunk.Content, chunk.CreatedAt,
		)
		if err != nil {
			return fmt.Errorf("appending output sequence %d: %w", chunk.Sequence, err)
		}
		inserted, err := rowsAffected(result)
		if err != nil {
			return fmt.Errorf("checking output sequence %d: %w", chunk.Sequence, err)
		}
		if inserted == 1 {
			continue
		}
		var stream runs.OutputStream
		var content string
		var createdAt time.Time
		if err := tx.QueryRowContext(opCtx, `SELECT stream, content, created_at
            FROM run_output_chunks WHERE project_run_id = $1 AND sequence = $2`,
			chunk.ProjectRunID, chunk.Sequence,
		).Scan(&stream, &content, &createdAt); err != nil {
			return mapLookupError("reading replayed output chunk", err)
		}
		if stream != chunk.Stream || content != chunk.Content || !normalizeTime(createdAt).Equal(chunk.CreatedAt) {
			return fmt.Errorf("appending output sequence %d: %w", chunk.Sequence, runs.ErrConflict)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing output batch: %w", err)
	}
	return nil
}

func (s *Store) AppendAuditEvent(ctx context.Context, event runs.AuditEvent) error {
	if err := event.Validate(); err != nil {
		return fmt.Errorf("validating audit event: %w", err)
	}
	event = normalizeAuditEvent(event)
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()

	result, err := s.db.ExecContext(opCtx, `INSERT INTO audit_events (
        id, repository, pull_number, run_id, actor, event_type, metadata, created_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING`,
		event.ID, event.Repository, intPointerValue(event.PullNumber), idPointerValue(event.RunID),
		event.Actor, event.EventType, []byte(event.Metadata), event.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("appending audit event: %w", err)
	}
	inserted, err := rowsAffected(result)
	if err != nil {
		return fmt.Errorf("checking appended audit event: %w", err)
	}
	if inserted == 1 {
		return nil
	}

	existing, err := s.getAuditEvent(opCtx, event.ID)
	if err != nil {
		if errors.Is(err, runs.ErrNotFound) {
			return fmt.Errorf("appending audit event: %w", runs.ErrConflict)
		}
		return err
	}
	if !reflect.DeepEqual(existing, event) {
		return fmt.Errorf("appending audit event %s: %w", event.ID, runs.ErrConflict)
	}
	return nil
}

func rowsAffected(result sql.Result) (int64, error) {
	return result.RowsAffected()
}

func mapLookupError(action string, err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%s: %w", action, runs.ErrNotFound)
	}
	return fmt.Errorf("%s: %w", action, err)
}

func normalizeMetadata(metadata runs.Metadata) runs.Metadata {
	if len(metadata) == 0 {
		return runs.Metadata(`{}`)
	}
	var value map[string]json.RawMessage
	if err := json.Unmarshal(metadata, &value); err != nil {
		return metadata
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return metadata
	}
	return runs.Metadata(canonical)
}

func normalizeRun(run runs.Run) runs.Run {
	run.CreatedAt = normalizeTime(run.CreatedAt)
	run.StartedAt = normalizeTimePtr(run.StartedAt)
	run.CompletedAt = normalizeTimePtr(run.CompletedAt)
	run.Metadata = normalizeMetadata(run.Metadata)
	return run
}

func normalizeProjectRun(projectRun runs.ProjectRun) runs.ProjectRun {
	projectRun.StartedAt = normalizeTimePtr(projectRun.StartedAt)
	projectRun.CompletedAt = normalizeTimePtr(projectRun.CompletedAt)
	projectRun.Metadata = normalizeMetadata(projectRun.Metadata)
	if projectRun.PlanArtifact != nil {
		artifact := *projectRun.PlanArtifact
		artifact.CreatedAt = normalizeTime(artifact.CreatedAt)
		artifact.ExpiresAt = normalizeTimePtr(artifact.ExpiresAt)
		projectRun.PlanArtifact = &artifact
	}
	return projectRun
}

func normalizeProjectRunCompletion(completion runs.ProjectRunCompletion) runs.ProjectRunCompletion {
	completion.CompletedAt = normalizeTime(completion.CompletedAt)
	completion.Metadata = normalizeMetadata(completion.Metadata)
	if completion.PlanArtifact != nil {
		artifact := *completion.PlanArtifact
		artifact.CreatedAt = normalizeTime(artifact.CreatedAt)
		artifact.ExpiresAt = normalizeTimePtr(artifact.ExpiresAt)
		completion.PlanArtifact = &artifact
	}
	return completion
}

func normalizeAuditEvent(event runs.AuditEvent) runs.AuditEvent {
	event.CreatedAt = normalizeTime(event.CreatedAt)
	event.Metadata = normalizeMetadata(event.Metadata)
	return event
}

func intPointerValue(value *int) any {
	if value == nil {
		return nil
	}
	return int64(*value)
}

func idPointerValue(value *runs.ID) any {
	if value == nil {
		return nil
	}
	return *value
}

type nullableArtifact struct {
	key       any
	checksum  any
	createdAt any
	expiresAt any
}

func artifactValues(artifact *runs.ArtifactReference) nullableArtifact {
	if artifact == nil {
		return nullableArtifact{}
	}
	return nullableArtifact{
		key:       artifact.Key,
		checksum:  nullableString(artifact.Checksum),
		createdAt: normalizeTime(artifact.CreatedAt),
		expiresAt: normalizeTimePtr(artifact.ExpiresAt),
	}
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func projectCompletionMatches(projectRun runs.ProjectRun, completion runs.ProjectRunCompletion) bool {
	return projectRun.Status == completion.Status &&
		projectRun.Additions == completion.Additions &&
		projectRun.Changes == completion.Changes &&
		projectRun.Destructions == completion.Destructions &&
		projectRun.Imports == completion.Imports &&
		projectRun.Forgets == completion.Forgets &&
		projectRun.CompletedAt != nil && projectRun.CompletedAt.Equal(completion.CompletedAt) &&
		projectRun.ErrorSummary == completion.ErrorSummary &&
		reflect.DeepEqual(projectRun.PlanArtifact, completion.PlanArtifact) &&
		reflect.DeepEqual(projectRun.Metadata, completion.Metadata)
}
