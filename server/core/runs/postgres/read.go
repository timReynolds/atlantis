// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
)

const (
	defaultPageLimit = 50
	maximumPageLimit = 500
)

const runColumns = `id, repository, pull_number, command, trigger, actor, base_ref,
    head_ref, head_sha, status, created_at, started_at, completed_at, metadata`

const projectRunColumns = `id, run_id, project_name, directory, workspace, status,
    additions, changes, destructions, imports, forgets, started_at, completed_at,
    error_summary, plan_artifact_key, plan_artifact_checksum,
    plan_artifact_created_at, plan_artifact_expires_at, metadata`

const auditEventColumns = `id, repository, pull_number, run_id, actor, event_type,
    metadata, created_at`

type rowScanner interface {
	Scan(dest ...any) error
}

func (s *Store) GetRun(ctx context.Context, id runs.ID) (runs.Run, error) {
	if _, err := runs.ParseID(string(id)); err != nil {
		return runs.Run{}, fmt.Errorf("validating run ID: %w", err)
	}
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()
	return s.getRun(opCtx, id)
}

func (s *Store) getRun(ctx context.Context, id runs.ID) (runs.Run, error) {
	return scanRun(s.db.QueryRowContext(ctx, "SELECT "+runColumns+" FROM runs WHERE id = $1", id))
}

func (s *Store) ListRuns(ctx context.Context, filter runs.RunFilter, page runs.PageRequest) (runs.RunPage, error) {
	limit, err := pageLimit(page.Limit)
	if err != nil {
		return runs.RunPage{}, err
	}
	query := "SELECT " + runColumns + " FROM runs"
	clauses := make([]string, 0, 9)
	args := make([]any, 0, 12)
	add := func(clause string, value any) {
		args = append(args, value)
		clauses = append(clauses, fmt.Sprintf(clause, len(args)))
	}
	if filter.Repository != "" {
		add("repository = $%d", filter.Repository)
	}
	if filter.PullNumber != nil {
		add("pull_number = $%d", int64(*filter.PullNumber))
	}
	if filter.HeadSHA != "" {
		add("head_sha = $%d", filter.HeadSHA)
	}
	if filter.Actor != "" {
		add("actor = $%d", filter.Actor)
	}
	if len(filter.Commands) > 0 {
		clauses = append(clauses, inClause("command", commandsToAny(filter.Commands), &args))
	}
	if len(filter.Statuses) > 0 {
		clauses = append(clauses, inClause("status", statusesToAny(filter.Statuses), &args))
	}
	if filter.CreatedFrom != nil {
		add("created_at >= $%d", normalizeTime(*filter.CreatedFrom))
	}
	if filter.CreatedTo != nil {
		add("created_at < $%d", normalizeTime(*filter.CreatedTo))
	}
	if page.Cursor != "" {
		cursor, err := decodeTimeCursor(page.Cursor)
		if err != nil {
			return runs.RunPage{}, fmt.Errorf("decoding run page cursor: %w", err)
		}
		args = append(args, cursor.CreatedAt, cursor.ID)
		clauses = append(clauses, fmt.Sprintf("(created_at, id) < ($%d, $%d)", len(args)-1, len(args)))
	}
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	args = append(args, limit+1)
	query += fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", len(args))

	opCtx, cancel := s.operationContext(ctx)
	defer cancel()
	rows, err := s.db.QueryContext(opCtx, query, args...)
	if err != nil {
		return runs.RunPage{}, fmt.Errorf("listing runs: %w", err)
	}
	defer rows.Close()

	result := runs.RunPage{Runs: make([]runs.Run, 0, limit)}
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return runs.RunPage{}, err
		}
		result.Runs = append(result.Runs, run)
	}
	if err := rows.Err(); err != nil {
		return runs.RunPage{}, fmt.Errorf("iterating runs: %w", err)
	}
	if len(result.Runs) > limit {
		last := result.Runs[limit-1]
		result.NextCursor, err = encodeCursor(timeCursor{CreatedAt: last.CreatedAt, ID: last.ID})
		if err != nil {
			return runs.RunPage{}, fmt.Errorf("encoding run page cursor: %w", err)
		}
		result.Runs = result.Runs[:limit]
	}
	return result, nil
}

func (s *Store) GetProjectRun(ctx context.Context, id runs.ID) (runs.ProjectRun, error) {
	if _, err := runs.ParseID(string(id)); err != nil {
		return runs.ProjectRun{}, fmt.Errorf("validating project run ID: %w", err)
	}
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()
	return s.getProjectRun(opCtx, id)
}

func (s *Store) getProjectRun(ctx context.Context, id runs.ID) (runs.ProjectRun, error) {
	return scanProjectRun(s.db.QueryRowContext(ctx, "SELECT "+projectRunColumns+" FROM project_runs WHERE id = $1", id))
}

func (s *Store) ListProjectRuns(ctx context.Context, runID runs.ID, filter runs.ProjectRunFilter, page runs.PageRequest) (runs.ProjectRunPage, error) {
	if _, err := runs.ParseID(string(runID)); err != nil {
		return runs.ProjectRunPage{}, fmt.Errorf("validating run ID: %w", err)
	}
	limit, err := pageLimit(page.Limit)
	if err != nil {
		return runs.ProjectRunPage{}, err
	}
	query := "SELECT " + projectRunColumns + " FROM project_runs"
	args := []any{runID}
	clauses := []string{"run_id = $1"}
	add := func(clause string, value any) {
		args = append(args, value)
		clauses = append(clauses, fmt.Sprintf(clause, len(args)))
	}
	if filter.ProjectName != "" {
		add("project_name = $%d", filter.ProjectName)
	}
	if filter.Directory != "" {
		add("directory = $%d", filter.Directory)
	}
	if filter.Workspace != "" {
		add("workspace = $%d", filter.Workspace)
	}
	if len(filter.Statuses) > 0 {
		clauses = append(clauses, inClause("status", statusesToAny(filter.Statuses), &args))
	}
	if page.Cursor != "" {
		cursor, err := decodeProjectCursor(page.Cursor)
		if err != nil {
			return runs.ProjectRunPage{}, fmt.Errorf("decoding project run page cursor: %w", err)
		}
		args = append(args, cursor.ProjectName, cursor.Directory, cursor.Workspace, cursor.ID)
		clauses = append(clauses, fmt.Sprintf(
			"(project_name, directory, workspace, id) > ($%d, $%d, $%d, $%d)",
			len(args)-3, len(args)-2, len(args)-1, len(args),
		))
	}
	args = append(args, limit+1)
	query += " WHERE " + strings.Join(clauses, " AND ")
	query += fmt.Sprintf(" ORDER BY project_name, directory, workspace, id LIMIT $%d", len(args))

	opCtx, cancel := s.operationContext(ctx)
	defer cancel()
	rows, err := s.db.QueryContext(opCtx, query, args...)
	if err != nil {
		return runs.ProjectRunPage{}, fmt.Errorf("listing project runs: %w", err)
	}
	defer rows.Close()

	result := runs.ProjectRunPage{ProjectRuns: make([]runs.ProjectRun, 0, limit)}
	for rows.Next() {
		projectRun, err := scanProjectRun(rows)
		if err != nil {
			return runs.ProjectRunPage{}, err
		}
		result.ProjectRuns = append(result.ProjectRuns, projectRun)
	}
	if err := rows.Err(); err != nil {
		return runs.ProjectRunPage{}, fmt.Errorf("iterating project runs: %w", err)
	}
	if len(result.ProjectRuns) > limit {
		last := result.ProjectRuns[limit-1]
		result.NextCursor, err = encodeCursor(projectCursor{
			ProjectName: last.ProjectName, Directory: last.Directory,
			Workspace: last.Workspace, ID: last.ID,
		})
		if err != nil {
			return runs.ProjectRunPage{}, fmt.Errorf("encoding project run page cursor: %w", err)
		}
		result.ProjectRuns = result.ProjectRuns[:limit]
	}
	return result, nil
}

func (s *Store) SummarizeProjectRuns(ctx context.Context, runID runs.ID) (runs.ProjectRunSummary, error) {
	if _, err := runs.ParseID(string(runID)); err != nil {
		return runs.ProjectRunSummary{}, fmt.Errorf("validating run ID: %w", err)
	}
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()
	rows, err := s.db.QueryContext(opCtx, `SELECT status, COUNT(*)
        FROM project_runs WHERE run_id = $1 GROUP BY status`, runID)
	if err != nil {
		return runs.ProjectRunSummary{}, fmt.Errorf("summarizing project runs: %w", err)
	}
	defer rows.Close()

	var summary runs.ProjectRunSummary
	for rows.Next() {
		var status runs.Status
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return runs.ProjectRunSummary{}, fmt.Errorf("scanning project run summary: %w", err)
		}
		summary.Total += count
		switch status {
		case runs.StatusPending:
			summary.Pending += count
		case runs.StatusRunning:
			summary.Running += count
		case runs.StatusSucceeded:
			summary.Succeeded += count
		case runs.StatusUnchanged:
			summary.Unchanged += count
		case runs.StatusFailed:
			summary.Failed += count
		case runs.StatusPartial:
			summary.Partial += count
		case runs.StatusCancelled:
			summary.Cancelled += count
		case runs.StatusSkipped:
			summary.Skipped += count
		case runs.StatusUnknown:
			summary.Unknown += count
		default:
			return runs.ProjectRunSummary{}, fmt.Errorf("summarizing project runs: unknown status %q", status)
		}
	}
	if err := rows.Err(); err != nil {
		return runs.ProjectRunSummary{}, fmt.Errorf("iterating project run summary: %w", err)
	}
	return summary, nil
}

func (s *Store) GetOutput(ctx context.Context, projectRunID runs.ID, afterSequence int64, limit int) (runs.OutputPage, error) {
	if _, err := runs.ParseID(string(projectRunID)); err != nil {
		return runs.OutputPage{}, fmt.Errorf("validating project run ID: %w", err)
	}
	pageSize, err := pageLimit(limit)
	if err != nil {
		return runs.OutputPage{}, err
	}
	opCtx, cancel := s.operationContext(ctx)
	defer cancel()
	rows, err := s.db.QueryContext(opCtx, `SELECT project_run_id, sequence, stream, content, created_at
        FROM run_output_chunks
        WHERE project_run_id = $1 AND sequence > $2
        ORDER BY sequence LIMIT $3`, projectRunID, afterSequence, pageSize+1)
	if err != nil {
		return runs.OutputPage{}, fmt.Errorf("reading project output: %w", err)
	}
	defer rows.Close()

	result := runs.OutputPage{Chunks: make([]runs.OutputChunk, 0, pageSize)}
	for rows.Next() {
		var chunk runs.OutputChunk
		if err := rows.Scan(&chunk.ProjectRunID, &chunk.Sequence, &chunk.Stream, &chunk.Content, &chunk.CreatedAt); err != nil {
			return runs.OutputPage{}, fmt.Errorf("scanning output chunk: %w", err)
		}
		chunk.CreatedAt = normalizeTime(chunk.CreatedAt)
		result.Chunks = append(result.Chunks, chunk)
	}
	if err := rows.Err(); err != nil {
		return runs.OutputPage{}, fmt.Errorf("iterating project output: %w", err)
	}
	if len(result.Chunks) > pageSize {
		next := result.Chunks[pageSize-1].Sequence
		result.NextSequence = &next
		result.Chunks = result.Chunks[:pageSize]
	}
	return result, nil
}

func (s *Store) ListAuditEvents(ctx context.Context, filter runs.AuditFilter, page runs.PageRequest) (runs.AuditPage, error) {
	limit, err := pageLimit(page.Limit)
	if err != nil {
		return runs.AuditPage{}, err
	}
	query := "SELECT " + auditEventColumns + " FROM audit_events"
	clauses := make([]string, 0, 8)
	args := make([]any, 0, 10)
	add := func(clause string, value any) {
		args = append(args, value)
		clauses = append(clauses, fmt.Sprintf(clause, len(args)))
	}
	if filter.Repository != "" {
		add("repository = $%d", filter.Repository)
	}
	if filter.PullNumber != nil {
		add("pull_number = $%d", int64(*filter.PullNumber))
	}
	if filter.RunID != nil {
		add("run_id = $%d", *filter.RunID)
	}
	if filter.Actor != "" {
		add("actor = $%d", filter.Actor)
	}
	if len(filter.EventTypes) > 0 {
		values := make([]any, len(filter.EventTypes))
		for i, eventType := range filter.EventTypes {
			values[i] = eventType
		}
		clauses = append(clauses, inClause("event_type", values, &args))
	}
	if filter.CreatedFrom != nil {
		add("created_at >= $%d", normalizeTime(*filter.CreatedFrom))
	}
	if filter.CreatedTo != nil {
		add("created_at < $%d", normalizeTime(*filter.CreatedTo))
	}
	if page.Cursor != "" {
		cursor, err := decodeTimeCursor(page.Cursor)
		if err != nil {
			return runs.AuditPage{}, fmt.Errorf("decoding audit page cursor: %w", err)
		}
		args = append(args, cursor.CreatedAt, cursor.ID)
		clauses = append(clauses, fmt.Sprintf("(created_at, id) < ($%d, $%d)", len(args)-1, len(args)))
	}
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	args = append(args, limit+1)
	query += fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", len(args))

	opCtx, cancel := s.operationContext(ctx)
	defer cancel()
	rows, err := s.db.QueryContext(opCtx, query, args...)
	if err != nil {
		return runs.AuditPage{}, fmt.Errorf("listing audit events: %w", err)
	}
	defer rows.Close()

	result := runs.AuditPage{Events: make([]runs.AuditEvent, 0, limit)}
	for rows.Next() {
		event, err := scanAuditEvent(rows)
		if err != nil {
			return runs.AuditPage{}, err
		}
		result.Events = append(result.Events, event)
	}
	if err := rows.Err(); err != nil {
		return runs.AuditPage{}, fmt.Errorf("iterating audit events: %w", err)
	}
	if len(result.Events) > limit {
		last := result.Events[limit-1]
		result.NextCursor, err = encodeCursor(timeCursor{CreatedAt: last.CreatedAt, ID: last.ID})
		if err != nil {
			return runs.AuditPage{}, fmt.Errorf("encoding audit page cursor: %w", err)
		}
		result.Events = result.Events[:limit]
	}
	return result, nil
}

func (s *Store) getAuditEvent(ctx context.Context, id runs.ID) (runs.AuditEvent, error) {
	return scanAuditEvent(s.db.QueryRowContext(ctx, "SELECT "+auditEventColumns+" FROM audit_events WHERE id = $1", id))
}

func scanRun(row rowScanner) (runs.Run, error) {
	var run runs.Run
	var pullNumber sql.NullInt64
	var startedAt, completedAt sql.NullTime
	var metadata []byte
	if err := row.Scan(
		&run.ID, &run.Repository, &pullNumber, &run.Command, &run.Trigger, &run.Actor,
		&run.BaseRef, &run.HeadRef, &run.HeadSHA, &run.Status, &run.CreatedAt,
		&startedAt, &completedAt, &metadata,
	); err != nil {
		return runs.Run{}, mapLookupError("scanning run", err)
	}
	run.PullNumber = intFromNull(pullNumber)
	run.CreatedAt = normalizeTime(run.CreatedAt)
	run.StartedAt = timeFromNull(startedAt)
	run.CompletedAt = timeFromNull(completedAt)
	run.Metadata = normalizeMetadata(metadata)
	return run, nil
}

func scanProjectRun(row rowScanner) (runs.ProjectRun, error) {
	var projectRun runs.ProjectRun
	var startedAt, completedAt sql.NullTime
	var artifactKey, artifactChecksum sql.NullString
	var artifactCreatedAt, artifactExpiresAt sql.NullTime
	var metadata []byte
	if err := row.Scan(
		&projectRun.ID, &projectRun.RunID, &projectRun.ProjectName, &projectRun.Directory,
		&projectRun.Workspace, &projectRun.Status, &projectRun.Additions, &projectRun.Changes,
		&projectRun.Destructions, &projectRun.Imports, &projectRun.Forgets, &startedAt,
		&completedAt, &projectRun.ErrorSummary, &artifactKey, &artifactChecksum,
		&artifactCreatedAt, &artifactExpiresAt, &metadata,
	); err != nil {
		return runs.ProjectRun{}, mapLookupError("scanning project run", err)
	}
	projectRun.StartedAt = timeFromNull(startedAt)
	projectRun.CompletedAt = timeFromNull(completedAt)
	projectRun.Metadata = normalizeMetadata(metadata)
	if artifactKey.Valid {
		projectRun.PlanArtifact = &runs.ArtifactReference{
			Key: artifactKey.String, Checksum: artifactChecksum.String,
			CreatedAt: normalizeTime(artifactCreatedAt.Time), ExpiresAt: timeFromNull(artifactExpiresAt),
		}
	}
	return projectRun, nil
}

func scanAuditEvent(row rowScanner) (runs.AuditEvent, error) {
	var event runs.AuditEvent
	var pullNumber sql.NullInt64
	var runID sql.NullString
	var metadata []byte
	if err := row.Scan(
		&event.ID, &event.Repository, &pullNumber, &runID, &event.Actor,
		&event.EventType, &metadata, &event.CreatedAt,
	); err != nil {
		return runs.AuditEvent{}, mapLookupError("scanning audit event", err)
	}
	event.PullNumber = intFromNull(pullNumber)
	if runID.Valid {
		id := runs.ID(runID.String)
		event.RunID = &id
	}
	event.Metadata = normalizeMetadata(metadata)
	event.CreatedAt = normalizeTime(event.CreatedAt)
	return event, nil
}

func timeFromNull(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	t := normalizeTime(value.Time)
	return &t
}

func intFromNull(value sql.NullInt64) *int {
	if !value.Valid {
		return nil
	}
	i := int(value.Int64)
	return &i
}

func pageLimit(limit int) (int, error) {
	if limit == 0 {
		return defaultPageLimit, nil
	}
	if limit < 0 || limit > maximumPageLimit {
		return 0, fmt.Errorf("page limit must be between 1 and %d", maximumPageLimit)
	}
	return limit, nil
}

func inClause(column string, values []any, args *[]any) string {
	placeholders := make([]string, len(values))
	for i, value := range values {
		*args = append(*args, value)
		placeholders[i] = "$" + strconv.Itoa(len(*args))
	}
	return column + " IN (" + strings.Join(placeholders, ", ") + ")"
}

func commandsToAny(commands []runs.Command) []any {
	values := make([]any, len(commands))
	for i, command := range commands {
		values[i] = command
	}
	return values
}

func statusesToAny(statuses []runs.Status) []any {
	values := make([]any, len(statuses))
	for i, status := range statuses {
		values[i] = status
	}
	return values
}

type timeCursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        runs.ID   `json:"id"`
}

type projectCursor struct {
	ProjectName string  `json:"project_name"`
	Directory   string  `json:"directory"`
	Workspace   string  `json:"workspace"`
	ID          runs.ID `json:"id"`
}

func encodeCursor(cursor any) (string, error) {
	encoded, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodeTimeCursor(value string) (timeCursor, error) {
	var cursor timeCursor
	if err := decodeCursor(value, &cursor); err != nil {
		return timeCursor{}, err
	}
	if cursor.CreatedAt.IsZero() {
		return timeCursor{}, errors.New("cursor created time is required")
	}
	if _, err := runs.ParseID(string(cursor.ID)); err != nil {
		return timeCursor{}, fmt.Errorf("validating cursor ID: %w", err)
	}
	cursor.CreatedAt = normalizeTime(cursor.CreatedAt)
	return cursor, nil
}

func decodeProjectCursor(value string) (projectCursor, error) {
	var cursor projectCursor
	if err := decodeCursor(value, &cursor); err != nil {
		return projectCursor{}, err
	}
	if _, err := runs.ParseID(string(cursor.ID)); err != nil {
		return projectCursor{}, fmt.Errorf("validating cursor ID: %w", err)
	}
	return cursor, nil
}

func decodeCursor(value string, target any) error {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return fmt.Errorf("decoding base64: %w", err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(decoded)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decoding JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("cursor contains trailing JSON")
	}
	return nil
}
