// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package runs

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrNotFound is returned when a requested durable history record does not exist.
	ErrNotFound = errors.New("run history record not found")
	// ErrConflict is returned when a write violates a durable history identity invariant.
	ErrConflict = errors.New("run history record conflicts with existing data")
)

// RunCompletion contains the fields that may change when a Run finishes.
type RunCompletion struct {
	ID          ID
	Status      Status
	CompletedAt time.Time
}

// Validate checks completion invariants before a Store adapter writes them.
func (c RunCompletion) Validate() error {
	if _, err := ParseID(string(c.ID)); err != nil {
		return err
	}
	if !c.Status.validForRun() || c.Status == StatusPending || c.Status == StatusRunning {
		return errors.New("run completion requires a terminal status")
	}
	if c.CompletedAt.IsZero() {
		return errors.New("run completion time is required")
	}
	return nil
}

// ProjectRunCompletion contains the fields that may change when a ProjectRun
// finishes. A nil PlanArtifact leaves the project without an artifact reference.
type ProjectRunCompletion struct {
	ID           ID
	Status       Status
	Additions    int
	Changes      int
	Destructions int
	Imports      int
	Forgets      int
	CompletedAt  time.Time
	ErrorSummary string
	PlanArtifact *ArtifactReference
	Metadata     Metadata
}

// Validate checks completion invariants before a Store adapter writes them.
func (c ProjectRunCompletion) Validate() error {
	if _, err := ParseID(string(c.ID)); err != nil {
		return err
	}
	if !c.Status.validForProject() || c.Status == StatusPending || c.Status == StatusRunning {
		return errors.New("project run completion requires a terminal status")
	}
	if c.CompletedAt.IsZero() {
		return errors.New("project run completion time is required")
	}
	for _, count := range []int{c.Additions, c.Changes, c.Destructions, c.Imports, c.Forgets} {
		if count < 0 {
			return errors.New("project run completion counts cannot be negative")
		}
	}
	if c.PlanArtifact != nil {
		if c.PlanArtifact.Key == "" || c.PlanArtifact.CreatedAt.IsZero() {
			return errors.New("project run completion has an invalid plan artifact")
		}
		if c.PlanArtifact.ExpiresAt != nil && c.PlanArtifact.ExpiresAt.Before(c.PlanArtifact.CreatedAt) {
			return errors.New("plan artifact expiry precedes its created time")
		}
	}
	return validateMetadata(c.Metadata)
}

// Writer is the execution-facing seam for durable run history. Implementations
// must make completion writes idempotent and append output chunks atomically per
// batch. A (project_run_id, sequence) pair identifies one output chunk; replaying
// the same pair and content is a no-op, while different content is ErrConflict.
type Writer interface {
	CreateRun(ctx context.Context, run Run) error
	StartRun(ctx context.Context, id ID, startedAt time.Time) error
	CompleteRun(ctx context.Context, completion RunCompletion) error
	CreateProjectRun(ctx context.Context, projectRun ProjectRun) error
	StartProjectRun(ctx context.Context, id ID, startedAt time.Time) error
	CompleteProjectRun(ctx context.Context, completion ProjectRunCompletion) error
	AppendOutput(ctx context.Context, chunks []OutputChunk) error
	AppendAuditEvent(ctx context.Context, event AuditEvent) error
}

// PageRequest asks a Store for one bounded page. Cursor is opaque to callers.
type PageRequest struct {
	Limit  int
	Cursor string
}

// RunFilter selects runs for history and audit views.
type RunFilter struct {
	Repository  string
	PullNumber  *int
	HeadSHA     string
	Actor       string
	Commands    []Command
	Statuses    []Status
	CreatedFrom *time.Time
	CreatedTo   *time.Time
}

// RunPage is a bounded page of Runs ordered newest first.
type RunPage struct {
	Runs       []Run
	NextCursor string
}

// ProjectRunFilter selects projects within a Run.
type ProjectRunFilter struct {
	ProjectName string
	Directory   string
	Workspace   string
	Statuses    []Status
}

// ProjectRunPage is a bounded page of ProjectRuns ordered by identity.
type ProjectRunPage struct {
	ProjectRuns []ProjectRun
	NextCursor  string
}

// ProjectRunSummary contains status counts for one logical Run. It supports
// large-run summaries without loading every ProjectRun into a controller.
type ProjectRunSummary struct {
	Total     int
	Pending   int
	Running   int
	Succeeded int
	Unchanged int
	Failed    int
	Partial   int
	Cancelled int
	Skipped   int
	Unknown   int
}

// OutputPage is an ordered page of output chunks.
type OutputPage struct {
	Chunks       []OutputChunk
	NextSequence *int64
}

// AuditFilter selects audit events for the global timeline.
type AuditFilter struct {
	Repository  string
	PullNumber  *int
	RunID       *ID
	Actor       string
	EventTypes  []string
	CreatedFrom *time.Time
	CreatedTo   *time.Time
}

// AuditPage is a bounded page of audit events ordered newest first.
type AuditPage struct {
	Events     []AuditEvent
	NextCursor string
}

// Reader is the UI-facing seam for durable run history.
type Reader interface {
	GetRun(ctx context.Context, id ID) (Run, error)
	ListRuns(ctx context.Context, filter RunFilter, page PageRequest) (RunPage, error)
	GetProjectRun(ctx context.Context, id ID) (ProjectRun, error)
	ListProjectRuns(ctx context.Context, runID ID, filter ProjectRunFilter, page PageRequest) (ProjectRunPage, error)
	SummarizeProjectRuns(ctx context.Context, runID ID) (ProjectRunSummary, error)
	GetOutput(ctx context.Context, projectRunID ID, afterSequence int64, limit int) (OutputPage, error)
	ListAuditEvents(ctx context.Context, filter AuditFilter, page PageRequest) (AuditPage, error)
}

// RetentionPolicy independently controls deletion of high-volume data. Nil
// cutoffs retain that record class indefinitely.
type RetentionPolicy struct {
	RunMetadataBefore *time.Time
	OutputBefore      *time.Time
	AuditEventsBefore *time.Time
	DriftStatusBefore *time.Time
}

// RetentionResult reports the records removed by a retention pass.
type RetentionResult struct {
	RunsDeleted          int64
	ProjectRunsDeleted   int64
	OutputChunksDeleted  int64
	AuditEventsDeleted   int64
	DriftStatusesDeleted int64
}

// Retainer applies independently configurable history retention.
type Retainer interface {
	ApplyRetention(ctx context.Context, policy RetentionPolicy) (RetentionResult, error)
}

// Store is the complete durable run history seam. Execution code should depend
// on Writer and UI code should depend on Reader whenever they need only one side.
type Store interface {
	Writer
	Reader
	Retainer
	ExecutionStore
	PlanArtifactStore
}
