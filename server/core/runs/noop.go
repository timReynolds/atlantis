// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package runs

import (
	"context"
	"time"
)

// NoopStore preserves existing Atlantis behavior when durable run history is
// not configured.
type NoopStore struct{}

func (NoopStore) CreateRun(context.Context, Run) error { return nil }

func (NoopStore) StartRun(context.Context, ID, time.Time) error { return nil }

func (NoopStore) CompleteRun(context.Context, RunCompletion) error { return nil }

func (NoopStore) CreateProjectRun(context.Context, ProjectRun) error { return nil }

func (NoopStore) StartProjectRun(context.Context, ID, time.Time) error { return nil }

func (NoopStore) CompleteProjectRun(context.Context, ProjectRunCompletion) error { return nil }

func (NoopStore) AppendOutput(context.Context, []OutputChunk) error { return nil }

func (NoopStore) AppendAuditEvent(context.Context, AuditEvent) error { return nil }

func (NoopStore) GetRun(context.Context, ID) (Run, error) {
	return Run{}, ErrNotFound
}

func (NoopStore) ListRuns(context.Context, RunFilter, PageRequest) (RunPage, error) {
	return RunPage{Runs: []Run{}}, nil
}

func (NoopStore) GetProjectRun(context.Context, ID) (ProjectRun, error) {
	return ProjectRun{}, ErrNotFound
}

func (NoopStore) ListProjectRuns(context.Context, ID, ProjectRunFilter, PageRequest) (ProjectRunPage, error) {
	return ProjectRunPage{ProjectRuns: []ProjectRun{}}, nil
}

func (NoopStore) SummarizeProjectRuns(context.Context, ID) (ProjectRunSummary, error) {
	return ProjectRunSummary{}, nil
}

func (NoopStore) GetOutput(context.Context, ID, int64, int) (OutputPage, error) {
	return OutputPage{Chunks: []OutputChunk{}}, nil
}

func (NoopStore) ListAuditEvents(context.Context, AuditFilter, PageRequest) (AuditPage, error) {
	return AuditPage{Events: []AuditEvent{}}, nil
}

func (NoopStore) ApplyRetention(context.Context, RetentionPolicy) (RetentionResult, error) {
	return RetentionResult{}, nil
}

var _ Store = NoopStore{}
