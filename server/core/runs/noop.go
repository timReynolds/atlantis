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

func (NoopStore) RegisterInstance(context.Context, ExecutionInstance) error { return nil }

func (NoopStore) HeartbeatInstance(context.Context, ID, time.Time) error { return nil }

func (NoopStore) StopInstance(context.Context, ID, time.Time) error { return nil }

func (NoopStore) CreateAttempt(context.Context, RunAttempt) error { return nil }

func (NoopStore) StartAttempt(context.Context, ID, time.Time) error { return nil }

func (NoopStore) HeartbeatAttempt(context.Context, ID, time.Time) error { return nil }

func (NoopStore) MarkAttemptSideEffectStarted(context.Context, ID, time.Time) error { return nil }

func (NoopStore) CompleteAttempt(context.Context, AttemptCompletion) error { return nil }

func (NoopStore) ReconcileAttempt(context.Context, AttemptReconciliation) error { return nil }

func (NoopStore) PrepareAttemptTakeover(context.Context, AttemptTakeoverRequest) (AttemptTakeoverResult, error) {
	return AttemptTakeoverResult{}, nil
}

func (NoopStore) RecordProjectPlanArtifact(context.Context, ProjectPlanArtifactUpdate) error {
	return nil
}

func (NoopStore) FindPlanArtifact(context.Context, PlanArtifactLookup) (PlanArtifactExpectation, error) {
	return PlanArtifactExpectation{}, ErrNotFound
}

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

func (NoopStore) GetInstance(context.Context, ID) (ExecutionInstance, error) {
	return ExecutionInstance{}, ErrNotFound
}

func (NoopStore) GetAttempt(context.Context, ID) (RunAttempt, error) {
	return RunAttempt{}, ErrNotFound
}

func (NoopStore) ListRunAttempts(context.Context, ID, PageRequest) (AttemptPage, error) {
	return AttemptPage{Attempts: []RunAttempt{}}, nil
}

func (NoopStore) ApplyRetention(context.Context, RetentionPolicy) (RetentionResult, error) {
	return RetentionResult{}, nil
}

var _ Store = NoopStore{}
