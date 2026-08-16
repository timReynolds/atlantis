// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/require"
)

func TestRunHistoryAggregatesPlanAndPolicyIntoOneProjectRun(t *testing.T) {
	writer := &recordingRunWriter{}
	history := newTestRunHistory(t, writer)
	ctx := testRunContext(t)
	lifecycle := history.Begin(ctx, runs.CommandPlan, runs.TriggerComment)
	require.NotEmpty(t, ctx.RunID)

	projectCtx := history.beginProject(command.ProjectContext{
		RunID: ctx.RunID, ProjectName: "network", RepoRelDir: "terraform/network",
		Workspace: "production", Log: ctx.Log,
	})
	history.recordProject(projectCtx, command.Plan, command.ProjectCommandOutput{
		PlanSuccess: &models.PlanSuccess{TerraformOutput: "Plan: 1 to import, 2 to add, 3 to change, 4 to destroy, 5 to forget."},
	})
	policyCtx := history.beginProject(command.ProjectContext{
		RunID: ctx.RunID, ProjectName: "network", RepoRelDir: "terraform/network",
		Workspace: "production", Log: ctx.Log,
	})
	require.Equal(t, projectCtx.ProjectRunID, policyCtx.ProjectRunID)
	history.recordProject(policyCtx, command.PolicyCheck, command.ProjectCommandOutput{
		Failure: "policy denied",
		PolicyCheckResults: &models.PolicyCheckResults{PolicySetResults: []models.PolicySetResult{
			{PolicySetName: "production", Passed: false},
		}},
	})
	history.RecordPlanArtifact(projectCtx, runs.ArtifactReference{
		Key: "plans/network.tfplan", Checksum: "sha256:abc", CreatedAt: testHistoryTime,
	})
	ctx.CommandHasErrors = true
	lifecycle.Finish()

	require.Len(t, writer.runsCreated, 1)
	require.Equal(t, runs.StatusRunning, writer.runsCreated[0].Status)
	require.Len(t, writer.projectsCreated, 1)
	require.Len(t, writer.projectsCompleted, 1)
	completion := writer.projectsCompleted[0]
	require.Equal(t, runs.StatusFailed, completion.Status)
	require.Equal(t, 2, completion.Additions)
	require.Equal(t, 3, completion.Changes)
	require.Equal(t, 4, completion.Destructions)
	require.Equal(t, 1, completion.Imports)
	require.Equal(t, 5, completion.Forgets)
	require.Equal(t, "policy denied", completion.ErrorSummary)
	require.Equal(t, "plans/network.tfplan", completion.PlanArtifact.Key)
	var metadata struct {
		Phases     []string `json:"phases"`
		PolicySets []struct {
			Name   string `json:"name"`
			Passed bool   `json:"passed"`
		} `json:"policy_sets"`
	}
	require.NoError(t, json.Unmarshal(completion.Metadata, &metadata))
	require.Equal(t, []string{"plan", "policy_check"}, metadata.Phases)
	require.Equal(t, "production", metadata.PolicySets[0].Name)
	require.False(t, metadata.PolicySets[0].Passed)
	require.Equal(t, runs.StatusFailed, writer.runsCompleted[0].Status)
	require.Equal(t, []string{"plan.requested", "plan.completed"}, writer.auditTypes())
}

func TestRunHistoryRepresentsHundredsOfProjectsInOneRun(t *testing.T) {
	writer := &recordingRunWriter{}
	history := newTestRunHistory(t, writer)
	ctx := testRunContext(t)
	lifecycle := history.Begin(ctx, runs.CommandPlan, runs.TriggerAutoplan)

	const projectCount = 500
	var group sync.WaitGroup
	for i := 0; i < projectCount; i++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			projectCtx := history.beginProject(command.ProjectContext{
				RunID: ctx.RunID, ProjectName: fmt.Sprintf("project-%03d", index),
				RepoRelDir: fmt.Sprintf("terraform/project-%03d", index),
				Workspace:  "default", Log: ctx.Log,
			})
			history.recordProject(projectCtx, command.Plan, command.ProjectCommandOutput{
				PlanSuccess: &models.PlanSuccess{TerraformOutput: "No changes. Your infrastructure matches the configuration."},
			})
		}(i)
	}
	group.Wait()
	lifecycle.Finish()

	require.Len(t, writer.projectsCreated, projectCount)
	require.Len(t, writer.projectsCompleted, projectCount)
	require.Equal(t, runs.StatusSucceeded, writer.runsCompleted[0].Status)
}

func TestRunHistoryRecordsPartialRun(t *testing.T) {
	writer := &recordingRunWriter{}
	history := newTestRunHistory(t, writer)
	ctx := testRunContext(t)
	lifecycle := history.Begin(ctx, runs.CommandApply, runs.TriggerComment)
	for i, output := range []command.ProjectCommandOutput{
		{ApplySuccess: "complete"},
		{Error: errors.New("apply errored")},
	} {
		projectCtx := history.beginProject(command.ProjectContext{
			RunID: ctx.RunID, ProjectName: string(rune('a' + i)),
			RepoRelDir: string(rune('a' + i)), Workspace: "default", Log: ctx.Log,
		})
		history.recordProject(projectCtx, command.Apply, output)
	}
	ctx.CommandHasErrors = true
	lifecycle.Finish()

	require.Equal(t, runs.StatusPartial, writer.runsCompleted[0].Status)
}

func TestRunHistoryRecordsSkippedRunWithoutProjects(t *testing.T) {
	writer := &recordingRunWriter{}
	history := newTestRunHistory(t, writer)
	ctx := testRunContext(t)
	lifecycle := history.Begin(ctx, runs.CommandPlan, runs.TriggerAutoplan)
	ctx.CommandSkipped = true
	lifecycle.Finish()

	require.Equal(t, runs.StatusSkipped, writer.runsCompleted[0].Status)
}

func TestRunLifecycleFinishRecoveringMarksPanicFailed(t *testing.T) {
	writer := &recordingRunWriter{}
	history := newTestRunHistory(t, writer)
	ctx := testRunContext(t)
	lifecycle := history.Begin(ctx, runs.CommandPlan, runs.TriggerComment)

	func() {
		defer func() { require.Equal(t, "boom", recover()) }()
		defer lifecycle.FinishRecovering()
		panic("boom")
	}()

	require.Equal(t, runs.StatusFailed, writer.runsCompleted[0].Status)
}

var testHistoryTime = time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)

func newTestRunHistory(t *testing.T, writer runs.Writer) *RunHistory {
	t.Helper()
	history := NewRunHistory(writer, logging.NewNoopLogger(t))
	history.now = func() time.Time { return testHistoryTime }
	history.newID = func() (runs.ID, error) { return runs.NewID() }
	return history
}

func testRunContext(t *testing.T) *command.Context {
	t.Helper()
	return &command.Context{
		Log:  logging.NewNoopLogger(t),
		User: models.User{Username: "operator"},
		Pull: models.PullRequest{
			Num: 42, BaseBranch: "main", HeadBranch: "feature", HeadCommit: "abc123",
			URL: "https://example.test/pull/42",
			BaseRepo: models.Repo{
				FullName: "org/repo", Owner: "org", Name: "repo",
				VCSHost: models.VCSHost{Type: models.Github, Hostname: "github.example.test"},
			},
		},
	}
}

type recordingRunWriter struct {
	mu                sync.Mutex
	runsCreated       []runs.Run
	runsCompleted     []runs.RunCompletion
	projectsCreated   []runs.ProjectRun
	projectsCompleted []runs.ProjectRunCompletion
	output            []runs.OutputChunk
	audit             []runs.AuditEvent
}

func (w *recordingRunWriter) CreateRun(_ context.Context, run runs.Run) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.runsCreated = append(w.runsCreated, run)
	return nil
}

func (w *recordingRunWriter) StartRun(context.Context, runs.ID, time.Time) error { return nil }

func (w *recordingRunWriter) CompleteRun(_ context.Context, completion runs.RunCompletion) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.runsCompleted = append(w.runsCompleted, completion)
	return nil
}

func (w *recordingRunWriter) CreateProjectRun(_ context.Context, project runs.ProjectRun) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.projectsCreated = append(w.projectsCreated, project)
	return nil
}

func (w *recordingRunWriter) StartProjectRun(context.Context, runs.ID, time.Time) error { return nil }

func (w *recordingRunWriter) CompleteProjectRun(_ context.Context, completion runs.ProjectRunCompletion) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.projectsCompleted = append(w.projectsCompleted, completion)
	return nil
}

func (w *recordingRunWriter) AppendOutput(_ context.Context, chunks []runs.OutputChunk) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.output = append(w.output, chunks...)
	return nil
}

func (w *recordingRunWriter) AppendAuditEvent(_ context.Context, event runs.AuditEvent) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.audit = append(w.audit, event)
	return nil
}

func (w *recordingRunWriter) auditTypes() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	result := make([]string, len(w.audit))
	for i, event := range w.audit {
		result[i] = event.EventType
	}
	return result
}
