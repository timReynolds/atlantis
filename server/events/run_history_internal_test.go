// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

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
	commentFinalized := false
	require.True(t, history.DeferRunComment(ctx.RunID, func(complete bool) {
		commentFinalized = complete
	}))

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
	require.True(t, commentFinalized)
}

func TestRunHistoryReportsIncompleteProjectPersistence(t *testing.T) {
	writer := &recordingRunWriter{createProjectErr: errors.New("store unavailable")}
	history := newTestRunHistory(t, writer)
	ctx := testRunContext(t)
	lifecycle := history.Begin(ctx, runs.CommandPlan, runs.TriggerComment)
	commentFinalized := false
	commentComplete := true
	require.True(t, history.DeferRunComment(ctx.RunID, func(complete bool) {
		commentFinalized = true
		commentComplete = complete
	}))
	require.True(t, history.IsRunHistoryComplete(ctx.RunID))

	history.beginProject(command.ProjectContext{
		RunID: ctx.RunID, ProjectName: "network", RepoRelDir: "terraform/network",
		Workspace: "production", Log: ctx.Log,
	})
	require.False(t, history.IsRunHistoryComplete(ctx.RunID))
	lifecycle.Finish()
	require.True(t, commentFinalized)
	require.False(t, commentComplete)
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

func TestRunHistoryPersistsSuppressedResultOutputWithoutPublicStream(t *testing.T) {
	writer := &recordingRunWriter{}
	history := newTestRunHistory(t, writer)
	ctx := testRunContext(t)
	lifecycle := history.Begin(ctx, runs.CommandDriftRemediation, runs.TriggerAPI)
	projectCtx := history.beginProject(command.ProjectContext{
		RunID: ctx.RunID, ProjectName: "network", RepoRelDir: "terraform/network",
		Workspace: "production", Log: ctx.Log, SuppressJobOutput: true,
	})
	longOutput := strings.Repeat("é", resultOutputChunkBytes)
	history.recordProject(projectCtx, command.Plan, command.ProjectCommandOutput{
		PlanSuccess: &models.PlanSuccess{TerraformOutput: longOutput + "\x00"},
	})
	history.recordProject(projectCtx, command.Apply, command.ProjectCommandOutput{
		ApplySuccess: "Apply complete! Resources: 1 added.",
	})
	lifecycle.Finish()

	require.Len(t, writer.output, 4)
	for i, chunk := range writer.output {
		require.Equal(t, projectCtx.ProjectRunID, chunk.ProjectRunID)
		require.Equal(t, int64(i), chunk.Sequence)
		require.LessOrEqual(t, len(chunk.Content), resultOutputChunkBytes)
		require.True(t, utf8.ValidString(chunk.Content))
	}
	require.Equal(t, runs.OutputStdout, writer.output[0].Stream)
	require.Contains(t, writer.output[2].Content, "�")
	require.Equal(t, "Apply complete! Resources: 1 added.\n", writer.output[3].Content)
}

func TestRunHistoryLeavesRunIncompleteWhenSuppressedOutputCannotPersist(t *testing.T) {
	writer := &recordingRunWriter{appendOutputErr: errors.New("store unavailable")}
	history := newTestRunHistory(t, writer)
	ctx := testRunContext(t)
	lifecycle := history.Begin(ctx, runs.CommandDriftDetection, runs.TriggerAPI)
	projectCtx := history.beginProject(command.ProjectContext{
		RunID: ctx.RunID, ProjectName: "network", RepoRelDir: "terraform/network",
		Workspace: "production", Log: ctx.Log, SuppressJobOutput: true,
	})
	history.recordProject(projectCtx, command.Plan, command.ProjectCommandOutput{
		PlanSuccess: &models.PlanSuccess{TerraformOutput: "only durable copy"},
	})

	lifecycle.Finish()

	require.Empty(t, writer.runsCompleted, "the running record must remain visibly incomplete")
}

func TestRunHistoryDoesNotDuplicateNormalStreamedOutput(t *testing.T) {
	writer := &recordingRunWriter{}
	history := newTestRunHistory(t, writer)
	ctx := testRunContext(t)
	lifecycle := history.Begin(ctx, runs.CommandPlan, runs.TriggerComment)
	projectCtx := history.beginProject(command.ProjectContext{
		RunID: ctx.RunID, ProjectName: "network", RepoRelDir: "terraform/network",
		Workspace: "production", Log: ctx.Log,
	})
	history.recordProject(projectCtx, command.Plan, command.ProjectCommandOutput{
		PlanSuccess: &models.PlanSuccess{TerraformOutput: "Plan: 1 to add."},
	})
	lifecycle.Finish()

	require.Empty(t, writer.output)
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

func TestRunHistoryRecordsSkippedOutcomeWithoutSuppressingHooks(t *testing.T) {
	writer := &recordingRunWriter{}
	history := newTestRunHistory(t, writer)
	ctx := testRunContext(t)
	lifecycle := history.Begin(ctx, runs.CommandApply, runs.TriggerComment)
	ctx.CommandOutcomeSkipped = true
	lifecycle.Finish()

	require.False(t, ctx.CommandSkipped)
	require.Equal(t, runs.StatusSkipped, writer.runsCompleted[0].Status)
}

func TestRunHistoryRecordsSupersededOwnershipAsCancelled(t *testing.T) {
	writer := &recordingRunWriter{}
	history := newTestRunHistory(t, writer)
	ctx := testRunContext(t)
	lifecycle := history.Begin(ctx, runs.CommandPlan, runs.TriggerComment)
	projectCtx := history.beginProject(command.ProjectContext{
		RunID: ctx.RunID, ProjectName: "network", RepoRelDir: "terraform/network",
		Workspace: "production", Log: ctx.Log,
	})
	history.recordProject(projectCtx, command.Plan, command.ProjectCommandOutput{
		Error: errors.New("ownership changed"), OwnershipLost: true,
	})
	ctx.CommandSuperseded = true
	lifecycle.Finish()

	require.Equal(t, runs.StatusCancelled, writer.projectsCompleted[0].Status)
	require.Equal(t, runs.StatusCancelled, writer.runsCompleted[0].Status)
}

func TestRunHistoryRetainsVCSDeliveryID(t *testing.T) {
	writer := &recordingRunWriter{}
	history := newTestRunHistory(t, writer)
	ctx := testRunContext(t)
	ctx.Pull.VCSDeliveryID = "delivery-123"

	history.Begin(ctx, runs.CommandPlan, runs.TriggerComment)

	var metadata map[string]any
	require.NoError(t, json.Unmarshal(writer.runsCreated[0].Metadata, &metadata))
	require.Equal(t, "delivery-123", metadata["vcs_delivery_id"])
}

func TestRunHistoryLeavesRunIncompleteWhenProjectCompletionCannotPersist(t *testing.T) {
	writer := &recordingRunWriter{completeProjectErr: errors.New("store unavailable")}
	history := newTestRunHistory(t, writer)
	ctx := testRunContext(t)
	enableHAContext(ctx)
	lifecycle := history.Begin(ctx, runs.CommandPlan, runs.TriggerComment)
	projectCtx := history.beginProject(command.ProjectContext{
		RunID: ctx.RunID, ProjectName: "network", RepoRelDir: "network", Workspace: "default", Log: ctx.Log,
	})
	history.recordProject(projectCtx, command.Plan, command.ProjectCommandOutput{PlanSuccess: &models.PlanSuccess{}})
	secondProject := history.beginProject(command.ProjectContext{
		RunID: ctx.RunID, ProjectName: "database", RepoRelDir: "database", Workspace: "default", Log: ctx.Log,
	})
	history.recordProject(secondProject, command.Plan, command.ProjectCommandOutput{PlanSuccess: &models.PlanSuccess{}})

	lifecycle.Finish()

	require.Empty(t, writer.runsCompleted, "the running record must remain visibly incomplete")
	require.Equal(t, 1, writer.completeProjectCalls, "one store outage must stop later completion writes")
	require.Len(t, writer.attemptsCompleted, 1, "attempt admission must not remain active")
	require.Equal(t, runs.AttemptSucceeded, writer.attemptsCompleted[0].Status)
}

func TestRunHistoryLeavesRunIncompleteWhenFinalOutputCannotPersist(t *testing.T) {
	writer := &recordingRunWriter{}
	history := newTestRunHistory(t, writer)
	history.SetOutputFinalizer(incompleteOutputFinalizer{})
	ctx := testRunContext(t)
	lifecycle := history.Begin(ctx, runs.CommandPlan, runs.TriggerComment)

	lifecycle.Finish()

	require.Empty(t, writer.runsCompleted, "the running record must remain visibly incomplete")
}

func TestRunHistoryDoesNotAuditCompletionWhenRunCompletionCannotPersist(t *testing.T) {
	writer := &recordingRunWriter{completeRunErr: errors.New("store unavailable")}
	history := newTestRunHistory(t, writer)
	ctx := testRunContext(t)
	lifecycle := history.Begin(ctx, runs.CommandPlan, runs.TriggerComment)

	lifecycle.Finish()

	require.Empty(t, writer.runsCompleted, "the running record must remain visibly incomplete")
	require.Equal(t, []string{"plan.requested"}, writer.auditTypes())
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

func TestRunHistoryRecordsRoutedExecutionAttempt(t *testing.T) {
	writer := &recordingRunWriter{}
	history := newTestRunHistory(t, writer)
	ctx := testRunContext(t)
	ctx.ExecutionInstanceID = "0198a0df-85f1-7d83-a60b-2e57b725c62d"
	ctx.ExecutionDeploymentID = "prod-eu"
	ctx.ConcurrencyKey = "sha256:pull-ownership-key"
	ctx.OwnershipClaimID = "claim-1"

	lifecycle := history.Begin(ctx, runs.CommandApply, runs.TriggerComment)
	require.True(t, lifecycle.CanExecute())
	require.NotEmpty(t, ctx.AttemptID)
	require.NotNil(t, ctx.SideEffectMarker)
	require.NoError(t, ctx.SideEffectMarker.MarkSideEffectStarted(context.Background()))
	lifecycle.Finish()

	require.Len(t, writer.attemptsCreated, 1)
	require.Equal(t, runs.AttemptClaimed, writer.attemptsCreated[0].Status)
	require.Equal(t, ctx.RunID, writer.attemptsCreated[0].RunID)
	require.Equal(t, ctx.ExecutionInstanceID, writer.attemptsCreated[0].InstanceID)
	require.Equal(t, ctx.ExecutionDeploymentID, writer.attemptsCreated[0].DeploymentID)
	require.Equal(t, []runs.ID{ctx.AttemptID}, writer.attemptsStarted)
	require.Equal(t, []runs.ID{ctx.AttemptID}, writer.sideEffectsStarted)
	require.Len(t, writer.attemptsCompleted, 1)
	require.Equal(t, runs.AttemptSucceeded, writer.attemptsCompleted[0].Status)
	require.Equal(t, []string{"apply.requested", "apply.attempt_started", "apply.completed"}, writer.auditTypes())
}

func TestRunHistoryFailsClosedWhenAttemptAdmissionIsNotDurable(t *testing.T) {
	writer := &recordingRunWriter{createAttemptErr: errors.New("database unavailable")}
	history := newTestRunHistory(t, writer)
	ctx := testRunContext(t)
	ctx.ExecutionInstanceID = "0198a0df-85f1-7d83-a60b-2e57b725c62d"
	ctx.ExecutionDeploymentID = "prod-eu"
	ctx.ConcurrencyKey = "sha256:pull-ownership-key"
	ctx.OwnershipClaimID = "claim-1"

	lifecycle := history.Begin(ctx, runs.CommandPlan, runs.TriggerComment)
	require.False(t, lifecycle.CanExecute())
	require.True(t, ctx.CommandHasErrors)
	lifecycle.Finish()

	require.Empty(t, writer.attemptsStarted)
	require.Equal(t, runs.StatusFailed, writer.runsCompleted[0].Status)
}

func TestRunHistoryMarksPanicAfterSideEffectUnknown(t *testing.T) {
	writer := &recordingRunWriter{}
	history := newTestRunHistory(t, writer)
	ctx := testRunContext(t)
	ctx.ExecutionInstanceID = "0198a0df-85f1-7d83-a60b-2e57b725c62d"
	ctx.ExecutionDeploymentID = "prod-eu"
	ctx.ConcurrencyKey = "sha256:pull-ownership-key"
	ctx.OwnershipClaimID = "claim-1"
	lifecycle := history.Begin(ctx, runs.CommandApply, runs.TriggerComment)
	require.NoError(t, ctx.SideEffectMarker.MarkSideEffectStarted(context.Background()))

	func() {
		defer func() { require.Equal(t, "boom", recover()) }()
		defer lifecycle.FinishRecovering()
		panic("boom")
	}()

	require.Equal(t, runs.StatusUnknown, writer.runsCompleted[0].Status)
	require.Equal(t, runs.AttemptUnknown, writer.attemptsCompleted[0].Status)
	require.Contains(t, writer.attemptsCompleted[0].FailureReason, "side effect")
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

func enableHAContext(ctx *command.Context) {
	ctx.ExecutionInstanceID = "0198a0df-85f1-7d83-a60b-2e57b725c62d"
	ctx.ExecutionDeploymentID = "prod-eu"
	ctx.ConcurrencyKey = "sha256:pull-ownership-key"
	ctx.OwnershipClaimID = "claim-1"
}

type recordingRunWriter struct {
	mu                   sync.Mutex
	runsCreated          []runs.Run
	runsCompleted        []runs.RunCompletion
	projectsCreated      []runs.ProjectRun
	projectsCompleted    []runs.ProjectRunCompletion
	output               []runs.OutputChunk
	audit                []runs.AuditEvent
	completeRunErr       error
	createProjectErr     error
	completeProjectErr   error
	completeProjectCalls int
	appendOutputErr      error
	createAttemptErr     error
	attemptsCreated      []runs.RunAttempt
	attemptsStarted      []runs.ID
	attemptHeartbeats    []runs.ID
	sideEffectsStarted   []runs.ID
	attemptsCompleted    []runs.AttemptCompletion
}

func (w *recordingRunWriter) CreateRun(_ context.Context, run runs.Run) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.runsCreated = append(w.runsCreated, run)
	return nil
}

func (w *recordingRunWriter) StartRun(context.Context, runs.ID, time.Time) error { return nil }

func (w *recordingRunWriter) CompleteRun(_ context.Context, completion runs.RunCompletion) error {
	if w.completeRunErr != nil {
		return w.completeRunErr
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.runsCompleted = append(w.runsCompleted, completion)
	return nil
}

func (w *recordingRunWriter) CreateProjectRun(_ context.Context, project runs.ProjectRun) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.createProjectErr != nil {
		return w.createProjectErr
	}
	w.projectsCreated = append(w.projectsCreated, project)
	return nil
}

func (w *recordingRunWriter) StartProjectRun(context.Context, runs.ID, time.Time) error { return nil }

func (w *recordingRunWriter) CompleteProjectRun(_ context.Context, completion runs.ProjectRunCompletion) error {
	w.mu.Lock()
	w.completeProjectCalls++
	w.mu.Unlock()
	if w.completeProjectErr != nil {
		return w.completeProjectErr
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.projectsCompleted = append(w.projectsCompleted, completion)
	return nil
}

type incompleteOutputFinalizer struct{}

func (incompleteOutputFinalizer) FinishRun(runs.ID) bool         { return false }
func (incompleteOutputFinalizer) RunOutputComplete(runs.ID) bool { return false }

var _ RunOutputFinalizer = incompleteOutputFinalizer{}

func (w *recordingRunWriter) AppendOutput(_ context.Context, chunks []runs.OutputChunk) error {
	if w.appendOutputErr != nil {
		return w.appendOutputErr
	}
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

func (w *recordingRunWriter) RegisterInstance(context.Context, runs.ExecutionInstance) error {
	return nil
}

func (w *recordingRunWriter) HeartbeatInstance(context.Context, runs.ID, time.Time) error {
	return nil
}

func (w *recordingRunWriter) StopInstance(context.Context, runs.ID, time.Time) error {
	return nil
}

func (w *recordingRunWriter) CreateAttempt(_ context.Context, attempt runs.RunAttempt) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.createAttemptErr != nil {
		return w.createAttemptErr
	}
	w.attemptsCreated = append(w.attemptsCreated, attempt)
	return nil
}

func (w *recordingRunWriter) StartAttempt(_ context.Context, id runs.ID, _ time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.attemptsStarted = append(w.attemptsStarted, id)
	return nil
}

func (w *recordingRunWriter) HeartbeatAttempt(_ context.Context, id runs.ID, _ time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.attemptHeartbeats = append(w.attemptHeartbeats, id)
	return nil
}

func (w *recordingRunWriter) MarkAttemptSideEffectStarted(_ context.Context, id runs.ID, _ time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sideEffectsStarted = append(w.sideEffectsStarted, id)
	return nil
}

func (w *recordingRunWriter) CompleteAttempt(_ context.Context, completion runs.AttemptCompletion) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.attemptsCompleted = append(w.attemptsCompleted, completion)
	return nil
}

func (w *recordingRunWriter) ReconcileAttempt(context.Context, runs.AttemptReconciliation) error {
	return nil
}
