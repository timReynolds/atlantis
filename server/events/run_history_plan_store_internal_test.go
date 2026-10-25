// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/core/runtime"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/require"
)

func TestRunHistoryPlanStoreRecordsExternalArtifactMetadata(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		command runs.Command
		trigger runs.Trigger
	}{
		{name: "comment plan", command: runs.CommandPlan, trigger: runs.TriggerComment},
		{name: "drift detection", command: runs.CommandDriftDetection, trigger: runs.TriggerAPI},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			writer := &recordingRunWriter{}
			history := newTestRunHistory(t, writer)
			runCtx := testRunContext(t)
			lifecycle := history.Begin(runCtx, testCase.command, testCase.trigger)
			projectCtx := history.beginProject(command.ProjectContext{
				RunID: runCtx.RunID, ProjectName: "network", RepoRelDir: "terraform/network",
				Workspace: "production", WorkflowIdentity: testWorkflowIdentity, Log: runCtx.Log,
			})
			planContent := []byte("opaque terraform plan")
			planPath := filepath.Join(t.TempDir(), "network.tfplan")
			require.NoError(t, os.WriteFile(planPath, planContent, 0o600))
			delegate := &externalPlanStore{key: "plans/org/repo/42/production/network.tfplan"}
			store := NewRunHistoryPlanStore(delegate, history, logging.NewNoopLogger(t))
			store.(*RunHistoryPlanStore).now = func() time.Time { return testHistoryTime }

			require.NoError(t, store.Save(projectCtx, planPath))
			history.recordProject(projectCtx, command.Plan, command.ProjectCommandOutput{})
			lifecycle.Finish()

			require.True(t, delegate.saved)
			require.Len(t, writer.artifactUpdates, 1)
			require.Len(t, writer.projectsCompleted, 1)
			reference := writer.projectsCompleted[0].PlanArtifact
			require.NotNil(t, reference)
			require.Equal(t, delegate.key, reference.Key)
			require.Equal(t, fmt.Sprintf("sha256:%x", sha256.Sum256(planContent)), reference.Checksum)
			require.Equal(t, testHistoryTime, reference.CreatedAt)
		})
	}
}

func TestRunHistoryPlanStoreRecordsPlanInsideAPIApplyLifecycle(t *testing.T) {
	writer := &recordingRunWriter{}
	history := newTestRunHistory(t, writer)
	runCtx := testRunContext(t)
	runCtx.API = true
	lifecycle := history.Begin(runCtx, runs.CommandApply, runs.TriggerAPI)
	defer lifecycle.Finish()
	projectCtx := history.beginProject(command.ProjectContext{
		RunID: runCtx.RunID, ProjectName: "network", RepoRelDir: "terraform/network",
		Workspace: "production", WorkflowIdentity: testWorkflowIdentity, Log: runCtx.Log,
	})
	planPath := filepath.Join(t.TempDir(), "network.tfplan")
	require.NoError(t, os.WriteFile(planPath, []byte("api apply plan"), 0o600))
	delegate := &externalPlanStore{key: "plans/api-apply/network.tfplan"}
	store := NewRunHistoryPlanStore(delegate, history, logging.NewNoopLogger(t))

	require.NoError(t, store.Save(projectCtx, planPath))
	require.True(t, delegate.saved)
	require.Len(t, writer.artifactUpdates, 1)
}

func TestRunHistoryPlanStoreRecordsPlanInsideDriftRemediationLifecycle(t *testing.T) {
	writer := &recordingRunWriter{}
	history := newTestRunHistory(t, writer)
	runCtx := testRunContext(t)
	runCtx.API = true
	lifecycle := history.Begin(runCtx, runs.CommandDriftRemediation, runs.TriggerAPI)
	defer lifecycle.Finish()
	projectCtx := history.beginProject(command.ProjectContext{
		RunID: runCtx.RunID, ProjectName: "network", RepoRelDir: "terraform/network",
		Workspace: "production", WorkflowIdentity: testWorkflowIdentity, Log: runCtx.Log,
	})
	planPath := filepath.Join(t.TempDir(), "network.tfplan")
	require.NoError(t, os.WriteFile(planPath, []byte("drift remediation plan"), 0o600))
	delegate := &externalPlanStore{key: "plans/drift-remediation/network.tfplan"}
	store := NewRunHistoryPlanStore(delegate, history, logging.NewNoopLogger(t))

	require.NoError(t, store.Save(projectCtx, planPath))
	require.True(t, delegate.saved)
	require.Len(t, writer.artifactUpdates, 1)
}

func TestRunHistoryPlanStoreLeavesLocalStoreUnwrapped(t *testing.T) {
	local := &runtime.LocalPlanStore{}
	store := NewRunHistoryPlanStore(local, NewRunHistory(&recordingRunWriter{}, logging.NewNoopLogger(t)), logging.NewNoopLogger(t))
	require.Same(t, local, store)
}

func TestRunHistoryPlanStoreFailsClosedBeforeUploadWhenExpectationWriteFails(t *testing.T) {
	writer := &recordingRunWriter{artifactErr: errors.New("postgres unavailable")}
	history := newTestRunHistory(t, writer)
	runCtx := testRunContext(t)
	lifecycle := history.Begin(runCtx, runs.CommandPlan, runs.TriggerComment)
	defer lifecycle.Finish()
	projectCtx := history.beginProject(command.ProjectContext{
		RunID: runCtx.RunID, ProjectName: "network", RepoRelDir: "terraform/network",
		Workspace: "production", WorkflowIdentity: testWorkflowIdentity, Log: runCtx.Log,
	})
	planPath := filepath.Join(t.TempDir(), "network.tfplan")
	require.NoError(t, os.WriteFile(planPath, []byte("plan"), 0o600))
	delegate := &externalPlanStore{key: "plans/network.tfplan"}
	store := NewRunHistoryPlanStore(delegate, history, logging.NewNoopLogger(t))

	err := store.Save(projectCtx, planPath)

	require.ErrorContains(t, err, "postgres unavailable")
	require.False(t, delegate.saved)
}

func TestRunHistoryPlanStoreLoadValidatesIndependentDurableChecksum(t *testing.T) {
	content := []byte("exact external plan")
	checksum := fmt.Sprintf("sha256:%x", sha256.Sum256(content))
	ctx := command.ProjectContext{
		BaseRepo:    models.Repo{Owner: "org", Name: "repo", FullName: "org/repo"},
		Pull:        models.PullRequest{Num: 42, HeadCommit: "abc123"},
		ProjectName: "network", RepoRelDir: "terraform/network", Workspace: "production",
		ProjectRunID:      "0198a0df-85f1-7d83-a60b-2e57b725c621",
		RepoConfigVersion: 3, WorkflowIdentity: testWorkflowIdentity,
	}
	delegate := &externalPlanStore{key: "plans/org/repo/42/production/network.tfplan", loadContent: content}
	writer := &recordingRunWriter{artifactResult: runs.PlanArtifactExpectation{
		RunID:        "0198a0df-85f1-7d83-a60b-2e57b725c620",
		ProjectRunID: "0198a0df-85f1-7d83-a60b-2e57b725c621",
		Artifact:     runs.ArtifactReference{Key: delegate.key, Checksum: checksum, CreatedAt: testHistoryTime},
		Identity:     runs.PlanArtifactIdentity{RepoConfigVersion: 3, WorkflowChecksum: testWorkflowIdentity},
	}}
	store := NewRunHistoryPlanStore(delegate, NewRunHistory(writer, logging.NewNoopLogger(t)), logging.NewNoopLogger(t))
	planPath := filepath.Join(t.TempDir(), "network.tfplan")

	require.NoError(t, store.Load(ctx, planPath))
	require.True(t, delegate.loaded)
	require.Equal(t, ctx.ProjectRunID, writer.artifactLookup.ProjectRunID)

	writer.artifactResult.Artifact.Checksum = testPlanChecksum
	err := store.Load(ctx, planPath)
	require.ErrorContains(t, err, "checksum does not match durable history")
	require.NoFileExists(t, planPath)
}

func TestRunHistoryPlanStoreRemovesRestoredFileWhenDelegateValidationFails(t *testing.T) {
	planPath := filepath.Join(t.TempDir(), "rejected.tfplan")
	delegate := &externalPlanStore{
		key: "plans/rejected.tfplan", loadContent: []byte("previously restored plan"),
		loadErr: errors.New("S3 object identity does not match"),
	}
	store := NewRunHistoryPlanStore(delegate, NewRunHistory(&recordingRunWriter{}, logging.NewNoopLogger(t)), logging.NewNoopLogger(t))

	err := store.Load(command.ProjectContext{}, planPath)

	require.ErrorContains(t, err, "S3 object identity does not match")
	require.NoFileExists(t, planPath)
}

type externalPlanStore struct {
	key         string
	saved       bool
	loaded      bool
	loadContent []byte
	loadErr     error
}

func (s *externalPlanStore) ArtifactKey(command.ProjectContext, string) string { return s.key }
func (s *externalPlanStore) Save(command.ProjectContext, string) error {
	s.saved = true
	return nil
}
func (s *externalPlanStore) Load(_ command.ProjectContext, path string) error {
	s.loaded = true
	if s.loadContent == nil {
		return s.loadErr
	}
	if err := os.WriteFile(path, s.loadContent, 0o600); err != nil {
		return err
	}
	return s.loadErr
}
func (s *externalPlanStore) Remove(command.ProjectContext, string) error { return nil }
func (s *externalPlanStore) ListWorkspaces(string, string, int) ([]string, error) {
	return nil, nil
}
func (s *externalPlanStore) RestorePlans(string, string, string, int) error { return nil }
func (s *externalPlanStore) DeleteForPull(string, string, int) error        { return nil }
func (s *externalPlanStore) DeletePlanForProject(string, string, int, string, string, string) error {
	return nil
}

var _ runtime.PlanStore = (*externalPlanStore)(nil)
