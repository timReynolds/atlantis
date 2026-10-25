// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/core/runtime"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/require"
)

func TestRunHistoryPlanStoreRecordsExternalArtifactMetadata(t *testing.T) {
	writer := &recordingRunWriter{}
	history := newTestRunHistory(t, writer)
	runCtx := testRunContext(t)
	lifecycle := history.Begin(runCtx, runs.CommandPlan, runs.TriggerComment)
	projectCtx := history.beginProject(command.ProjectContext{
		RunID: runCtx.RunID, ProjectName: "network", RepoRelDir: "terraform/network",
		Workspace: "production", Log: runCtx.Log,
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
	require.Len(t, writer.projectsCompleted, 1)
	reference := writer.projectsCompleted[0].PlanArtifact
	require.NotNil(t, reference)
	require.Equal(t, delegate.key, reference.Key)
	require.Equal(t, fmt.Sprintf("sha256:%x", sha256.Sum256(planContent)), reference.Checksum)
	require.Equal(t, testHistoryTime, reference.CreatedAt)
}

func TestRunHistoryPlanStoreLeavesLocalStoreUnwrapped(t *testing.T) {
	local := &runtime.LocalPlanStore{}
	store := NewRunHistoryPlanStore(local, NewRunHistory(&recordingRunWriter{}, logging.NewNoopLogger(t)), logging.NewNoopLogger(t))
	require.Same(t, local, store)
}

type externalPlanStore struct {
	key   string
	saved bool
}

func (s *externalPlanStore) ArtifactKey(command.ProjectContext, string) string { return s.key }
func (s *externalPlanStore) Save(command.ProjectContext, string) error {
	s.saved = true
	return nil
}
func (s *externalPlanStore) Load(command.ProjectContext, string) error   { return nil }
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
