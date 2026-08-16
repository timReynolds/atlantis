// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package drift_test

import (
	"testing"

	"github.com/runatlantis/atlantis/server/core/drift"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/stretchr/testify/require"
)

func TestOutcomeForProjectPreservesPartialStates(t *testing.T) {
	tests := []struct {
		name    string
		project models.ProjectDrift
		outcome drift.DetectionOutcome
	}{
		{name: "clean", outcome: drift.DetectionOutcomeClean},
		{name: "drifted", project: models.ProjectDrift{Drift: models.DriftSummary{HasDrift: true}}, outcome: drift.DetectionOutcomeDrifted},
		{name: "failed", project: models.ProjectDrift{Error: "provider unavailable"}, outcome: drift.DetectionOutcomeFailed},
		{name: "locked", project: models.ProjectDrift{Error: "project is currently locked by a plan"}, outcome: drift.DetectionOutcomeLocked},
		{name: "lock prefix", project: models.ProjectDrift{Error: "lock acquisition failed"}, outcome: drift.DetectionOutcomeLocked},
		{name: "locked before skipped", project: models.ProjectDrift{Error: "skipped because the project is locked"}, outcome: drift.DetectionOutcomeLocked},
		{name: "skipped", project: models.ProjectDrift{Error: "project was skipped"}, outcome: drift.DetectionOutcomeSkipped},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.outcome, drift.OutcomeForProject(test.project))
		})
	}
}

func TestInMemoryRemediationResultStoreDefensiveCopies(t *testing.T) {
	store := drift.NewInMemoryRemediationResultStore()
	result := models.NewRemediationResult("id", "owner/repo", "main", models.RemediationPlanOnly)
	result.Projects = []models.ProjectRemediationResult{{ProjectName: "network"}}
	require.NoError(t, store.Put(result))
	result.Projects[0].ProjectName = "mutated"

	stored, err := store.GetResult("id")
	require.NoError(t, err)
	require.Equal(t, "network", stored.Projects[0].ProjectName)
	stored.Projects[0].ProjectName = "mutated again"
	again, err := store.GetResult("id")
	require.NoError(t, err)
	require.Equal(t, "network", again.Projects[0].ProjectName)
}

func TestRemediationServiceUsesPreallocatedRunID(t *testing.T) {
	store := drift.NewInMemoryRemediationResultStore()
	service := drift.NewRemediationService(nil, store)
	const runID = "019c0000-0000-7000-8000-000000000000"
	result, err := service.Remediate(models.RemediationRequest{
		RunID: runID, Repository: "owner/repo", Ref: "main", Action: models.RemediationPlanOnly,
	}, noExecutionRemediationExecutor{})
	require.NoError(t, err)
	require.Equal(t, runID, result.ID)
	require.Equal(t, runID, result.RunID)
	stored, err := store.GetResult(runID)
	require.NoError(t, err)
	require.Equal(t, runID, stored.RunID)
}

type noExecutionRemediationExecutor struct{}

func (noExecutionRemediationExecutor) ExecutePlan(string, string, string, string, string, string) (string, *models.DriftSummary, error) {
	panic("no projects should execute")
}

func (noExecutionRemediationExecutor) ExecuteApplyProjects(string, string, string, []models.ProjectDrift) ([]models.ProjectRemediationResult, error) {
	panic("no projects should execute")
}
