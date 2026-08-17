// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package controllers

import (
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/core/drift"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/stretchr/testify/require"
)

func TestNewDriftDetectionRecordPreservesEveryProjectOutcome(t *testing.T) {
	startedAt := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	result := &models.DriftDetectionResult{
		ID: "019c0000-0000-7000-8000-000000000000", Repository: "org/repo",
		TotalProjects: 3, ProjectsWithDrift: 1,
		Projects: []models.ProjectDrift{
			{ProjectName: "drifted", Drift: models.DriftSummary{HasDrift: true}},
			{ProjectName: "failed", Error: "provider unavailable"},
			{ProjectName: "locked", Error: "project is currently locked by another plan"},
		},
	}
	record := newDriftDetectionRecord(
		result, "github.com/org/repo", "main", "main", "deadbeef",
		result.ID, startedAt, startedAt.Add(time.Minute), false,
	)

	require.Equal(t, drift.DetectionStatusPartial, record.Run.Status)
	require.Equal(t, 1, record.Run.ProjectsWithDrift)
	require.Equal(t, 1, record.Run.FailedProjects)
	require.Equal(t, 1, record.Run.LockedProjects)
	require.Len(t, record.Projects, 3)
	require.Equal(t, drift.DetectionOutcomeDrifted, record.Projects[0].Outcome)
	require.Equal(t, drift.DetectionOutcomeFailed, record.Projects[1].Outcome)
	require.Equal(t, drift.DetectionOutcomeLocked, record.Projects[2].Outcome)
}

func TestNewDriftDetectionRecordMarksRunLevelFailure(t *testing.T) {
	startedAt := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	result := &models.DriftDetectionResult{
		ID: "019c0000-0000-7000-8000-000000000000", Repository: "org/repo",
		TotalProjects: 1, Projects: []models.ProjectDrift{{ProjectName: "network"}},
	}
	record := newDriftDetectionRecord(
		result, "github.com/org/repo", "main", "main", "deadbeef",
		result.ID, startedAt, startedAt.Add(time.Minute), true,
	)

	require.Equal(t, drift.DetectionStatusFailed, record.Run.Status)
	require.Equal(t, drift.DetectionOutcomeClean, record.Projects[0].Outcome)
}

func TestDriftProjectsFromCommandResultDeduplicatesProjectIdentity(t *testing.T) {
	result := &command.Result{ProjectResults: []command.ProjectResult{
		{
			Command: command.Plan, ProjectName: "network", RepoRelDir: "terraform/network",
			Workspace: "production", ProjectCommandOutput: command.ProjectCommandOutput{
				PlanSuccess: &models.PlanSuccess{TerraformOutput: "stale plan"},
			},
		},
		{
			Command: command.Plan, ProjectName: "network", RepoRelDir: "terraform/network",
			Workspace: "production", ProjectCommandOutput: command.ProjectCommandOutput{
				Failure: "latest execution failed",
			},
		},
	}}

	projects := driftProjectsFromCommandResult(result, "main", "main", "deadbeef", "detection-1", time.Now())

	require.Len(t, projects, 1)
	require.Equal(t, "latest execution failed", projects[0].Error)
}
