// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/core/drift"
	driftpostgres "github.com/runatlantis/atlantis/server/core/drift/postgres"
	"github.com/runatlantis/atlantis/server/core/runs"
	runspostgres "github.com/runatlantis/atlantis/server/core/runs/postgres"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/stretchr/testify/require"
)

// TestDurableHistoryConformance proves append-only detection, artifact
// correlation, remediation summaries, current-state enrichment, and restart
// durability against a real PostgreSQL server.
func TestDurableHistoryConformance(t *testing.T) {
	if testing.Short() {
		t.Skip("PostgreSQL conformance test is disabled in short mode")
	}
	testURL := os.Getenv("ATLANTIS_POSTGRES_TEST_URL")
	if testURL == "" {
		t.Skip("ATLANTIS_POSTGRES_TEST_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	storeURL, _, cleanup := newIsolatedDatabase(t, ctx, testURL)
	defer cleanup()

	runStore, err := runspostgres.New(ctx, runspostgres.Config{URL: storeURL, OperationTimeout: 5 * time.Second})
	require.NoError(t, err)
	history := driftpostgres.NewHistoryStore(runStore.Database(), 5*time.Second)
	latest := driftpostgres.New(runStore.Database(), 5*time.Second)
	remediations := driftpostgres.NewRemediationStore(runStore.Database(), 5*time.Second)

	startedAt := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Microsecond)
	completedAt := startedAt.Add(time.Minute)
	runID := mustHistoryID(t)
	projectRunID := mustHistoryID(t)
	require.NoError(t, runStore.CreateRun(ctx, runs.Run{
		ID: runID, Repository: "github.com/example/infrastructure",
		Command: runs.CommandDriftDetection, Trigger: runs.TriggerAPI,
		Status: runs.StatusRunning, CreatedAt: startedAt, StartedAt: &startedAt,
	}))
	require.NoError(t, runStore.CreateProjectRun(ctx, runs.ProjectRun{
		ID: projectRunID, RunID: runID, ProjectName: "network",
		Directory: "terraform/network", Workspace: "production",
		Status: runs.StatusRunning, StartedAt: &startedAt,
	}))
	require.NoError(t, runStore.CompleteProjectRun(ctx, runs.ProjectRunCompletion{
		ID: projectRunID, Status: runs.StatusSucceeded, CompletedAt: completedAt,
		PlanArtifact: &runs.ArtifactReference{
			Key: "plans/network.tfplan", Checksum: "sha256:abc", CreatedAt: startedAt,
		},
	}))

	drifted := models.ProjectDrift{
		ProjectName: "network", Path: "terraform/network", Workspace: "production",
		Ref: "main", BaseBranch: "main", ResolvedCommit: "deadbeef", DetectionID: string(runID),
		Drift:       models.DriftSummary{HasDrift: true, ToAdd: 1, Summary: "1 to add"},
		LastChecked: completedAt,
	}
	locked := models.ProjectDrift{
		ProjectName: "database", Path: "terraform/database", Workspace: "production",
		Ref: "main", BaseBranch: "main", ResolvedCommit: "deadbeef", DetectionID: string(runID),
		Error: "lock acquisition failed", LastChecked: completedAt,
	}
	require.NoError(t, history.RecordDetectionWithLatest(ctx, "github.com/example/infrastructure", drift.DetectionRecord{
		Run: drift.DetectionRun{
			ID: string(runID), RunID: string(runID), Repository: "github.com/example/infrastructure",
			DisplayRepository: "example/infrastructure", Ref: "main", BaseBranch: "main",
			ResolvedCommit: "deadbeef", Status: drift.DetectionStatusPartial,
			StartedAt: startedAt, CompletedAt: completedAt, TotalProjects: 2,
			ProjectsWithDrift: 1, LockedProjects: 1,
		},
		Projects: []drift.DetectionProject{
			{DetectionID: string(runID), Ordinal: 0, Project: drifted, Outcome: drift.DetectionOutcomeDrifted},
			{DetectionID: string(runID), Ordinal: 1, Project: locked, Outcome: drift.DetectionOutcomeLocked},
		},
	}, false))

	// A history insert error must roll back latest-state changes from the same
	// transaction. Reusing the immutable detection ID forces that late error.
	rollbackProject := drifted
	rollbackProject.ProjectName = "must-roll-back"
	rollbackProject.Path = "terraform/must-roll-back"
	require.Error(t, history.RecordDetectionWithLatest(ctx, "github.com/example/infrastructure", drift.DetectionRecord{
		Run: drift.DetectionRun{
			ID: string(runID), RunID: string(runID), Repository: "github.com/example/infrastructure",
			DisplayRepository: "example/infrastructure", Ref: "main", BaseBranch: "main",
			ResolvedCommit: "deadbeef", Status: drift.DetectionStatusSucceeded,
			StartedAt: startedAt, CompletedAt: completedAt, TotalProjects: 1,
		},
		Projects: []drift.DetectionProject{{
			DetectionID: string(runID), Ordinal: 0, Project: rollbackProject,
			Outcome: drift.DetectionOutcomeDrifted,
		}},
	}, false))
	rolledBack, err := latest.Get("github.com/example/infrastructure", drift.GetOptions{ProjectName: "must-roll-back"})
	require.NoError(t, err)
	require.Empty(t, rolledBack)

	remediationID := mustHistoryID(t)
	remediation := models.NewRemediationResult(string(remediationID), "example/infrastructure", "main", models.RemediationPlanOnly)
	remediation.StorageRepository = "github.com/example/infrastructure"
	remediation.BaseBranch = "main"
	remediation.StartedAt = completedAt.Add(time.Minute)
	remediation.Status = models.RemediationStatusSuccess
	remediation.Projects = []models.ProjectRemediationResult{{
		ProjectName: "network", Path: "terraform/network", Workspace: "production",
		Status: models.RemediationStatusSuccess, PlanOutput: "must not be duplicated",
		DriftBefore: &drifted.Drift, DriftAfter: &models.DriftSummary{HasDrift: false},
	}}
	remediation.Complete()
	require.NoError(t, remediations.Put(remediation))

	detection, page, err := history.GetDetection(ctx, string(runID), drift.HistoryPageRequest{Limit: 100})
	require.NoError(t, err)
	require.Equal(t, drift.DetectionStatusPartial, detection.Status)
	require.Len(t, page.Projects, 2)
	require.Equal(t, "plans/network.tfplan", page.Projects[0].ArtifactKey)
	require.Equal(t, "sha256:abc", page.Projects[0].ArtifactChecksum)

	current, err := history.ListCurrentStatus(ctx, drift.CurrentStatusFilter{
		Repository: "example/infrastructure",
	}, drift.HistoryPageRequest{Limit: 100})
	require.NoError(t, err)
	require.Len(t, current.Projects, 2)
	currentByName := indexCurrentByProject(current.Projects)
	require.Equal(t, drift.DetectionOutcomeLocked, currentByName["database"].Outcome)
	require.Equal(t, string(remediationID), currentByName["network"].LastRemediationID)

	// Recreate all adapters over a reopened pool to model an Atlantis restart.
	require.NoError(t, runStore.Close())
	runStore, err = runspostgres.New(ctx, runspostgres.Config{URL: storeURL, OperationTimeout: 5 * time.Second})
	require.NoError(t, err)
	history = driftpostgres.NewHistoryStore(runStore.Database(), 5*time.Second)
	remediations = driftpostgres.NewRemediationStore(runStore.Database(), 5*time.Second)
	detections, err := history.ListDetections(ctx, "example/infrastructure", drift.HistoryPageRequest{Limit: 20})
	require.NoError(t, err)
	require.Len(t, detections.Runs, 1)
	storedRemediation, err := remediations.GetResult(string(remediationID))
	require.NoError(t, err)
	require.Len(t, storedRemediation.Projects, 1)
	require.Empty(t, storedRemediation.Projects[0].PlanOutput, "raw output belongs to generic RunStore")
	require.Equal(t, remediation.Projects[0].DriftBefore, storedRemediation.Projects[0].DriftBefore)
	require.NoError(t, runStore.Close())
}

func mustHistoryID(t *testing.T) runs.ID {
	t.Helper()
	id, err := runs.NewID()
	require.NoError(t, err)
	return id
}

func indexCurrentByProject(projects []drift.CurrentProjectStatus) map[string]drift.CurrentProjectStatus {
	indexed := make(map[string]drift.CurrentProjectStatus, len(projects))
	for _, project := range projects {
		indexed[project.Project.ProjectName] = project
	}
	return indexed
}
