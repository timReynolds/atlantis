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
	runspostgres "github.com/runatlantis/atlantis/server/core/runs/postgres"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/stretchr/testify/require"
)

func TestHistoryRepositoryFilterTreatsNamesLiterally(t *testing.T) {
	testURL := os.Getenv("ATLANTIS_POSTGRES_TEST_URL")
	if testing.Short() || testURL == "" {
		t.Skip("requires ATLANTIS_POSTGRES_TEST_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	storeURL, _, cleanup := newIsolatedDatabase(t, ctx, testURL)
	defer cleanup()
	store, err := runspostgres.New(ctx, runspostgres.Config{URL: storeURL})
	require.NoError(t, err)
	defer store.Close()
	history := driftpostgres.NewHistoryStore(store.Database(), 5*time.Second)
	now := time.Now().UTC().Truncate(time.Microsecond)
	for _, name := range []string{"org/infra_prod", "org/infraXprod", "org/infra%prod", "org/infraLONGprod"} {
		repository := "github.com/" + name
		id := string(mustHistoryID(t))
		require.NoError(t, history.RecordDetectionWithLatest(ctx, repository, drift.DetectionRecord{
			Run:      drift.DetectionRun{ID: id, Repository: repository, DisplayRepository: name, Ref: "main", BaseBranch: "main", Status: drift.DetectionStatusSucceeded, StartedAt: now, CompletedAt: now, TotalProjects: 1},
			Projects: []drift.DetectionProject{{DetectionID: id, Ordinal: 0, Outcome: drift.DetectionOutcomeClean, Project: models.ProjectDrift{ProjectName: "network", Path: ".", Workspace: "default", Ref: "main", BaseBranch: "main", DetectionID: id, LastChecked: now}}},
		}, false))
	}
	for _, name := range []string{"org/infra_prod", "org/infra%prod"} {
		t.Run(name, func(t *testing.T) {
			current, err := history.ListCurrentStatus(ctx, drift.CurrentStatusFilter{Repository: name}, drift.HistoryPageRequest{Limit: 100})
			require.NoError(t, err)
			require.Len(t, current.Projects, 1, "repository names must not be SQL LIKE patterns")
			require.Equal(t, "github.com/"+name, current.Projects[0].Repository)
			detections, err := history.ListDetections(ctx, name, drift.HistoryPageRequest{Limit: 100})
			require.NoError(t, err)
			require.Len(t, detections.Runs, 1)
			require.Equal(t, "github.com/"+name, detections.Runs[0].Repository)
		})
	}
}
