// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/stretchr/testify/require"
)

func TestHistoryReplayPreservesNestedMetadata(t *testing.T) {
	testURL := os.Getenv("ATLANTIS_POSTGRES_TEST_URL")
	if testing.Short() || testURL == "" {
		t.Skip("requires ATLANTIS_POSTGRES_TEST_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, cleanup := newIsolatedStore(t, ctx, testURL)
	defer cleanup()
	for _, metadata := range []runs.Metadata{
		runs.Metadata(`{"nested":{"z":1,"a":2}}`),
		runs.Metadata(`{"array":[{"z":1,"a":2}]}`),
		runs.Metadata(`{"number":1e3}`),
		runs.Metadata(`{"number":9007199254740993}`),
	} {
		t.Run(string(metadata), func(t *testing.T) {
			run := runs.Run{ID: mustID(t), Repository: "org/infra", Command: runs.CommandPlan, Trigger: runs.TriggerComment, Status: runs.StatusPending, CreatedAt: time.Now(), Metadata: metadata}
			require.NoError(t, store.CreateRun(ctx, run))
			require.NoError(t, store.CreateRun(ctx, run), "replaying identical metadata must remain idempotent after JSONB serialization")
			project := runs.ProjectRun{ID: mustID(t), RunID: run.ID, ProjectName: "network", Directory: ".", Workspace: "default", Status: runs.StatusPending, Metadata: metadata}
			require.NoError(t, store.CreateProjectRun(ctx, project))
			require.NoError(t, store.CreateProjectRun(ctx, project))
			startedAt := run.CreatedAt.Add(time.Second)
			require.NoError(t, store.StartProjectRun(ctx, project.ID, startedAt))
			completion := runs.ProjectRunCompletion{ID: project.ID, Status: runs.StatusSucceeded, CompletedAt: startedAt.Add(time.Second), Metadata: metadata}
			require.NoError(t, store.CompleteProjectRun(ctx, completion))
			require.NoError(t, store.CompleteProjectRun(ctx, completion))
			event := runs.AuditEvent{ID: mustID(t), Repository: run.Repository, RunID: &run.ID, Actor: "operator", EventType: "test.replay", CreatedAt: run.CreatedAt, Metadata: metadata}
			require.NoError(t, store.AppendAuditEvent(ctx, event))
			require.NoError(t, store.AppendAuditEvent(ctx, event))
			run.Metadata = runs.Metadata(`{"different":true}`)
			require.ErrorIs(t, store.CreateRun(ctx, run), runs.ErrConflict)

		})
	}
}

func TestHistoryReplayDoesNotRoundLargeNumbers(t *testing.T) {
	testURL := os.Getenv("ATLANTIS_POSTGRES_TEST_URL")
	if testing.Short() || testURL == "" {
		t.Skip("requires ATLANTIS_POSTGRES_TEST_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, cleanup := newIsolatedStore(t, ctx, testURL)
	defer cleanup()
	run := runs.Run{ID: mustID(t), Repository: "org/infra", Command: runs.CommandPlan, Trigger: runs.TriggerComment, Status: runs.StatusPending, CreatedAt: time.Now(), Metadata: runs.Metadata(`{"number":9007199254740992}`)}
	require.NoError(t, store.CreateRun(ctx, run))
	run.Metadata = runs.Metadata(`{"number":9007199254740993}`)
	require.ErrorIs(t, store.CreateRun(ctx, run), runs.ErrConflict)
}
