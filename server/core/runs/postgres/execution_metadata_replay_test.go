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

func TestExecutionMetadataReplayJSONB(t *testing.T) {
	testURL := os.Getenv("ATLANTIS_POSTGRES_TEST_URL")
	if testing.Short() || testURL == "" {
		t.Skip("requires ATLANTIS_POSTGRES_TEST_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, cleanup := newIsolatedStore(t, ctx, testURL)
	defer cleanup()
	for _, tc := range []struct{ name, metadata, equivalent, different string }{
		{"nested", `{"nested":{"z":1,"a":[{"z":2,"a":3}]}}`, `{"nested":{"a":[{"a":3,"z":2}],"z":1}}`, `{"nested":{"z":2}}`},
		{"numeric exponent", `{"number":1e3}`, `{"number":1000.00}`, `{"number":1001}`},
		{"large integer", `{"number":9007199254740993}`, `{"number":9007199254740993.0}`, `{"number":9007199254740992}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now().UTC().Truncate(time.Microsecond)
			instance := runs.ExecutionInstance{ID: mustID(t), ReplicaID: "replica", DeploymentID: "metadata-replay", StartedAt: now, HeartbeatAt: now, Metadata: runs.Metadata(tc.metadata)}
			require.NoError(t, store.RegisterInstance(ctx, instance))
			t.Run("instance", func(t *testing.T) {
				require.NoError(t, store.RegisterInstance(ctx, instance), "identical instance replay after JSONB round trip")
				replay := instance
				replay.Metadata = runs.Metadata(tc.equivalent)
				require.NoError(t, store.RegisterInstance(ctx, replay))
				replay.Metadata = runs.Metadata(tc.different)
				require.ErrorIs(t, store.RegisterInstance(ctx, replay), runs.ErrConflict)
			})
			run := runs.Run{ID: mustID(t), Repository: "org/infra", Command: runs.CommandPlan, Trigger: runs.TriggerComment, Status: runs.StatusPending, CreatedAt: now}
			require.NoError(t, store.CreateRun(ctx, run))
			attempt := runs.RunAttempt{ID: mustID(t), RunID: run.ID, InstanceID: instance.ID, DeploymentID: instance.DeploymentID, ConcurrencyKey: string(run.ID), OwnershipClaimID: "claim", Status: runs.AttemptClaimed, ClaimedAt: now, HeartbeatAt: now, Metadata: runs.Metadata(tc.metadata)}
			require.NoError(t, store.CreateAttempt(ctx, attempt, now.Add(-time.Hour)))
			t.Run("admission", func(t *testing.T) {
				require.NoError(t, store.CreateAttempt(ctx, attempt, now.Add(-time.Hour)), "identical attempt replay after JSONB round trip")
				replay := attempt
				replay.Metadata = runs.Metadata(tc.equivalent)
				require.NoError(t, store.CreateAttempt(ctx, replay, now.Add(-time.Hour)))
				replay.Metadata = runs.Metadata(tc.different)
				require.ErrorIs(t, store.CreateAttempt(ctx, replay, now.Add(-time.Hour)), runs.ErrConflict)
			})
			// Completion carries no metadata; its own replay contract must keep
			// working for attempts whose metadata was normalized by PostgreSQL.
			require.NoError(t, store.StartAttempt(ctx, attempt.ID, now.Add(time.Second)))
			completion := runs.AttemptCompletion{ID: attempt.ID, Status: runs.AttemptSucceeded, CompletedAt: now.Add(2 * time.Second)}
			require.NoError(t, store.CompleteAttempt(ctx, completion))
			require.NoError(t, store.CompleteAttempt(ctx, completion))
			completion.CompletedAt = completion.CompletedAt.Add(time.Second)
			require.ErrorIs(t, store.CompleteAttempt(ctx, completion), runs.ErrConflict)
		})
	}
}
