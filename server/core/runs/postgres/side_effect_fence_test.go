// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/core/runs/postgres"
	"github.com/stretchr/testify/require"
)

func TestPlanSideEffectRequiresUnknownAttemptReconciliation(t *testing.T) {
	for _, supersede := range []bool{false, true} {
		t.Run(fmt.Sprintf("superseded=%t", supersede), func(t *testing.T) {
			ctx, store := sideEffectFenceStore(t)
			now := time.Now().UTC().Truncate(time.Microsecond)
			old := sideEffectFenceAttempt(t, ctx, store, runs.CommandApply, "deployment", "pull", now)
			require.NoError(t, store.CreateAttempt(ctx, old))
			require.NoError(t, store.StartAttempt(ctx, old.ID, now))
			require.NoError(t, store.MarkAttemptSideEffectStarted(ctx, old.ID, now))
			require.NoError(t, store.MarkAttemptSideEffectStarted(ctx, old.ID, now), "same-attempt marker replay is idempotent")
			if !supersede {
				require.NoError(t, store.CompleteAttempt(ctx, runs.AttemptCompletion{
					ID: old.ID, Status: runs.AttemptUnknown, CompletedAt: now.Add(time.Second),
					FailureReason: "worker lost after infrastructure mutation began",
				}))
			}

			plan := sideEffectFenceAttempt(t, ctx, store, runs.CommandPlan, "deployment", "pull", now.Add(2*time.Second))
			require.NoError(t, store.CreateAttempt(ctx, plan), "read-only plans remain available for reconciliation")
			require.NoError(t, store.StartAttempt(ctx, plan.ID, plan.ClaimedAt))
			markerAt := now.Add(3 * time.Second)
			require.ErrorIs(t, store.MarkAttemptSideEffectStarted(ctx, plan.ID, markerAt), runs.ErrConflict)
			require.ErrorIs(t, store.MarkAttemptSideEffectStarted(ctx, plan.ID, markerAt), runs.ErrConflict, "replaying a refused marker must not bypass the fence")
			persisted, err := store.GetAttempt(ctx, plan.ID)
			require.NoError(t, err)
			require.Equal(t, runs.AttemptRunning, persisted.Status)
			require.Nil(t, persisted.SideEffectStartedAt, "refused mutation must leave the plan read-only")
			require.ErrorIs(t, store.MarkAttemptSideEffectStarted(ctx, old.ID, now), runs.ErrConflict, "a terminal attempt cannot replay a successful marker")

			for _, scope := range []struct{ deployment, key string }{{"deployment", "other-pull"}, {"other-deployment", "pull"}} {
				other := sideEffectFenceAttempt(t, ctx, store, runs.CommandPlan, scope.deployment, scope.key, markerAt)
				require.NoError(t, store.CreateAttempt(ctx, other))
				require.NoError(t, store.StartAttempt(ctx, other.ID, markerAt))
				require.NoError(t, store.MarkAttemptSideEffectStarted(ctx, other.ID, markerAt), "unknown fence is scoped to deployment and concurrency key")
			}

			require.NoError(t, store.ReconcileAttempt(ctx, runs.AttemptReconciliation{
				ID: old.ID, AuditEventID: mustID(t), At: now.Add(4 * time.Second),
				Actor: "operator", Summary: "inspected state and confirmed mutation outcome",
			}))
			markerAt = now.Add(5 * time.Second)
			require.NoError(t, store.MarkAttemptSideEffectStarted(ctx, plan.ID, markerAt))
			require.NoError(t, store.MarkAttemptSideEffectStarted(ctx, plan.ID, markerAt))
			require.ErrorIs(t, store.MarkAttemptSideEffectStarted(ctx, plan.ID, markerAt.Add(time.Second)), runs.ErrConflict)
		})
	}
}

func TestSideEffectMarkerSerializesWithAttemptTakeover(t *testing.T) {
	ctx, store := sideEffectFenceStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	for i := range 20 {
		key := fmt.Sprintf("pull-%d", i)
		old := sideEffectFenceAttempt(t, ctx, store, runs.CommandPlan, "deployment", key, now)
		require.NoError(t, store.CreateAttempt(ctx, old))
		require.NoError(t, store.StartAttempt(ctx, old.ID, now))
		replacement := sideEffectFenceAttempt(t, ctx, store, runs.CommandPlan, "deployment", key, now.Add(time.Minute))
		start := make(chan struct{})
		marked := make(chan error, 1)
		admitted := make(chan error, 1)
		go func() {
			<-start
			marked <- store.MarkAttemptSideEffectStarted(ctx, old.ID, now)
		}()
		go func() {
			<-start
			admitted <- store.CreateAttempt(ctx, replacement)
		}()
		close(start)
		markErr, admitErr := <-marked, <-admitted
		require.NoError(t, admitErr)
		require.NoError(t, store.StartAttempt(ctx, replacement.ID, replacement.ClaimedAt))
		replacementErr := store.MarkAttemptSideEffectStarted(ctx, replacement.ID, replacement.ClaimedAt)
		retired, err := store.GetAttempt(ctx, old.ID)
		require.NoError(t, err)
		if markErr == nil {
			require.Equal(t, runs.AttemptUnknown, retired.Status)
			require.ErrorIs(t, replacementErr, runs.ErrConflict, "takeover after a mutation must fence the replacement plan's custom commands")
		} else {
			require.ErrorIs(t, markErr, runs.ErrConflict)
			require.Equal(t, runs.AttemptInterrupted, retired.Status)
			require.NoError(t, replacementErr, "takeover before mutation leaves no unknown outcome")
		}
	}
}

func sideEffectFenceStore(t *testing.T) (context.Context, *postgres.Store) {
	t.Helper()
	if testing.Short() {
		t.Skip("PostgreSQL side-effect fence test is disabled in short mode")
	}
	url := os.Getenv("ATLANTIS_POSTGRES_TEST_URL")
	if url == "" {
		t.Skip("ATLANTIS_POSTGRES_TEST_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	store, cleanup := newIsolatedStore(t, ctx, url)
	t.Cleanup(cleanup)
	return ctx, store
}

func sideEffectFenceAttempt(t *testing.T, ctx context.Context, store *postgres.Store, cmd runs.Command, deployment, key string, at time.Time) runs.RunAttempt {
	t.Helper()
	instanceID := mustID(t)
	require.NoError(t, store.RegisterInstance(ctx, runs.ExecutionInstance{
		ID: instanceID, ReplicaID: string(instanceID), DeploymentID: deployment, AdvertiseURL: "http://replica:4141",
		StartedAt: at, HeartbeatAt: at, Version: "test", Commit: "test",
	}))
	runID := mustID(t)
	pull := 42
	require.NoError(t, store.CreateRun(ctx, runs.Run{
		ID: runID, Repository: "github.com/org/repo", PullNumber: &pull, Command: cmd,
		Trigger: runs.TriggerComment, Actor: "operator", BaseRef: "main", HeadRef: "feature", HeadSHA: "deadbeef",
		Status: runs.StatusRunning, CreatedAt: at, StartedAt: &at,
	}))
	return runs.RunAttempt{
		ID: mustID(t), RunID: runID, InstanceID: instanceID, DeploymentID: deployment, ConcurrencyKey: key,
		OwnershipClaimID: string(instanceID), Status: runs.AttemptClaimed, ClaimedAt: at, HeartbeatAt: at,
	}
}
