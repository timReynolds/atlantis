// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/core/runs/postgres"
	"github.com/stretchr/testify/require"
)

// TestStoreConformance exercises the complete adapter against a real
// PostgreSQL server. It is opt-in so unit-test environments do not require a
// database. Example:
//
//	ATLANTIS_POSTGRES_TEST_URL=postgres://localhost/atlantis_test go test ./server/core/runs/postgres
func TestStoreConformance(t *testing.T) {
	if testing.Short() {
		t.Skip("PostgreSQL conformance test is disabled in short mode")
	}
	testURL := os.Getenv("ATLANTIS_POSTGRES_TEST_URL")
	if testURL == "" {
		t.Skip("ATLANTIS_POSTGRES_TEST_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, cleanup := newIsolatedStore(t, ctx, testURL)
	defer cleanup()

	createdAt := time.Now().UTC().Truncate(time.Microsecond)
	runID := mustID(t)
	projectRunID := mustID(t)
	auditID := mustID(t)
	pull := 17
	run := runs.Run{
		ID: runID, Repository: "example/infrastructure", PullNumber: &pull,
		Command: runs.CommandPlan, Trigger: runs.TriggerComment, Actor: "operator",
		BaseRef: "main", HeadRef: "feature", HeadSHA: "deadbeef",
		Status: runs.StatusPending, CreatedAt: createdAt,
		Metadata: runs.Metadata(`{"source":"conformance"}`),
	}
	require.NoError(t, store.CreateRun(ctx, run))
	require.NoError(t, store.CreateRun(ctx, run), "create replay must be idempotent")

	startedAt := createdAt.Add(time.Second)
	require.NoError(t, store.StartRun(ctx, runID, startedAt))
	require.NoError(t, store.StartRun(ctx, runID, startedAt), "start replay must be idempotent")

	instanceID := mustID(t)
	instance := runs.ExecutionInstance{
		ID: instanceID, ReplicaID: "atlantis-0", DeploymentID: "conformance",
		AdvertiseURL: "http://atlantis-0.atlantis:4141", StartedAt: createdAt,
		HeartbeatAt: createdAt, Version: "test", Commit: "deadbeef",
	}
	require.NoError(t, store.RegisterInstance(ctx, instance))
	require.NoError(t, store.RegisterInstance(ctx, instance), "instance replay must be idempotent")
	require.NoError(t, store.HeartbeatInstance(ctx, instanceID, startedAt))
	storedInstance, err := store.GetInstance(ctx, instanceID)
	require.NoError(t, err)
	require.Equal(t, startedAt, storedInstance.HeartbeatAt)

	attemptID := mustID(t)
	attempt := runs.RunAttempt{
		ID: attemptID, RunID: runID, InstanceID: instanceID,
		DeploymentID:   "conformance",
		ConcurrencyKey: "sha256:conformance-pull", OwnershipClaimID: "claim-1",
		Status: runs.AttemptClaimed, ClaimedAt: createdAt, HeartbeatAt: createdAt,
	}
	require.NoError(t, store.CreateAttempt(ctx, attempt))
	require.NoError(t, store.CreateAttempt(ctx, attempt), "attempt replay must be idempotent")
	otherInstance := instance
	otherInstance.ID = mustID(t)
	otherInstance.ReplicaID = "atlantis-other-0"
	otherInstance.DeploymentID = "other-deployment"
	require.NoError(t, store.RegisterInstance(ctx, otherInstance))
	otherAttempt := attempt
	otherAttempt.ID = mustID(t)
	otherAttempt.InstanceID = otherInstance.ID
	otherAttempt.DeploymentID = otherInstance.DeploymentID
	require.NoError(t, store.CreateAttempt(ctx, otherAttempt), "another deployment may use the same concurrency key")
	require.NoError(t, store.CompleteAttempt(ctx, runs.AttemptCompletion{
		ID: otherAttempt.ID, Status: runs.AttemptInterrupted, CompletedAt: startedAt,
		FailureReason: "conformance cleanup before execution",
	}))
	require.NoError(t, store.StartAttempt(ctx, attemptID, startedAt))
	sideEffectAt := startedAt.Add(250 * time.Millisecond)
	require.NoError(t, store.MarkAttemptSideEffectStarted(ctx, attemptID, sideEffectAt))
	require.NoError(t, store.HeartbeatAttempt(ctx, attemptID, sideEffectAt.Add(250*time.Millisecond)))
	attemptCompletedAt := startedAt.Add(750 * time.Millisecond)
	require.NoError(t, store.CompleteAttempt(ctx, runs.AttemptCompletion{
		ID: attemptID, Status: runs.AttemptUnknown, CompletedAt: attemptCompletedAt,
		FailureReason: "simulated process loss after apply started",
	}))
	replacementAttempt := attempt
	replacementAttempt.ID = mustID(t)
	replacementAttempt.OwnershipClaimID = "claim-2"
	require.ErrorIs(t, store.CreateAttempt(ctx, replacementAttempt), runs.ErrConflict,
		"unreconciled unknown apply must retain the durable admission fence")
	reconciledAt := startedAt.Add(900 * time.Millisecond)
	require.NoError(t, store.ReconcileAttempt(ctx, runs.AttemptReconciliation{
		ID: attemptID, At: reconciledAt, Actor: "operator",
		Summary: "state inspected; fresh plan required",
	}))
	require.NoError(t, store.CreateAttempt(ctx, replacementAttempt),
		"operator reconciliation must release the durable admission fence")
	storedAttempt, err := store.GetAttempt(ctx, attemptID)
	require.NoError(t, err)
	require.Equal(t, runs.AttemptUnknown, storedAttempt.Status)
	require.Equal(t, "operator", storedAttempt.ReconciledBy)
	attemptPage, err := store.ListRunAttempts(ctx, runID, runs.PageRequest{Limit: 1})
	require.NoError(t, err)
	require.Len(t, attemptPage.Attempts, 1)
	replacementStartedAt := startedAt.Add(901 * time.Millisecond)
	replacementSideEffectAt := startedAt.Add(925 * time.Millisecond)
	replacementCompletedAt := startedAt.Add(950 * time.Millisecond)
	require.NoError(t, store.StartAttempt(ctx, replacementAttempt.ID, replacementStartedAt))
	require.NoError(t, store.MarkAttemptSideEffectStarted(ctx, replacementAttempt.ID, replacementSideEffectAt))
	require.NoError(t, store.CompleteAttempt(ctx, runs.AttemptCompletion{
		ID: replacementAttempt.ID, Status: runs.AttemptUnknown, CompletedAt: replacementCompletedAt,
		FailureReason: "simulated second process loss after apply started",
	}))

	projectRun := runs.ProjectRun{
		ID: projectRunID, RunID: runID, ProjectName: "network",
		Directory: "terraform/network", Workspace: "production",
		Status: runs.StatusPending,
	}
	require.NoError(t, store.CreateProjectRun(ctx, projectRun))
	require.NoError(t, store.StartProjectRun(ctx, projectRunID, startedAt))

	chunks := []runs.OutputChunk{
		{ProjectRunID: projectRunID, Sequence: 0, Stream: runs.OutputStdout, Content: "Terraform will perform the following actions:\n", CreatedAt: startedAt},
		{ProjectRunID: projectRunID, Sequence: 1, Stream: runs.OutputStderr, Content: "warning\n", CreatedAt: startedAt.Add(time.Millisecond)},
	}
	require.NoError(t, store.AppendOutput(ctx, chunks))
	require.NoError(t, store.AppendOutput(ctx, chunks), "output replay must be idempotent")

	completedAt := startedAt.Add(time.Second)
	artifactCreatedAt := startedAt
	artifactExpiresAt := completedAt.Add(24 * time.Hour)
	projectCompletion := runs.ProjectRunCompletion{
		ID: projectRunID, Status: runs.StatusSucceeded, Additions: 1, Changes: 2,
		CompletedAt: completedAt,
		Metadata:    runs.Metadata(`{"phases":["plan","policy_check"]}`),
		PlanArtifact: &runs.ArtifactReference{
			Key: "plans/example/17/network.tfplan", Checksum: "sha256:abc",
			CreatedAt: artifactCreatedAt, ExpiresAt: &artifactExpiresAt,
		},
	}
	require.NoError(t, store.CompleteProjectRun(ctx, projectCompletion))
	require.NoError(t, store.CompleteProjectRun(ctx, projectCompletion), "completion replay must be idempotent")
	require.NoError(t, store.CompleteRun(ctx, runs.RunCompletion{
		ID: runID, Status: runs.StatusUnknown, CompletedAt: completedAt,
	}))

	event := runs.AuditEvent{
		ID: auditID, Repository: run.Repository, PullNumber: &pull, RunID: &runID,
		Actor: "operator", EventType: "plan.completed",
		Metadata: runs.Metadata(`{"result":"succeeded"}`), CreatedAt: completedAt,
	}
	require.NoError(t, store.AppendAuditEvent(ctx, event))
	require.NoError(t, store.AppendAuditEvent(ctx, event), "audit replay must be idempotent")

	storedRun, err := store.GetRun(ctx, runID)
	require.NoError(t, err)
	require.Equal(t, runs.StatusUnknown, storedRun.Status)
	require.Equal(t, run.HeadSHA, storedRun.HeadSHA)
	storedProject, err := store.GetProjectRun(ctx, projectRunID)
	require.NoError(t, err)
	require.Equal(t, projectCompletion.Additions, storedProject.Additions)
	require.Equal(t, projectCompletion.PlanArtifact, storedProject.PlanArtifact)
	require.JSONEq(t, string(projectCompletion.Metadata), string(storedProject.Metadata))

	runPage, err := store.ListRuns(ctx, runs.RunFilter{
		Repository: run.Repository, PullNumber: &pull,
		Commands: []runs.Command{runs.CommandPlan}, Statuses: []runs.Status{runs.StatusUnknown},
	}, runs.PageRequest{Limit: 1})
	require.NoError(t, err)
	require.Len(t, runPage.Runs, 1)
	projectPage, err := store.ListProjectRuns(ctx, runID, runs.ProjectRunFilter{
		Directory: projectRun.Directory, Statuses: []runs.Status{runs.StatusSucceeded},
	}, runs.PageRequest{Limit: 1})
	require.NoError(t, err)
	require.Len(t, projectPage.ProjectRuns, 1)
	summary, err := store.SummarizeProjectRuns(ctx, runID)
	require.NoError(t, err)
	require.Equal(t, runs.ProjectRunSummary{Total: 1, Succeeded: 1}, summary)
	outputPage, err := store.GetOutput(ctx, projectRunID, -1, 1)
	require.NoError(t, err)
	require.Len(t, outputPage.Chunks, 1)
	require.NotNil(t, outputPage.NextSequence)
	secondOutputPage, err := store.GetOutput(ctx, projectRunID, *outputPage.NextSequence, 1)
	require.NoError(t, err)
	require.Len(t, secondOutputPage.Chunks, 1)
	require.Equal(t, int64(1), secondOutputPage.Chunks[0].Sequence)
	auditPage, err := store.ListAuditEvents(ctx, runs.AuditFilter{
		Repository: run.Repository, RunID: &runID, EventTypes: []string{"plan.completed"},
	}, runs.PageRequest{Limit: 1})
	require.NoError(t, err)
	require.Len(t, auditPage.Events, 1)
	require.NoError(t, store.StopInstance(ctx, instanceID, completedAt))

	retentionCutoff := completedAt.Add(time.Second)
	retention, err := store.ApplyRetention(ctx, runs.RetentionPolicy{
		RunMetadataBefore: &retentionCutoff,
	})
	require.NoError(t, err)
	require.Equal(t, runs.RetentionResult{}, retention,
		"unreconciled unknown attempts must retain their run, project output, and admission fence")
	_, err = store.GetRun(ctx, runID)
	require.NoError(t, err)
	require.NoError(t, store.ReconcileAttempt(ctx, runs.AttemptReconciliation{
		ID: replacementAttempt.ID, At: completedAt.Add(time.Millisecond), Actor: "operator",
		Summary: "state inspected after retention protection test",
	}))
	retention, err = store.ApplyRetention(ctx, runs.RetentionPolicy{
		RunMetadataBefore: &retentionCutoff,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), retention.RunsDeleted)
	require.Equal(t, int64(1), retention.ProjectRunsDeleted)
	require.Equal(t, int64(2), retention.OutputChunksDeleted)
	_, err = store.GetRun(ctx, runID)
	require.ErrorIs(t, err, runs.ErrNotFound)
}

func newIsolatedStore(t *testing.T, ctx context.Context, rawURL string) (*postgres.Store, func()) {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	require.NoError(t, err)
	require.NotEmpty(t, parsed.Scheme, "ATLANTIS_POSTGRES_TEST_URL must be a PostgreSQL URL")
	schema := fmt.Sprintf("atlantis_runs_test_%d", time.Now().UnixNano())

	admin, err := sql.Open("pgx", rawURL)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(ctx))
	_, err = admin.ExecContext(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)

	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	store, err := postgres.New(ctx, postgres.Config{URL: parsed.String(), OperationTimeout: 5 * time.Second})
	if err != nil {
		_, _ = admin.ExecContext(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		_ = admin.Close()
	}
	require.NoError(t, err)

	cleanup := func() {
		require.NoError(t, store.Close())
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := admin.ExecContext(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE")
		require.NoError(t, err)
		require.NoError(t, admin.Close())
	}
	return store, cleanup
}

func mustID(t *testing.T) runs.ID {
	t.Helper()
	id, err := runs.NewID()
	require.NoError(t, err)
	return id
}
