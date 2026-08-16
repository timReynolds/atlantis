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
		Command: runs.CommandApply, Trigger: runs.TriggerComment, Actor: "operator",
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
	takeoverAttempt := attempt
	takeoverAttempt.ID = mustID(t)
	takeoverAttempt.OwnershipClaimID = "claim-2"
	takeoverAttempt.ClaimedAt = createdAt.Add(500 * time.Millisecond)
	takeoverAttempt.HeartbeatAt = takeoverAttempt.ClaimedAt
	require.NoError(t, store.CreateAttempt(ctx, takeoverAttempt), "a new lease generation must retire its predecessor")
	interruptedAttempt, err := store.GetAttempt(ctx, attemptID)
	require.NoError(t, err)
	require.Equal(t, runs.AttemptInterrupted, interruptedAttempt.Status)
	require.Contains(t, interruptedAttempt.FailureReason, "ownership transferred")
	attempt = takeoverAttempt
	attemptID = takeoverAttempt.ID
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
		ID: attemptID, AuditEventID: mustID(t), At: reconciledAt, Actor: "operator",
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
		ID: projectRunID, RunID: runID, AttemptID: &attemptID, ProjectName: "network",
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
	artifact := &runs.ArtifactReference{
		Key:       "plans/example/17/network.tfplan",
		Checksum:  "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		CreatedAt: artifactCreatedAt, ExpiresAt: &artifactExpiresAt,
	}
	artifactIdentity := runs.PlanArtifactIdentity{
		RepoConfigVersion: 3,
		WorkflowChecksum:  "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}
	require.NoError(t, store.RecordProjectPlanArtifact(ctx, runs.ProjectPlanArtifactUpdate{
		ProjectRunID: projectRunID, Artifact: *artifact, Identity: artifactIdentity,
	}))
	require.NoError(t, store.RecordProjectPlanArtifact(ctx, runs.ProjectPlanArtifactUpdate{
		ProjectRunID: projectRunID, Artifact: *artifact, Identity: artifactIdentity,
	}), "artifact replay must be idempotent")
	projectCompletion := runs.ProjectRunCompletion{
		ID: projectRunID, Status: runs.StatusSucceeded, Additions: 1, Changes: 2,
		CompletedAt:  completedAt,
		Metadata:     runs.Metadata(`{"phases":["plan","policy_check"]}`),
		PlanArtifact: artifact,
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
	expectedArtifact, err := store.FindPlanArtifact(ctx, runs.PlanArtifactLookup{
		Repository: run.Repository, PullNumber: pull, HeadSHA: run.HeadSHA,
		ProjectName: projectRun.ProjectName, Directory: projectRun.Directory, Workspace: projectRun.Workspace,
	})
	require.NoError(t, err)
	require.Equal(t, projectRunID, expectedArtifact.ProjectRunID)
	require.Equal(t, *artifact, expectedArtifact.Artifact)
	require.Equal(t, artifactIdentity, expectedArtifact.Identity)

	runPage, err := store.ListRuns(ctx, runs.RunFilter{
		Repository: run.Repository, PullNumber: &pull,
		Commands: []runs.Command{runs.CommandApply}, Statuses: []runs.Status{runs.StatusUnknown},
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

	// A process that exits after durable claim admission but before command
	// start leaves a claimed attempt. It is interrupted and the exact plan may
	// reuse the logical Run.
	claimedRunID := mustID(t)
	claimedPull := 19
	claimedRun := runs.Run{
		ID: claimedRunID, Repository: "example/infrastructure", PullNumber: &claimedPull,
		Command: runs.CommandPlan, Trigger: runs.TriggerComment, Actor: "operator",
		BaseRef: "main", HeadRef: "claimed-plan", HeadSHA: "a11ce",
		Status: runs.StatusRunning, CreatedAt: completedAt, StartedAt: &completedAt,
	}
	require.NoError(t, store.CreateRun(ctx, claimedRun))
	claimedInstanceID := mustID(t)
	require.NoError(t, store.RegisterInstance(ctx, runs.ExecutionInstance{
		ID: claimedInstanceID, ReplicaID: "atlantis-claimed", DeploymentID: "conformance",
		StartedAt: completedAt, HeartbeatAt: completedAt,
	}))
	claimedAttemptID := mustID(t)
	require.NoError(t, store.CreateAttempt(ctx, runs.RunAttempt{
		ID: claimedAttemptID, RunID: claimedRunID, InstanceID: claimedInstanceID,
		DeploymentID:   "conformance",
		ConcurrencyKey: "sha256:claimed-plan", OwnershipClaimID: "claimed-old",
		Status: runs.AttemptClaimed, ClaimedAt: completedAt, HeartbeatAt: completedAt,
	}))
	duplicateAttemptErr := store.CreateAttempt(ctx, runs.RunAttempt{
		ID: mustID(t), RunID: claimedRunID, InstanceID: claimedInstanceID,
		DeploymentID:   "conformance",
		ConcurrencyKey: "sha256:claimed-plan", OwnershipClaimID: "claimed-old",
		Status: runs.AttemptClaimed, ClaimedAt: completedAt, HeartbeatAt: completedAt,
	})
	require.ErrorIs(t, duplicateAttemptErr, runs.ErrConflict,
		"a duplicate delivery cannot create overlapping active work")
	claimedRecoveredAt := completedAt.Add(2 * time.Minute)
	claimedTakeover, err := store.PrepareAttemptTakeover(ctx, runs.AttemptTakeoverRequest{
		ConcurrencyKey: "sha256:claimed-plan", OwnershipClaimID: "claimed-new",
		HeartbeatBefore: claimedRecoveredAt.Add(-time.Minute), RecoveredAt: claimedRecoveredAt,
		Repository: claimedRun.Repository, PullNumber: &claimedPull, Command: claimedRun.Command,
		Trigger: claimedRun.Trigger, Actor: claimedRun.Actor, BaseRef: claimedRun.BaseRef,
		HeadRef: claimedRun.HeadRef, HeadSHA: claimedRun.HeadSHA,
	})
	require.NoError(t, err)
	require.Equal(t, runs.AttemptInterrupted, claimedTakeover.RecoveredAttempt.Status)
	require.Equal(t, claimedRunID, claimedTakeover.RetryRun.ID)

	// An apply interrupted before the durable mutation marker is not unknown,
	// but it is also never converted into an automatic retry of the same Run.
	preApplyRunID := mustID(t)
	preApplyPull := 21
	preApplyCreatedAt := claimedRecoveredAt.Add(time.Second)
	preApplyRun := runs.Run{
		ID: preApplyRunID, Repository: "example/infrastructure", PullNumber: &preApplyPull,
		Command: runs.CommandApply, Trigger: runs.TriggerComment, Actor: "operator",
		BaseRef: "main", HeadRef: "pre-apply", HeadSHA: "b4apply",
		Status: runs.StatusRunning, CreatedAt: preApplyCreatedAt, StartedAt: &preApplyCreatedAt,
	}
	require.NoError(t, store.CreateRun(ctx, preApplyRun))
	preApplyInstanceID := mustID(t)
	require.NoError(t, store.RegisterInstance(ctx, runs.ExecutionInstance{
		ID: preApplyInstanceID, ReplicaID: "atlantis-pre-apply", DeploymentID: "conformance",
		StartedAt: preApplyCreatedAt, HeartbeatAt: preApplyCreatedAt,
	}))
	preApplyAttemptID := mustID(t)
	require.NoError(t, store.CreateAttempt(ctx, runs.RunAttempt{
		ID: preApplyAttemptID, RunID: preApplyRunID, InstanceID: preApplyInstanceID,
		DeploymentID:   "conformance",
		ConcurrencyKey: "sha256:pre-apply", OwnershipClaimID: "pre-apply-old",
		Status: runs.AttemptClaimed, ClaimedAt: preApplyCreatedAt, HeartbeatAt: preApplyCreatedAt,
	}))
	require.NoError(t, store.StartAttempt(ctx, preApplyAttemptID, preApplyCreatedAt))
	preApplyRecoveredAt := preApplyCreatedAt.Add(2 * time.Minute)
	preApplyTakeover, err := store.PrepareAttemptTakeover(ctx, runs.AttemptTakeoverRequest{
		ConcurrencyKey: "sha256:pre-apply", OwnershipClaimID: "pre-apply-new",
		HeartbeatBefore: preApplyRecoveredAt.Add(-time.Minute), RecoveredAt: preApplyRecoveredAt,
		Repository: preApplyRun.Repository, PullNumber: &preApplyPull, Command: preApplyRun.Command,
		Trigger: preApplyRun.Trigger, Actor: preApplyRun.Actor, BaseRef: preApplyRun.BaseRef,
		HeadRef: preApplyRun.HeadRef, HeadSHA: preApplyRun.HeadSHA,
	})
	require.NoError(t, err)
	require.Equal(t, runs.AttemptInterrupted, preApplyTakeover.RecoveredAttempt.Status)
	require.Nil(t, preApplyTakeover.RetryRun)
	require.Nil(t, preApplyTakeover.UnreconciledUnknown)
	storedPreApplyRun, err := store.GetRun(ctx, preApplyRunID)
	require.NoError(t, err)
	require.Equal(t, runs.StatusFailed, storedPreApplyRun.Status)

	// A stale plan attempt under an older Redis claim is interrupted and a
	// duplicate delivery reuses the same logical Run while preserving both
	// attempts and both sets of project results.
	retryCreatedAt := completedAt.Add(2 * time.Second)
	retryStartedAt := retryCreatedAt.Add(time.Second)
	retryPull := 23
	retryRunID := mustID(t)
	retryRun := runs.Run{
		ID: retryRunID, Repository: "example/infrastructure", PullNumber: &retryPull,
		Command: runs.CommandPlan, Trigger: runs.TriggerComment, Actor: "operator",
		BaseRef: "main", HeadRef: "retry-plan", HeadSHA: "feedface",
		Status: runs.StatusRunning, CreatedAt: retryCreatedAt, StartedAt: &retryStartedAt,
	}
	require.NoError(t, store.CreateRun(ctx, retryRun))
	staleInstanceID := mustID(t)
	require.NoError(t, store.RegisterInstance(ctx, runs.ExecutionInstance{
		ID: staleInstanceID, ReplicaID: "atlantis-1", DeploymentID: "conformance",
		StartedAt: retryCreatedAt, HeartbeatAt: retryStartedAt,
	}))
	staleAttemptID := mustID(t)
	require.NoError(t, store.CreateAttempt(ctx, runs.RunAttempt{
		ID: staleAttemptID, RunID: retryRunID, InstanceID: staleInstanceID,
		DeploymentID:   "conformance",
		ConcurrencyKey: "sha256:retry-plan", OwnershipClaimID: "old-plan-claim",
		Status: runs.AttemptClaimed, ClaimedAt: retryCreatedAt, HeartbeatAt: retryCreatedAt,
	}))
	require.NoError(t, store.StartAttempt(ctx, staleAttemptID, retryStartedAt))
	staleProjectID := mustID(t)
	require.NoError(t, store.CreateProjectRun(ctx, runs.ProjectRun{
		ID: staleProjectID, RunID: retryRunID, AttemptID: &staleAttemptID,
		ProjectName: "network", Directory: "terraform/network", Workspace: "production",
		Status: runs.StatusRunning, StartedAt: &retryStartedAt,
	}))
	liveTakeover, err := store.PrepareAttemptTakeover(ctx, runs.AttemptTakeoverRequest{
		ConcurrencyKey: "sha256:retry-plan", OwnershipClaimID: "premature-plan-claim",
		HeartbeatBefore: retryStartedAt.Add(-time.Second), RecoveredAt: retryStartedAt.Add(time.Second),
		Repository: retryRun.Repository, PullNumber: &retryPull, Command: retryRun.Command,
		Trigger: retryRun.Trigger, Actor: retryRun.Actor, BaseRef: retryRun.BaseRef,
		HeadRef: retryRun.HeadRef, HeadSHA: retryRun.HeadSHA,
	})
	require.NoError(t, err)
	require.Equal(t, staleAttemptID, liveTakeover.ActiveAttempt.ID,
		"Redis lease movement alone must not overlap a process with a fresh PostgreSQL heartbeat")
	require.Nil(t, liveTakeover.RecoveredAttempt)
	recoveredAt := retryStartedAt.Add(2 * time.Minute)
	takeover, err := store.PrepareAttemptTakeover(ctx, runs.AttemptTakeoverRequest{
		ConcurrencyKey: "sha256:retry-plan", OwnershipClaimID: "new-plan-claim",
		HeartbeatBefore: recoveredAt.Add(-time.Minute), RecoveredAt: recoveredAt,
		Repository: retryRun.Repository, PullNumber: &retryPull, Command: retryRun.Command,
		Trigger: retryRun.Trigger, Actor: retryRun.Actor, BaseRef: retryRun.BaseRef,
		HeadRef: retryRun.HeadRef, HeadSHA: retryRun.HeadSHA,
	})
	require.NoError(t, err)
	require.NotNil(t, takeover.RetryRun)
	require.Equal(t, retryRunID, takeover.RetryRun.ID)
	require.Equal(t, runs.AttemptInterrupted, takeover.RecoveredAttempt.Status)
	require.Nil(t, takeover.UnreconciledUnknown)
	staleProject, err := store.GetProjectRun(ctx, staleProjectID)
	require.NoError(t, err)
	require.Equal(t, runs.StatusFailed, staleProject.Status)
	gracefulRetry, err := store.PrepareAttemptTakeover(ctx, runs.AttemptTakeoverRequest{
		ConcurrencyKey: "sha256:retry-plan", OwnershipClaimID: "newer-plan-claim",
		HeartbeatBefore: recoveredAt.Add(-time.Minute), RecoveredAt: recoveredAt.Add(time.Second),
		Repository: retryRun.Repository, PullNumber: &retryPull, Command: retryRun.Command,
		Trigger: retryRun.Trigger, Actor: retryRun.Actor, BaseRef: retryRun.BaseRef,
		HeadRef: retryRun.HeadRef, HeadSHA: retryRun.HeadSHA,
	})
	require.NoError(t, err)
	require.Equal(t, retryRunID, gracefulRetry.RetryRun.ID,
		"an already interrupted graceful-shutdown plan must retain its logical Run")
	require.Equal(t, staleAttemptID, gracefulRetry.RecoveredAttempt.ID)

	replacementInstanceID := mustID(t)
	require.NoError(t, store.RegisterInstance(ctx, runs.ExecutionInstance{
		ID: replacementInstanceID, ReplicaID: "atlantis-2", DeploymentID: "conformance",
		StartedAt: recoveredAt, HeartbeatAt: recoveredAt,
	}))
	replacementAttemptID := mustID(t)
	require.NoError(t, store.CreateAttempt(ctx, runs.RunAttempt{
		ID: replacementAttemptID, RunID: retryRunID, InstanceID: replacementInstanceID,
		DeploymentID:   "conformance",
		ConcurrencyKey: "sha256:retry-plan", OwnershipClaimID: "new-plan-claim",
		Status: runs.AttemptClaimed, ClaimedAt: recoveredAt, HeartbeatAt: recoveredAt,
	}))
	require.NoError(t, store.StartAttempt(ctx, replacementAttemptID, recoveredAt))
	replacementProjectID := mustID(t)
	require.NoError(t, store.CreateProjectRun(ctx, runs.ProjectRun{
		ID: replacementProjectID, RunID: retryRunID, AttemptID: &replacementAttemptID,
		ProjectName: "network", Directory: "terraform/network", Workspace: "production",
		Status: runs.StatusRunning, StartedAt: &recoveredAt,
	}))
	retryCompletedAt := recoveredAt.Add(time.Minute)
	require.NoError(t, store.CompleteProjectRun(ctx, runs.ProjectRunCompletion{
		ID: replacementProjectID, Status: runs.StatusSucceeded, CompletedAt: retryCompletedAt,
	}))
	require.NoError(t, store.CompleteAttempt(ctx, runs.AttemptCompletion{
		ID: replacementAttemptID, Status: runs.AttemptSucceeded, CompletedAt: retryCompletedAt,
	}))
	require.NoError(t, store.CompleteRun(ctx, runs.RunCompletion{
		ID: retryRunID, Status: runs.StatusSucceeded, CompletedAt: retryCompletedAt,
	}))
	retryAttempts, err := store.ListRunAttempts(ctx, retryRunID, runs.PageRequest{Limit: 10})
	require.NoError(t, err)
	require.Len(t, retryAttempts.Attempts, 2)
	retryProjects, err := store.ListProjectRuns(ctx, retryRunID, runs.ProjectRunFilter{}, runs.PageRequest{Limit: 10})
	require.NoError(t, err)
	require.Len(t, retryProjects.ProjectRuns, 2)

	// A preserved interrupted plan can only retain its logical Run when the
	// replacement request is the exact same operation. A newer ref must
	// terminalize the old Run before creating a different logical operation.
	mismatchCreatedAt := retryCompletedAt.Add(500 * time.Millisecond)
	mismatchRunID := mustID(t)
	mismatchPull := 24
	mismatchRun := runs.Run{
		ID: mismatchRunID, Repository: "example/infrastructure", PullNumber: &mismatchPull,
		Command: runs.CommandPlan, Trigger: runs.TriggerComment, Actor: "operator",
		BaseRef: "main", HeadRef: "interrupted-plan", HeadSHA: "old-commit",
		Status: runs.StatusRunning, CreatedAt: mismatchCreatedAt, StartedAt: &mismatchCreatedAt,
	}
	require.NoError(t, store.CreateRun(ctx, mismatchRun))
	mismatchAttemptID := mustID(t)
	require.NoError(t, store.CreateAttempt(ctx, runs.RunAttempt{
		ID: mismatchAttemptID, RunID: mismatchRunID, InstanceID: staleInstanceID,
		DeploymentID: "conformance", ConcurrencyKey: "sha256:mismatched-plan",
		OwnershipClaimID: "old-mismatch-claim", Status: runs.AttemptClaimed,
		ClaimedAt: mismatchCreatedAt, HeartbeatAt: mismatchCreatedAt,
	}))
	require.NoError(t, store.StartAttempt(ctx, mismatchAttemptID, mismatchCreatedAt))
	mismatchInterruptedAt := mismatchCreatedAt.Add(time.Second)
	require.NoError(t, store.CompleteAttempt(ctx, runs.AttemptCompletion{
		ID: mismatchAttemptID, Status: runs.AttemptInterrupted, CompletedAt: mismatchInterruptedAt,
		FailureReason: "graceful shutdown deadline expired before a side effect",
	}))
	mismatchRecovery, err := store.PrepareAttemptTakeover(ctx, runs.AttemptTakeoverRequest{
		ConcurrencyKey: "sha256:mismatched-plan", OwnershipClaimID: "new-mismatch-claim",
		HeartbeatBefore: mismatchInterruptedAt, RecoveredAt: mismatchInterruptedAt.Add(time.Second),
		Repository: mismatchRun.Repository, PullNumber: &mismatchPull, Command: mismatchRun.Command,
		Trigger: mismatchRun.Trigger, Actor: mismatchRun.Actor, BaseRef: mismatchRun.BaseRef,
		HeadRef: mismatchRun.HeadRef, HeadSHA: "new-commit",
	})
	require.NoError(t, err)
	require.Nil(t, mismatchRecovery.RetryRun)
	require.Equal(t, mismatchAttemptID, mismatchRecovery.RecoveredAttempt.ID)
	storedMismatchRun, err := store.GetRun(ctx, mismatchRunID)
	require.NoError(t, err)
	require.Equal(t, runs.StatusFailed, storedMismatchRun.Status)

	// Older releases completed the Run before terminalizing its attempt. A
	// takeover repairs that ordering idempotently instead of blocking forever.
	terminalCreatedAt := retryCompletedAt.Add(time.Second)
	terminalRunID := mustID(t)
	terminalPull := 28
	terminalRun := runs.Run{
		ID: terminalRunID, Repository: "example/infrastructure", PullNumber: &terminalPull,
		Command: runs.CommandPlan, Trigger: runs.TriggerComment, Actor: "operator",
		BaseRef: "main", HeadRef: "terminal-parent", HeadSHA: "cab004e",
		Status: runs.StatusRunning, CreatedAt: terminalCreatedAt, StartedAt: &terminalCreatedAt,
	}
	require.NoError(t, store.CreateRun(ctx, terminalRun))
	terminalInstanceID := mustID(t)
	require.NoError(t, store.RegisterInstance(ctx, runs.ExecutionInstance{
		ID: terminalInstanceID, ReplicaID: "atlantis-terminal", DeploymentID: "conformance",
		StartedAt: terminalCreatedAt, HeartbeatAt: terminalCreatedAt,
	}))
	terminalAttemptID := mustID(t)
	require.NoError(t, store.CreateAttempt(ctx, runs.RunAttempt{
		ID: terminalAttemptID, RunID: terminalRunID, InstanceID: terminalInstanceID,
		DeploymentID: "conformance", ConcurrencyKey: "sha256:terminal-parent",
		OwnershipClaimID: "old-terminal-claim", Status: runs.AttemptClaimed,
		ClaimedAt: terminalCreatedAt, HeartbeatAt: terminalCreatedAt,
	}))
	require.NoError(t, store.StartAttempt(ctx, terminalAttemptID, terminalCreatedAt))
	terminalCompletedAt := terminalCreatedAt.Add(time.Minute)
	require.NoError(t, store.CompleteRun(ctx, runs.RunCompletion{
		ID: terminalRunID, Status: runs.StatusSucceeded, CompletedAt: terminalCompletedAt,
	}))
	terminalRecoveredAt := terminalCompletedAt.Add(time.Minute)
	terminalTakeover, err := store.PrepareAttemptTakeover(ctx, runs.AttemptTakeoverRequest{
		ConcurrencyKey: "sha256:terminal-parent", OwnershipClaimID: "new-terminal-claim",
		HeartbeatBefore: terminalRecoveredAt.Add(-time.Minute), RecoveredAt: terminalRecoveredAt,
		Repository: terminalRun.Repository, PullNumber: &terminalPull, Command: terminalRun.Command,
		Trigger: terminalRun.Trigger, Actor: terminalRun.Actor, BaseRef: terminalRun.BaseRef,
		HeadRef: terminalRun.HeadRef, HeadSHA: terminalRun.HeadSHA,
	})
	require.NoError(t, err)
	require.NotNil(t, terminalTakeover.RecoveredAttempt)
	require.Equal(t, runs.AttemptSucceeded, terminalTakeover.RecoveredAttempt.Status)
	require.Nil(t, terminalTakeover.RetryRun)
	storedTerminalRun, err := store.GetRun(ctx, terminalRunID)
	require.NoError(t, err)
	require.Equal(t, runs.StatusSucceeded, storedTerminalRun.Status)

	// A stale apply after the side-effect marker becomes unknown and blocks
	// further mutating admission until an operator records reconciliation.
	unknownCreatedAt := retryCompletedAt.Add(time.Second)
	unknownRunID := mustID(t)
	unknownPull := 29
	unknownRun := runs.Run{
		ID: unknownRunID, Repository: "example/infrastructure", PullNumber: &unknownPull,
		Command: runs.CommandApply, Trigger: runs.TriggerComment, Actor: "operator",
		BaseRef: "main", HeadRef: "unknown-apply", HeadSHA: "cab005e",
		Status: runs.StatusRunning, CreatedAt: unknownCreatedAt, StartedAt: &unknownCreatedAt,
	}
	require.NoError(t, store.CreateRun(ctx, unknownRun))
	unknownInstanceID := mustID(t)
	require.NoError(t, store.RegisterInstance(ctx, runs.ExecutionInstance{
		ID: unknownInstanceID, ReplicaID: "atlantis-3", DeploymentID: "conformance",
		StartedAt: unknownCreatedAt, HeartbeatAt: unknownCreatedAt,
	}))
	unknownAttemptID := mustID(t)
	require.NoError(t, store.CreateAttempt(ctx, runs.RunAttempt{
		ID: unknownAttemptID, RunID: unknownRunID, InstanceID: unknownInstanceID,
		DeploymentID:   "conformance",
		ConcurrencyKey: "sha256:unknown-apply", OwnershipClaimID: "old-apply-claim",
		Status: runs.AttemptClaimed, ClaimedAt: unknownCreatedAt, HeartbeatAt: unknownCreatedAt,
	}))
	require.NoError(t, store.StartAttempt(ctx, unknownAttemptID, unknownCreatedAt))
	require.NoError(t, store.MarkAttemptSideEffectStarted(ctx, unknownAttemptID, unknownCreatedAt))
	unknownRecoveredAt := unknownCreatedAt.Add(2 * time.Minute)
	unknownRequest := runs.AttemptTakeoverRequest{
		ConcurrencyKey: "sha256:unknown-apply", OwnershipClaimID: "new-apply-claim",
		HeartbeatBefore: unknownRecoveredAt.Add(-time.Minute), RecoveredAt: unknownRecoveredAt,
		Repository: unknownRun.Repository, PullNumber: &unknownPull, Command: unknownRun.Command,
		Trigger: unknownRun.Trigger, Actor: unknownRun.Actor, BaseRef: unknownRun.BaseRef,
		HeadRef: unknownRun.HeadRef, HeadSHA: unknownRun.HeadSHA,
	}
	unknownTakeover, err := store.PrepareAttemptTakeover(ctx, unknownRequest)
	require.NoError(t, err)
	require.Equal(t, runs.AttemptUnknown, unknownTakeover.RecoveredAttempt.Status)
	require.Equal(t, unknownAttemptID, unknownTakeover.UnreconciledUnknown.ID)
	require.Nil(t, unknownTakeover.RetryRun)
	storedUnknownRun, err := store.GetRun(ctx, unknownRunID)
	require.NoError(t, err)
	require.Equal(t, runs.StatusUnknown, storedUnknownRun.Status)
	stillBlocked, err := store.PrepareAttemptTakeover(ctx, unknownRequest)
	require.NoError(t, err)
	require.Equal(t, unknownAttemptID, stillBlocked.UnreconciledUnknown.ID)
	require.NoError(t, store.ReconcileAttempt(ctx, runs.AttemptReconciliation{
		ID: unknownAttemptID, AuditEventID: mustID(t), At: unknownRecoveredAt.Add(time.Second), Actor: "operator",
		Summary: "state inspected; fresh plan generated",
	}))
	afterReconciliation, err := store.PrepareAttemptTakeover(ctx, unknownRequest)
	require.NoError(t, err)
	require.Nil(t, afterReconciliation.UnreconciledUnknown)
	reconciliationAudit, err := store.ListAuditEvents(ctx, runs.AuditFilter{
		Repository: "example/infrastructure", EventTypes: []string{"execution_attempt.reconciled"},
	}, runs.PageRequest{Limit: 10})
	require.NoError(t, err)
	require.Len(t, reconciliationAudit.Events, 2)
	require.Contains(t, string(reconciliationAudit.Events[0].Metadata), "attempt_id")

	retentionCutoff := unknownRecoveredAt.Add(2 * time.Second)
	retention, err := store.ApplyRetention(ctx, runs.RetentionPolicy{
		RunMetadataBefore: &retentionCutoff,
	})
	require.NoError(t, err)
	require.Equal(t, int64(5), retention.RunsDeleted)
	require.Equal(t, int64(2), retention.ProjectRunsDeleted)
	require.Equal(t, int64(0), retention.OutputChunksDeleted)
	_, err = store.GetRun(ctx, runID)
	require.NoError(t, err,
		"unreconciled unknown attempts must retain their run, project output, and admission fence")
	require.NoError(t, store.ReconcileAttempt(ctx, runs.AttemptReconciliation{
		ID: replacementAttempt.ID, AuditEventID: mustID(t), At: completedAt.Add(time.Millisecond), Actor: "operator",
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

func TestAttemptTakeoverFinalizesSupersededRun(t *testing.T) {
	if testing.Short() {
		t.Skip("PostgreSQL conformance test is disabled in short mode")
	}
	testURL := os.Getenv("ATLANTIS_POSTGRES_TEST_URL")
	if testURL == "" {
		t.Skip("ATLANTIS_POSTGRES_TEST_URL is not set")
	}
	for _, test := range []struct {
		name              string
		sideEffectStarted bool
		expectedStatus    runs.Status
		expectedAttempt   runs.AttemptStatus
	}{
		{name: "before side effect", expectedStatus: runs.StatusFailed, expectedAttempt: runs.AttemptInterrupted},
		{name: "after side effect", sideEffectStarted: true, expectedStatus: runs.StatusUnknown, expectedAttempt: runs.AttemptUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			store, cleanup := newIsolatedStore(t, ctx, testURL)
			defer cleanup()

			createdAt := time.Now().UTC().Truncate(time.Microsecond)
			startedAt := createdAt.Add(time.Second)
			oldRunID, newRunID := mustID(t), mustID(t)
			for _, runID := range []runs.ID{oldRunID, newRunID} {
				run := runs.Run{
					ID: runID, Repository: "example/infrastructure", Command: runs.CommandApply,
					Trigger: runs.TriggerComment, Status: runs.StatusPending, CreatedAt: createdAt,
				}
				require.NoError(t, store.CreateRun(ctx, run))
				require.NoError(t, store.StartRun(ctx, runID, startedAt))
			}
			instanceID := mustID(t)
			require.NoError(t, store.RegisterInstance(ctx, runs.ExecutionInstance{
				ID: instanceID, ReplicaID: "atlantis-0", DeploymentID: "takeover-test",
				StartedAt: createdAt, HeartbeatAt: createdAt,
			}))
			oldAttemptID := mustID(t)
			oldAttempt := runs.RunAttempt{
				ID: oldAttemptID, RunID: oldRunID, InstanceID: instanceID,
				DeploymentID: "takeover-test", ConcurrencyKey: "sha256:pull",
				OwnershipClaimID: "claim-1", Status: runs.AttemptClaimed,
				ClaimedAt: createdAt, HeartbeatAt: createdAt,
			}
			require.NoError(t, store.CreateAttempt(ctx, oldAttempt))
			require.NoError(t, store.StartAttempt(ctx, oldAttemptID, startedAt))
			projectRunID := mustID(t)
			require.NoError(t, store.CreateProjectRun(ctx, runs.ProjectRun{
				ID: projectRunID, RunID: oldRunID, Directory: "terraform/network",
				Workspace: "production", Status: runs.StatusPending,
			}))
			require.NoError(t, store.StartProjectRun(ctx, projectRunID, startedAt))
			if test.sideEffectStarted {
				require.NoError(t, store.MarkAttemptSideEffectStarted(ctx, oldAttemptID, startedAt.Add(time.Millisecond)))
			}

			takeoverAt := startedAt.Add(2 * time.Millisecond)
			require.NoError(t, store.CreateAttempt(ctx, runs.RunAttempt{
				ID: mustID(t), RunID: newRunID, InstanceID: instanceID,
				DeploymentID: "takeover-test", ConcurrencyKey: "sha256:pull",
				OwnershipClaimID: "claim-2", Status: runs.AttemptClaimed,
				ClaimedAt: takeoverAt, HeartbeatAt: takeoverAt,
			}))

			storedAttempt, err := store.GetAttempt(ctx, oldAttemptID)
			require.NoError(t, err)
			require.Equal(t, test.expectedAttempt, storedAttempt.Status)
			storedRun, err := store.GetRun(ctx, oldRunID)
			require.NoError(t, err)
			require.Equal(t, test.expectedStatus, storedRun.Status)
			require.Equal(t, takeoverAt, *storedRun.CompletedAt)
			storedProject, err := store.GetProjectRun(ctx, projectRunID)
			require.NoError(t, err)
			require.Equal(t, test.expectedStatus, storedProject.Status)
			currentRun, err := store.GetRun(ctx, newRunID)
			require.NoError(t, err)
			require.Equal(t, runs.StatusRunning, currentRun.Status)
		})
	}
}

func newIsolatedStore(t testing.TB, ctx context.Context, rawURL string) (*postgres.Store, func()) {
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
