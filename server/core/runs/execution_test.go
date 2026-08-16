// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package runs_test

import (
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/stretchr/testify/require"
)

func TestRunAttemptValidationDistinguishesInterruptedAndUnknown(t *testing.T) {
	claimedAt := time.Date(2026, 8, 16, 10, 0, 0, 0, time.UTC)
	startedAt := claimedAt.Add(time.Second)
	completedAt := startedAt.Add(time.Minute)
	sideEffectAt := startedAt.Add(time.Second)
	base := runs.RunAttempt{
		ID:             "0198a0df-85f1-7d83-a60b-2e57b725c62c",
		RunID:          "0198a0df-85f1-7d83-a60b-2e57b725c62a",
		InstanceID:     "0198a0df-85f1-7d83-a60b-2e57b725c62d",
		ConcurrencyKey: "sha256:pull-ownership-key", ClaimedAt: claimedAt,
		StartedAt: &startedAt, HeartbeatAt: startedAt,
		CompletedAt: &completedAt,
	}

	interrupted := base
	interrupted.Status = runs.AttemptInterrupted
	interrupted.FailureReason = "replica heartbeat expired during plan"
	require.NoError(t, interrupted.Validate())
	interrupted.SideEffectStartedAt = &sideEffectAt
	require.ErrorContains(t, interrupted.Validate(), "interrupted attempt")

	unknown := base
	unknown.Status = runs.AttemptUnknown
	unknown.FailureReason = "replica heartbeat expired after apply started"
	unknown.SideEffectStartedAt = &sideEffectAt
	require.NoError(t, unknown.Validate())
	unknown.SideEffectStartedAt = nil
	require.ErrorContains(t, unknown.Validate(), "unknown attempt")
}

func TestUnknownAttemptReconciliationPreservesUnknownStatus(t *testing.T) {
	claimedAt := time.Date(2026, 8, 16, 10, 0, 0, 0, time.UTC)
	startedAt := claimedAt.Add(time.Second)
	sideEffectAt := startedAt.Add(time.Second)
	completedAt := sideEffectAt.Add(time.Minute)
	reconciledAt := completedAt.Add(time.Hour)
	attempt := runs.RunAttempt{
		ID:             "0198a0df-85f1-7d83-a60b-2e57b725c62c",
		RunID:          "0198a0df-85f1-7d83-a60b-2e57b725c62a",
		InstanceID:     "0198a0df-85f1-7d83-a60b-2e57b725c62d",
		ConcurrencyKey: "sha256:pull-ownership-key",
		Status:         runs.AttemptUnknown, ClaimedAt: claimedAt, StartedAt: &startedAt,
		HeartbeatAt: sideEffectAt, SideEffectStartedAt: &sideEffectAt,
		CompletedAt: &completedAt, FailureReason: "replica heartbeat expired after apply started",
		ReconciledAt: &reconciledAt,
		ReconciledBy: "operator", ReconciliationSummary: "state inspected; fresh plan required",
	}
	require.NoError(t, attempt.Validate())
	attempt.Status = runs.AttemptFailed
	require.ErrorContains(t, attempt.Validate(), "only unknown attempts")
}

func TestExecutionInstanceRequiresDeploymentScopedIdentity(t *testing.T) {
	startedAt := time.Date(2026, 8, 16, 10, 0, 0, 0, time.UTC)
	instance := runs.ExecutionInstance{
		ID: "0198a0df-85f1-7d83-a60b-2e57b725c62d", ReplicaID: "atlantis-0",
		DeploymentID: "prod-eu", StartedAt: startedAt, HeartbeatAt: startedAt,
	}
	require.NoError(t, instance.Validate())
	instance.DeploymentID = ""
	require.ErrorContains(t, instance.Validate(), "deployment ID")
}
