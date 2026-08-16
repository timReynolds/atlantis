// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/stretchr/testify/require"
)

func TestAttemptIsLiveUntilBothHeartbeatsExpire(t *testing.T) {
	cutoff := testTime
	request := runs.AttemptTakeoverRequest{HeartbeatBefore: cutoff}
	attempt := claimedAttempt()
	instance := runs.ExecutionInstance{HeartbeatAt: cutoff.Add(-time.Second)}

	attempt.HeartbeatAt = cutoff
	require.True(t, attemptIsLive(attempt, instance, request))
	attempt.HeartbeatAt = cutoff.Add(-time.Second)
	instance.HeartbeatAt = cutoff
	require.True(t, attemptIsLive(attempt, instance, request))
	instance.HeartbeatAt = cutoff.Add(-time.Second)
	require.False(t, attemptIsLive(attempt, instance, request))
	instance.StoppedAt = &cutoff
	require.False(t, attemptIsLive(attempt, instance, request))
}

func TestTakeoverClassificationSeparatesPlanFromPossibleSideEffect(t *testing.T) {
	attempt := claimedAttempt()
	status, reason := takeoverClassification(attempt, "new-claim")
	require.Equal(t, runs.AttemptInterrupted, status)
	require.Contains(t, reason, "before an infrastructure side effect")

	attempt.SideEffectStartedAt = &testTime
	status, reason = takeoverClassification(attempt, "new-claim")
	require.Equal(t, runs.AttemptUnknown, status)
	require.Contains(t, reason, "may have completed")
}

func TestRunMatchesPlanRetryRequiresExactLogicalOperation(t *testing.T) {
	pull := 42
	startedAt := testTime
	run := runs.Run{
		ID: testRunID, Repository: "org/repo", PullNumber: &pull,
		Command: runs.CommandPlan, Trigger: runs.TriggerComment, Actor: "operator",
		BaseRef: "main", HeadRef: "feature", HeadSHA: "abc123",
		Status: runs.StatusRunning, CreatedAt: startedAt, StartedAt: &startedAt,
	}
	request := runs.AttemptTakeoverRequest{
		Repository: run.Repository, PullNumber: &pull, Command: run.Command,
		Trigger: run.Trigger, Actor: run.Actor, BaseRef: run.BaseRef,
		HeadRef: run.HeadRef, HeadSHA: run.HeadSHA,
	}
	require.True(t, runMatchesPlanRetry(run, request))

	request.HeadSHA = "new-commit"
	require.False(t, runMatchesPlanRetry(run, request))
	request.HeadSHA = run.HeadSHA
	request.Command = runs.CommandApply
	require.False(t, runMatchesPlanRetry(run, request))
}
