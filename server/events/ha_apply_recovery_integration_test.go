// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
	runpostgres "github.com/runatlantis/atlantis/server/core/runs/postgres"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/require"
)

const (
	haApplyHelperEnv      = "ATLANTIS_HA_APPLY_RECOVERY_HELPER"
	haApplyReadyFileEnv   = "ATLANTIS_HA_APPLY_READY_FILE"
	haApplyPullNumberEnv  = "ATLANTIS_HA_APPLY_PULL_NUMBER"
	haApplyHeadSHAEnv     = "ATLANTIS_HA_APPLY_HEAD_SHA"
	haApplyConcurrencyKey = "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	haApplyOwnershipClaim = "apply-owner-before-process-loss"
	haApplyRecordedOutput = "terraform apply crossed the durable mutation fence\n"
)

// TestHAApplyProcessLossBecomesUnknown SIGKILLs a process after it records
// the infrastructure mutation boundary. The replacement must preserve output,
// classify the attempt and Run as unknown, and never return a retry Run.
func TestHAApplyProcessLossBecomesUnknown(t *testing.T) {
	if os.Getenv(haApplyHelperEnv) == "1" {
		runHAApplyRecoveryHelper(t)
		return
	}
	rawPostgresURL := os.Getenv(haPlanPostgresEnv)
	if rawPostgresURL == "" {
		t.Skip("set ATLANTIS_HA_FAILURE_POSTGRES_URL")
	}
	testURL, cleanupDatabase := isolatedHAPlanDatabase(t, rawPostgresURL)
	defer cleanupDatabase()

	pullNumber := int(time.Now().UnixNano()%900000000) + 1
	headSHA := fmt.Sprintf("%040x", time.Now().UnixNano())
	readyFile := filepath.Join(t.TempDir(), "apply-ready")
	executable, err := os.Executable()
	require.NoError(t, err)
	process := exec.Command(executable, "-test.run=^TestHAApplyProcessLossBecomesUnknown$", "-test.v")
	process.Env = append(os.Environ(),
		haApplyHelperEnv+"=1",
		haPlanPostgresEnv+"="+testURL,
		haApplyReadyFileEnv+"="+readyFile,
		haApplyPullNumberEnv+"="+strconv.Itoa(pullNumber),
		haApplyHeadSHAEnv+"="+headSHA,
	)
	var processOutput bytes.Buffer
	process.Stdout = &processOutput
	process.Stderr = &processOutput
	require.NoError(t, process.Start())
	processRunning := true
	t.Cleanup(func() {
		if processRunning {
			_ = process.Process.Kill()
			_ = process.Wait()
		}
	})

	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, err := os.Stat(readyFile); err == nil {
			break
		}
		if time.Now().After(deadline) {
			require.FailNow(t, "apply helper did not reach mutation fence", processOutput.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.NoError(t, process.Process.Kill())
	require.Error(t, process.Wait(), "SIGKILLed helper must not exit successfully")
	processRunning = false

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, err := runpostgres.New(ctx, runpostgres.Config{URL: testURL, OperationTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer store.Close()
	recoveredAt := time.Now().UTC().Add(2 * time.Minute)
	takeover, err := store.PrepareAttemptTakeover(ctx, runs.AttemptTakeoverRequest{
		ConcurrencyKey: haApplyConcurrencyKey, OwnershipClaimID: "replacement-apply-owner",
		HeartbeatBefore: recoveredAt.Add(-time.Minute), RecoveredAt: recoveredAt,
		Repository: haPlanRepository, PullNumber: &pullNumber, Command: runs.CommandApply,
		Trigger: runs.TriggerComment, Actor: "failure-test", BaseRef: "main",
		HeadRef: "failure-test", HeadSHA: headSHA,
	})
	require.NoError(t, err)
	require.NotNil(t, takeover.RecoveredAttempt)
	require.Equal(t, runs.AttemptUnknown, takeover.RecoveredAttempt.Status)
	require.Nil(t, takeover.RetryRun)
	require.NotNil(t, takeover.UnreconciledUnknown)
	require.Equal(t, takeover.RecoveredAttempt.ID, takeover.UnreconciledUnknown.ID)

	storedRun, err := store.GetRun(ctx, takeover.RecoveredAttempt.RunID)
	require.NoError(t, err)
	require.Equal(t, runs.StatusUnknown, storedRun.Status)
	projects, err := store.ListProjectRuns(ctx, storedRun.ID, runs.ProjectRunFilter{}, runs.PageRequest{Limit: 10})
	require.NoError(t, err)
	require.Len(t, projects.ProjectRuns, 1)
	output, err := store.GetOutput(ctx, projects.ProjectRuns[0].ID, -1, 10)
	require.NoError(t, err)
	require.Len(t, output.Chunks, 1)
	require.Equal(t, haApplyRecordedOutput, output.Chunks[0].Content)
}

func runHAApplyRecoveryHelper(t *testing.T) {
	pullNumber, err := strconv.Atoi(os.Getenv(haApplyPullNumberEnv))
	require.NoError(t, err)
	headSHA := os.Getenv(haApplyHeadSHAEnv)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, err := runpostgres.New(ctx, runpostgres.Config{
		URL: os.Getenv(haPlanPostgresEnv), OperationTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	instanceID, err := runs.NewID()
	require.NoError(t, err)
	startedAt := time.Now().UTC()
	require.NoError(t, store.RegisterInstance(ctx, runs.ExecutionInstance{
		ID: instanceID, ReplicaID: "apply-helper", DeploymentID: "failure-gates",
		StartedAt: startedAt, HeartbeatAt: startedAt,
	}))
	history := NewRunHistory(store, logging.NewNoopLogger(t))
	projectCtx := haPlanProjectContext(pullNumber, headSHA)
	runCtx := &command.Context{
		Log: projectCtx.Log, User: projectCtx.User, Pull: projectCtx.Pull,
		ExecutionInstanceID: instanceID, ConcurrencyKey: haApplyConcurrencyKey,
		OwnershipClaimID: haApplyOwnershipClaim,
	}
	lifecycle := history.Begin(runCtx, runs.CommandApply, runs.TriggerComment)
	require.True(t, lifecycle.CanExecute())
	projectCtx.RunID = runCtx.RunID
	projectCtx.AttemptID = runCtx.AttemptID
	projectCtx = history.beginProject(projectCtx)
	require.NoError(t, runCtx.SideEffectMarker.MarkSideEffectStarted(ctx))
	require.NoError(t, store.AppendOutput(ctx, []runs.OutputChunk{{
		ProjectRunID: projectCtx.ProjectRunID, Sequence: 0, Stream: runs.OutputStdout,
		Content: haApplyRecordedOutput, CreatedAt: time.Now().UTC(),
	}}))
	require.NoError(t, os.WriteFile(os.Getenv(haApplyReadyFileEnv), []byte("ready"), 0o600))
	select {}
}
