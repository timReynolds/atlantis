// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/runatlantis/atlantis/server/core/planstore"
	"github.com/runatlantis/atlantis/server/core/runs"
	runpostgres "github.com/runatlantis/atlantis/server/core/runs/postgres"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/require"
)

const (
	haPlanHelperEnv        = "ATLANTIS_HA_PLAN_RECOVERY_HELPER"
	haPlanPostgresEnv      = "ATLANTIS_HA_FAILURE_POSTGRES_URL"
	haPlanS3BucketEnv      = "ATLANTIS_HA_FAILURE_S3_BUCKET"
	haPlanS3RegionEnv      = "ATLANTIS_HA_FAILURE_S3_REGION"
	haPlanS3EndpointEnv    = "ATLANTIS_HA_FAILURE_S3_ENDPOINT"
	haPlanReadyFileEnv     = "ATLANTIS_HA_PLAN_READY_FILE"
	haPlanPullNumberEnv    = "ATLANTIS_HA_PLAN_PULL_NUMBER"
	haPlanHeadSHAEnv       = "ATLANTIS_HA_PLAN_HEAD_SHA"
	haPlanRepository       = "failure-tests/infrastructure"
	haPlanProject          = "network"
	haPlanDirectory        = "terraform/network"
	haPlanWorkspace        = "production"
	haPlanFilename         = "network-production.tfplan"
	haPlanContent          = "opaque plan persisted before SIGKILL"
	haPlanWorkflowIdentity = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

// TestHAPlanArtifactSurvivesProcessTermination is an opt-in release-gating
// test. It starts a real helper process, persists PostgreSQL metadata and an S3
// object, sends SIGKILL, then restores and validates the exact artifact through
// a replacement RunHistoryPlanStore.
func TestHAPlanArtifactSurvivesProcessTermination(t *testing.T) {
	if os.Getenv(haPlanHelperEnv) == "1" {
		runHAPlanRecoveryHelper(t)
		return
	}
	rawPostgresURL := os.Getenv(haPlanPostgresEnv)
	bucket := os.Getenv(haPlanS3BucketEnv)
	endpoint := os.Getenv(haPlanS3EndpointEnv)
	if rawPostgresURL == "" || bucket == "" || endpoint == "" {
		t.Skip("set ATLANTIS_HA_FAILURE_POSTGRES_URL, ATLANTIS_HA_FAILURE_S3_BUCKET, and ATLANTIS_HA_FAILURE_S3_ENDPOINT")
	}
	region := os.Getenv(haPlanS3RegionEnv)
	if region == "" {
		region = "us-east-1"
	}
	testURL, cleanupDatabase := isolatedHAPlanDatabase(t, rawPostgresURL)
	defer cleanupDatabase()

	pullNumber := int(time.Now().UnixNano()%900000000) + 1
	headSHA := fmt.Sprintf("%040x", time.Now().UnixNano())
	readyFile := filepath.Join(t.TempDir(), "artifact-ready")
	executable, err := os.Executable()
	require.NoError(t, err)
	process := exec.Command(executable, "-test.run=^TestHAPlanArtifactSurvivesProcessTermination$", "-test.v")
	process.Env = append(os.Environ(),
		haPlanHelperEnv+"=1",
		haPlanPostgresEnv+"="+testURL,
		haPlanS3BucketEnv+"="+bucket,
		haPlanS3RegionEnv+"="+region,
		haPlanS3EndpointEnv+"="+endpoint,
		haPlanReadyFileEnv+"="+readyFile,
		haPlanPullNumberEnv+"="+strconv.Itoa(pullNumber),
		haPlanHeadSHAEnv+"="+headSHA,
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
			require.FailNow(t, "helper did not persist plan before deadline", processOutput.String())
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
	s3Store, err := planstore.NewS3PlanStore(planstore.S3PlanStoreConfig{
		Bucket: bucket, Region: region, Endpoint: endpoint, ForcePathStyle: true,
	}, logging.NewNoopLogger(t))
	require.NoError(t, err)
	defer s3Store.DeleteForPull("failure-tests", "infrastructure", pullNumber)
	replacement := NewRunHistoryPlanStore(s3Store, NewRunHistory(store, logging.NewNoopLogger(t)), logging.NewNoopLogger(t))
	projectCtx := haPlanProjectContext(pullNumber, headSHA)
	restoredPath := filepath.Join(t.TempDir(), haPlanFilename)

	require.NoError(t, replacement.Load(projectCtx, restoredPath))
	restored, err := os.ReadFile(restoredPath)
	require.NoError(t, err)
	require.Equal(t, []byte(haPlanContent), restored)
}

func runHAPlanRecoveryHelper(t *testing.T) {
	pullNumber, err := strconv.Atoi(os.Getenv(haPlanPullNumberEnv))
	require.NoError(t, err)
	headSHA := os.Getenv(haPlanHeadSHAEnv)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, err := runpostgres.New(ctx, runpostgres.Config{
		URL: os.Getenv(haPlanPostgresEnv), OperationTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	s3Store, err := planstore.NewS3PlanStore(planstore.S3PlanStoreConfig{
		Bucket: os.Getenv(haPlanS3BucketEnv), Region: os.Getenv(haPlanS3RegionEnv),
		Endpoint: os.Getenv(haPlanS3EndpointEnv), ForcePathStyle: true,
	}, logging.NewNoopLogger(t))
	require.NoError(t, err)
	history := NewRunHistory(store, logging.NewNoopLogger(t))
	projectCtx := haPlanProjectContext(pullNumber, headSHA)
	runCtx := &command.Context{
		Log: projectCtx.Log, User: projectCtx.User,
		Pull: projectCtx.Pull,
	}
	history.Begin(runCtx, runs.CommandPlan, runs.TriggerComment)
	projectCtx.RunID = runCtx.RunID
	projectCtx = history.beginProject(projectCtx)
	planPath := filepath.Join(t.TempDir(), haPlanFilename)
	require.NoError(t, os.WriteFile(planPath, []byte(haPlanContent), 0o600))
	wrapped := NewRunHistoryPlanStore(s3Store, history, logging.NewNoopLogger(t))
	require.NoError(t, wrapped.Save(projectCtx, planPath))
	require.NoError(t, os.WriteFile(os.Getenv(haPlanReadyFileEnv), []byte("ready"), 0o600))
	select {}
}

func haPlanProjectContext(pullNumber int, headSHA string) command.ProjectContext {
	repository := models.Repo{Owner: "failure-tests", Name: "infrastructure", FullName: haPlanRepository}
	pull := models.PullRequest{
		Num: pullNumber, BaseRepo: repository, BaseBranch: "main",
		HeadBranch: "failure-test", HeadCommit: headSHA,
	}
	return command.ProjectContext{
		BaseRepo: repository, Pull: pull, User: models.User{Username: "failure-test"},
		ProjectName: haPlanProject, RepoRelDir: haPlanDirectory, Workspace: haPlanWorkspace,
		RepoConfigVersion: 3, WorkflowIdentity: haPlanWorkflowIdentity,
	}
}

func isolatedHAPlanDatabase(t *testing.T, rawURL string) (string, func()) {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	require.NoError(t, err)
	schema := fmt.Sprintf("atlantis_ha_plan_%d", time.Now().UnixNano())
	admin, err := sql.Open("pgx", rawURL)
	require.NoError(t, err)
	require.NoError(t, admin.Ping())
	_, err = admin.Exec("CREATE SCHEMA " + schema)
	require.NoError(t, err)
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String(), func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, cleanupErr := admin.ExecContext(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE")
		require.NoError(t, cleanupErr)
		require.NoError(t, admin.Close())
	}
}
