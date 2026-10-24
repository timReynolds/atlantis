// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/runatlantis/atlantis/server/core/planstore"
	"github.com/runatlantis/atlantis/server/core/queue"
	"github.com/runatlantis/atlantis/server/core/runs"
	runpostgres "github.com/runatlantis/atlantis/server/core/runs/postgres"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/require"
)

// This release gate uses real PostgreSQL, S3 conditional writes and Terraform's
// S3 state lock. It performs no cloud resource operations: terraform_data is a
// built-in provider and the only provisioner creates a local readiness file.
func TestQueueTerraformArtifactsAndNativeLock(t *testing.T) {
	postgresURL := os.Getenv("ATLANTIS_POSTGRES_TEST_URL")
	endpoint := os.Getenv("ATLANTIS_QUEUE_TEST_S3_ENDPOINT")
	binary := os.Getenv("ATLANTIS_QUEUE_TEST_TERRAFORM")
	if testing.Short() || postgresURL == "" || endpoint == "" || binary == "" {
		t.Skip("requires PostgreSQL, ATLANTIS_QUEUE_TEST_S3_ENDPOINT and ATLANTIS_QUEUE_TEST_TERRAFORM (Terraform 1.11+)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	testURL, cleanup := isolatedHAPlanDatabase(t, postgresURL)
	defer cleanup()
	store, err := runpostgres.New(ctx, runpostgres.Config{URL: testURL})
	require.NoError(t, err)
	defer store.Close()
	id, err := runs.NewID()
	require.NoError(t, err)
	bucket := "queue-" + string(id)
	client := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(endpoint), UsePathStyle: true, Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: os.Getenv("AWS_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY")}, nil
	})})
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	defer func() {
		objects, _ := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		if objects != nil {
			for _, object := range objects.Contents {
				_, _ = client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: object.Key})
			}
		}
		_, _ = client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	}()
	root := t.TempDir()
	other := t.TempDir()
	ready := filepath.Join(root, "lock-held")
	config := fmt.Sprintf(`terraform {
 backend "s3" {
  bucket = %q
  key = "state.tfstate"
  region = "us-east-1"
  endpoints = { s3 = %q }
  use_path_style = true
  use_lockfile = true
  skip_credentials_validation = true
  skip_region_validation = true
  skip_metadata_api_check = true
  skip_requesting_account_id = true
 }
}
resource "terraform_data" "example" {
 triggers_replace = [1]
 provisioner "local-exec" { command = "touch lock-held; sleep 2" }
}
`, bucket, endpoint)
	run := func(dir string, args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, string(out))
		return string(out)
	}
	require.NoError(t, os.WriteFile(filepath.Join(root, "main.tf"), []byte(config), 0600))
	run(root, "init", "-input=false", "-no-color")
	run(root, "apply", "-auto-approve", "-input=false", "-no-color")
	config = strings.Replace(config, "triggers_replace = [1]", "triggers_replace = [2]", 1)
	require.NoError(t, os.WriteFile(filepath.Join(root, "main.tf"), []byte(config), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(other, "main.tf"), []byte(config), 0600))
	run(other, "init", "-input=false", "-no-color")
	require.NoError(t, os.Remove(ready))
	run(root, "plan", "-input=false", "-no-color", "-out=state-default.tfplan")
	before, err := queue.ParseStateVersion([]byte(run(root, "state", "pull")))
	require.NoError(t, err)
	rawBackend, err := os.ReadFile(filepath.Join(root, ".terraform", "terraform.tfstate"))
	require.NoError(t, err)
	backend, err := queue.BackendStateID(rawBackend, "default")
	require.NoError(t, err)
	require.NoError(t, queue.ValidateBackendLocking(rawBackend))
	fingerprint, _, destructive, err := queue.SemanticPlan([]byte(run(root, "show", "-json", "state-default.tfplan")))
	require.NoError(t, err)
	require.NotEmpty(t, fingerprint)
	require.True(t, destructive)
	instance, err := runs.NewID()
	require.NoError(t, err)
	now := time.Now().UTC()
	require.NoError(t, store.RegisterInstance(ctx, runs.ExecutionInstance{ID: instance, ReplicaID: "queue-test", DeploymentID: "queue-test", StartedAt: now, HeartbeatAt: now}))
	history := NewRunHistory(store, logging.NewNoopLogger(t))
	p := haPlanProjectContext(21, "head")
	p.ProjectName = "state"
	p.RepoRelDir = "."
	p.Workspace = "default"
	p.Log = logging.NewNoopLogger(t)
	p.PlanGeneration = id
	p.RetainPlan = true
	c := &command.Context{Pull: p.Pull, User: p.User, Log: p.Log, ExecutionInstanceID: instance, ExecutionDeploymentID: "queue-test", ConcurrencyKey: "queue-test", OwnershipClaimID: string(id)}
	life := history.Begin(c, runs.CommandPlan, runs.TriggerComment)
	defer life.Finish()
	require.True(t, life.CanExecute())
	p.RunID = c.RunID
	p.AttemptID = c.AttemptID
	p = history.beginProject(p)
	s3Store, err := planstore.NewS3PlanStore(planstore.S3PlanStoreConfig{Bucket: bucket, Region: "us-east-1", Endpoint: endpoint, ForcePathStyle: true}, p.Log)
	require.NoError(t, err)
	wrapped := NewRunHistoryPlanStore(s3Store, history, p.Log)
	planPath := filepath.Join(root, "state-default.tfplan")
	require.NoError(t, wrapped.Save(p, planPath))
	expected, err := store.FindPlanArtifact(ctx, runs.PlanArtifactLookup{Repository: p.BaseRepo.ID(), PullNumber: p.Pull.Num, HeadSHA: p.Pull.HeadCommit, ProjectName: p.ProjectName, Directory: p.RepoRelDir, Workspace: p.Workspace})
	require.NoError(t, err)
	p.ExpectedPlanArtifact = &expected
	require.Error(t, wrapped.Save(p, planPath), "an immutable generation cannot be overwritten")
	require.NoError(t, wrapped.Remove(p, planPath))
	require.NoError(t, os.Remove(planPath))
	require.NoError(t, wrapped.Load(p, planPath), "retained artifact survives local checkout loss")
	// A different project directory has the same backend identity and lineage.
	otherRaw, err := os.ReadFile(filepath.Join(other, ".terraform", "terraform.tfstate"))
	require.NoError(t, err)
	otherBackend, err := queue.BackendStateID(otherRaw, "default")
	require.NoError(t, err)
	require.Equal(t, backend, otherBackend)
	otherState, err := queue.ParseStateVersion([]byte(run(other, "state", "pull")))
	require.NoError(t, err)
	require.Equal(t, before, otherState)
	qs := runpostgres.NewQueueStore(store)
	entry, err := qs.Enqueue(ctx, queue.Request{Repository: p.BaseRepo.ID(), PullNumber: p.Pull.Num, TargetBranch: "main", HeadSHA: "head", Actor: "operator", Source: []byte(`{}`)})
	require.NoError(t, err)
	entry, err = qs.ClaimNext(ctx, "worker", time.Minute)
	require.NoError(t, err)
	lineage := "lineage:" + queueHash(before.Lineage)
	require.NoError(t, qs.Reserve(ctx, entry, []string{backend, lineage}))
	_, err = qs.AcquireMutation(ctx, []string{otherBackend, lineage}, "", "", 0, "ordinary-apply")
	require.ErrorIs(t, err, queue.ErrReserved)
	// Terraform's own lock independently rejects an out-of-band concurrent apply.
	planBytes, err := os.ReadFile(planPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(other, "state-default.tfplan"), planBytes, 0600))
	running := exec.CommandContext(ctx, binary, "apply", "-input=false", "-no-color", planPath)
	running.Dir = root
	outputFile, err := os.CreateTemp(t.TempDir(), "apply-output")
	require.NoError(t, err)
	defer outputFile.Close()
	running.Stdout = outputFile
	running.Stderr = outputFile
	require.NoError(t, running.Start())
	defer func() { _ = running.Process.Kill() }()
	require.Eventually(t, func() bool { _, err := os.Stat(ready); return err == nil }, 10*time.Second, 10*time.Millisecond)
	competing := exec.CommandContext(ctx, binary, "apply", "-input=false", "-no-color", "-lock-timeout=0s", "state-default.tfplan")
	competing.Dir = other
	output, err := competing.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "Error acquiring the state lock")
	require.NoError(t, running.Wait())
	after, err := queue.ParseStateVersion([]byte(run(root, "state", "pull")))
	require.NoError(t, err)
	require.Equal(t, before.Lineage, after.Lineage)
	require.Greater(t, after.Serial, before.Serial)
	stale := exec.CommandContext(ctx, binary, "apply", "-input=false", "-no-color", "state-default.tfplan")
	stale.Dir = other
	output, err = stale.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "Saved plan is stale")
}
