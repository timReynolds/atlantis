// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/runatlantis/atlantis/server/core/queue"
	"github.com/runatlantis/atlantis/server/core/runs"
	runpostgres "github.com/runatlantis/atlantis/server/core/runs/postgres"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/logging"
	. "github.com/runatlantis/atlantis/testing"
)

func TestQueueFirstApplyFencesBackendAliases(t *testing.T) {
	for _, checkpointFails := range []bool{false, true} {
		t.Run(fmt.Sprintf("checkpoint_failure_%t", checkpointFails), func(t *testing.T) {
			testQueueFirstApplyFencesBackendAliases(t, checkpointFails)
		})
	}
}

func testQueueFirstApplyFencesBackendAliases(t *testing.T, checkpointFails bool) {
	postgresURL := os.Getenv("ATLANTIS_POSTGRES_TEST_URL")
	endpoint := os.Getenv("ATLANTIS_QUEUE_TEST_S3_ENDPOINT")
	binary := os.Getenv("ATLANTIS_QUEUE_TEST_TERRAFORM")
	if testing.Short() || postgresURL == "" || endpoint == "" || binary == "" {
		t.Skip("requires PostgreSQL, S3 endpoint, and Terraform 1.11+")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	testURL, cleanup := isolatedHAPlanDatabase(t, postgresURL)
	defer cleanup()
	db, err := runpostgres.New(ctx, runpostgres.Config{URL: testURL})
	Ok(t, err)
	defer db.Close()
	store := runpostgres.NewQueueStore(db)
	id, err := runs.NewID()
	Ok(t, err)
	bucket := "queue-alias-" + string(id)
	client := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String(endpoint), UsePathStyle: true, Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: os.Getenv("AWS_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY")}, nil
	})})
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	Ok(t, err)
	defer func() {
		objects, _ := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		if objects != nil {
			for _, object := range objects.Contents {
				_, _ = client.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: object.Key})
			}
		}
		_, _ = client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	}()
	parsed, err := url.Parse(endpoint)
	Ok(t, err)
	parsed.Host = strings.ToUpper(parsed.Host)
	alias := parsed.String()
	if configured := os.Getenv("ATLANTIS_QUEUE_TEST_S3_ALIAS_ENDPOINT"); configured != "" {
		alias = configured
	}
	Assert(t, endpoint != alias, "test requires distinct endpoint spellings")
	root, other := t.TempDir(), t.TempDir()
	run := func(dir string, args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		Assert(t, err == nil, "running Terraform: %v %s", err, out)
		return string(out)
	}
	for dir, host := range map[string]string{root: endpoint, other: alias} {
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
resource "terraform_data" "example" { input = "first apply" }
`, bucket, host)
		Ok(t, os.WriteFile(filepath.Join(dir, "main.tf"), []byte(config), 0600))
		run(dir, "init", "-input=false", "-no-color")
	}
	log := logging.NewNoopLogger(t)
	driver := &QueueDriver{Terraform: queueRealTerraform{binary: binary, ctx: ctx}, Store: store, Log: log}
	p := command.ProjectContext{Workspace: "default", Log: log}
	before, err := driver.state(p, root)
	Ok(t, err)
	Equals(t, queue.StateVersion{}, before)
	backend, err := queueStateID(p, root)
	Ok(t, err)
	otherBackend, err := queueStateID(p, other)
	Ok(t, err)
	if os.Getenv("ATLANTIS_QUEUE_TEST_S3_ALIAS_ENDPOINT") != "" {
		Assert(t, backend != otherBackend, "explicit alias must exercise lineage fencing independently of endpoint normalization")
	} else {
		Equals(t, backend, otherBackend)
	}
	run(root, "plan", "-input=false", "-no-color", "-out=saved.tfplan")
	entry, err := store.Enqueue(ctx, queue.Request{Repository: "github.com/o/r", PullNumber: 1, TargetBranch: "main", HeadSHA: "head", Actor: "user", Source: []byte(`{}`)})
	Ok(t, err)
	entry, err = store.ClaimNext(ctx, "worker", time.Minute)
	Ok(t, err)
	Ok(t, store.Reserve(ctx, entry, []string{backend}))
	next := entry
	next.State = queue.Planning
	entry, err = store.Save(ctx, entry, next, "planning")
	Ok(t, err)
	next = entry
	next.State = queue.Ready
	next.Plans = []queue.Plan{{Project: queue.Project{Directory: ".", Workspace: "default", StateID: backend, BackendID: backend}, State: before}}
	entry, err = store.Save(ctx, entry, next, "plans_saved")
	Ok(t, err)
	next = entry
	next.State, next.ApplyStarted = queue.Applying, true
	next.Plans[0].ApplyIntent = true
	entry, err = store.Save(ctx, entry, next, "apply_intent")
	Ok(t, err)
	run(root, "apply", "-input=false", "-no-color", "saved.tfplan")
	after, err := driver.state(p, root)
	Ok(t, err)
	Assert(t, after.Lineage != "", "first apply must create a lineage")
	observed, err := driver.state(p, other)
	Ok(t, err)
	Equals(t, after, observed)
	next = entry
	next.Plans[0].Applied = true
	next.Plans[0].ResultState = &after
	if checkpointFails {
		_, err = db.Database().ExecContext(ctx, `ALTER TABLE audit_events ADD CONSTRAINT reject_result_reservation_audit CHECK (event_type <> 'queue.result_states_reserved') NOT VALID`)
		Ok(t, err)
	}
	_, err = store.Save(ctx, entry, next, "project_applied")
	if checkpointFails {
		Assert(t, err != nil, "checkpoint must fail")
		_, err = db.Database().ExecContext(ctx, `UPDATE apply_queue_entries SET heartbeat_at=now()-interval '10 minutes' WHERE id=$1`, entry.ID)
		Ok(t, err)
		_, err = store.RecoverStale(ctx, time.Minute)
		Ok(t, err)
		entry, err = store.Get(ctx, entry.ID)
		Ok(t, err)
		Equals(t, queue.ApplyOutcomeUnknown, entry.State)
	} else {
		Ok(t, err)
	}
	// The queued apply is complete but has not merged. Another checkout of
	// exactly the same state must still be rejected by ordinary admission.
	release, err := driver.MutationGuard(p, other)
	if release != nil {
		release()
	}
	Assert(t, errors.Is(err, queue.ErrReserved), "backend alias bypassed the first apply's reservation: %v", err)
	if checkpointFails {
		_, err = store.Action(ctx, entry.ID, entry.Generation, "release", "operator", "", "Terraform stopped and resulting state reconciled")
		Ok(t, err)
		release, err = driver.MutationGuard(p, other)
		Ok(t, err)
		release()
	}
}
