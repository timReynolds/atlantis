// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/core/queue"
	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/core/runs/postgres"
	"github.com/stretchr/testify/require"
)

func queueFixture(t *testing.T) (context.Context, *postgres.Store, *postgres.QueueStore) {
	t.Helper()
	url := os.Getenv("ATLANTIS_POSTGRES_TEST_URL")
	if testing.Short() || url == "" {
		t.Skip("requires ATLANTIS_POSTGRES_TEST_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	store, cleanup := newIsolatedStore(t, ctx, url)
	t.Cleanup(cleanup)
	return ctx, store, postgres.NewQueueStore(store)
}
func queueRequest(pull int, branch string) queue.Request {
	return queue.Request{Repository: "github.com/o/r", PullNumber: pull, TargetBranch: branch, HeadSHA: "head", Actor: "user", DeliveryID: fmt.Sprintf("enqueue-%d", pull), Source: []byte(`{}`)}
}
func TestQueueConcurrentAdmissionAndReservations(t *testing.T) {
	ctx, _, store := queueFixture(t)
	request := queueRequest(1, "main")
	var wg sync.WaitGroup
	ids := make(chan string, 16)
	errs := make(chan error, 16)
	for range 16 {
		wg.Go(func() { e, err := store.Enqueue(ctx, request); ids <- string(e.ID); errs <- err })
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	id := ""
	for value := range ids {
		if id == "" {
			id = value
		}
		require.Equal(t, id, value)
	}
	first, err := store.ClaimNext(ctx, "worker-a", 30*time.Second)
	require.NoError(t, err)
	_, err = store.Enqueue(ctx, queueRequest(2, "main"))
	require.NoError(t, err)
	_, err = store.ClaimNext(ctx, "worker-b", 30*time.Second)
	require.ErrorIs(t, err, queue.ErrNotFound)
	_, err = store.Enqueue(ctx, queueRequest(3, "other"))
	require.NoError(t, err)
	other, err := store.ClaimNext(ctx, "worker-b", 30*time.Second)
	require.NoError(t, err)
	require.NoError(t, store.Reserve(ctx, first, []string{"state-b", "state-a"}))
	require.ErrorIs(t, store.Reserve(ctx, other, []string{"state-c", "state-a"}), queue.ErrReserved)
	// The losing all-or-nothing acquisition must not hold state-c.
	require.NoError(t, store.Reserve(ctx, first, []string{"state-c"}))
	_, err = store.AcquireMutation(ctx, []string{"state-a"}, "", "", 0, "normal-apply")
	require.ErrorIs(t, err, queue.ErrReserved)
	token, err := store.AcquireMutation(ctx, []string{"external"}, "", "", 0, "normal-apply")
	require.NoError(t, err)
	require.ErrorIs(t, store.Reserve(ctx, other, []string{"external"}), queue.ErrReserved)
	require.NoError(t, store.ReleaseMutation(ctx, token))
	require.NoError(t, store.Reserve(ctx, other, []string{"external"}))
	cancelled, err := store.Action(ctx, first.ID, first.Generation, "dequeue", "user", "cancel-1", "")
	require.NoError(t, err)
	require.Equal(t, queue.Cancelled, cancelled.State)
	require.ErrorIs(t, store.CheckClaim(ctx, first), queue.ErrConflict)
	require.NoError(t, store.Reserve(ctx, other, []string{"state-a"}))
	replay, err := store.Enqueue(ctx, request)
	require.NoError(t, err)
	require.Equal(t, first.ID, replay.ID)
	require.Equal(t, queue.Cancelled, replay.State)
}

type queueTestDriver struct {
	store               queue.Store
	projects            []queue.Project
	applies, merges     int
	applyErr, mergeErr  error
	failAt, changeAfter int
	changed             bool
	equivalent          bool
}

func (d *queueTestDriver) Prepare(context.Context, queue.Entry) (queue.PreparedPlan, error) {
	return queue.PreparedPlan{Projects: d.projects}, nil
}
func (d *queueTestDriver) Plan(context.Context, queue.Entry) ([]queue.Plan, error) {
	var plans []queue.Plan
	for _, p := range d.projects {
		plans = append(plans, queue.Plan{Project: p, SemanticDigest: "approved-plan"})
	}
	return plans, nil
}
func (d *queueTestDriver) Validate(context.Context, queue.Entry) error {
	if d.changed || (d.changeAfter > 0 && d.applies >= d.changeAfter) {
		return queue.ErrReplan
	}
	return nil
}
func (d *queueTestDriver) Apply(ctx context.Context, e queue.Entry, i int) (queue.ApplyResult, error) {
	if err := d.store.CheckClaim(ctx, e); err != nil {
		return queue.ApplyResult{}, err
	}
	d.applies++
	if d.failAt == d.applies {
		return queue.ApplyResult{}, errors.New("provider failed")
	}
	return queue.ApplyResult{State: queue.StateVersion{Lineage: "lineage", Serial: uint64(d.applies)}}, d.applyErr
}
func (d *queueTestDriver) Merge(context.Context, queue.Entry) (queue.MergeResult, error) {
	d.merges++
	return queue.MergeResult{SHA: "merged", Equivalent: d.equivalent}, d.mergeErr
}
func (d *queueTestDriver) Report(context.Context, queue.Entry) {}
func queuedEngine(t *testing.T, ctx context.Context, store *postgres.QueueStore) (*queue.Engine, *queueTestDriver, queue.Entry) {
	e, err := store.Enqueue(ctx, queueRequest(1, "main"))
	require.NoError(t, err)
	driver := &queueTestDriver{store: store, equivalent: true, projects: []queue.Project{{Name: "a", Directory: "a", Workspace: "default", StateID: "state-a"}, {Name: "b", Directory: "b", Workspace: "default", StateID: "state-b", DependsOn: []string{"a"}}}}
	engine := &queue.Engine{Store: store, Driver: driver, Worker: "worker", Lease: time.Minute, MaxTransientAttempts: 2}
	require.NoError(t, engine.Tick(ctx))
	e, err = store.Get(ctx, e.ID)
	require.NoError(t, err)
	require.Equal(t, queue.AwaitingApproval, e.State)
	require.Zero(t, driver.applies)
	_, err = store.Action(ctx, e.ID, e.Generation-1, "approve", "reviewer", "stale-approval", "")
	require.ErrorIs(t, err, queue.ErrConflict)
	e, err = store.Action(ctx, e.ID, e.Generation, "approve", "reviewer", "approval", "")
	require.NoError(t, err)
	return engine, driver, e
}
func TestQueueApplyMergeAndCrashRecovery(t *testing.T) {
	for _, scenario := range []string{"success", "apply_failure", "merge_timeout", "merge_mismatch", "state_changed"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, _, store := queueFixture(t)
			engine, driver, entry := queuedEngine(t, ctx, store)
			switch scenario {
			case "apply_failure":
				driver.applyErr = errors.New("provider failed")
			case "merge_timeout":
				driver.mergeErr = errors.New("timeout")
			case "merge_mismatch":
				driver.equivalent = false
			case "state_changed":
				driver.changed = true
			}
			require.NoError(t, engine.Tick(ctx))
			e, err := store.Get(ctx, entry.ID)
			require.NoError(t, err)
			switch scenario {
			case "success":
				require.Equal(t, queue.Completed, e.State)
				require.Equal(t, 2, driver.applies)
				token, err := store.AcquireMutation(ctx, []string{"state-a"}, "", "", 0, "normal")
				require.NoError(t, err)
				require.NoError(t, store.ReleaseMutation(ctx, token))
			case "apply_failure":
				require.Equal(t, queue.PartiallyApplied, e.State)
				require.Equal(t, 1, driver.applies)
				require.Zero(t, driver.merges)
			case "merge_timeout":
				require.Equal(t, queue.AppliedUnmerged, e.State)
				_, err = store.Action(ctx, e.ID, e.Generation, "recover", "operator", "recover", "")
				require.NoError(t, err)
				driver.mergeErr = nil
				require.NoError(t, engine.Tick(ctx))
				require.Equal(t, 2, driver.applies, "merge recovery must never apply again")
				e, err = store.Get(ctx, e.ID)
				require.NoError(t, err)
				require.Equal(t, queue.Completed, e.State)
			case "merge_mismatch":
				require.Equal(t, queue.RecoveryRequired, e.State)
			case "state_changed":
				require.Equal(t, queue.Replanning, e.State)
				require.Zero(t, driver.applies)
			}
			if scenario == "apply_failure" || scenario == "merge_mismatch" {
				_, err = store.AcquireMutation(ctx, []string{"state-a"}, "", "", 0, "normal")
				require.ErrorIs(t, err, queue.ErrReserved)
				_, err = store.Action(ctx, e.ID, e.Generation, "retry", "user", "retry", "")
				require.Error(t, err)
				require.NoError(t, engine.Tick(ctx))
				require.LessOrEqual(t, driver.applies, 2)
			}
			events, err := store.Events(ctx, e.ID, 200)
			require.NoError(t, err)
			require.NotEmpty(t, events)
		})
	}
}
func TestQueueLeaseExpiryPreservesFences(t *testing.T) {
	for _, mutating := range []bool{false, true} {
		t.Run(fmt.Sprint(mutating), func(t *testing.T) {
			ctx, db, store := queueFixture(t)
			engine, _, entry := queuedEngine(t, ctx, store)
			claim, err := store.ClaimNext(ctx, "dead-worker", time.Minute)
			require.NoError(t, err)
			require.NoError(t, store.Reserve(ctx, claim, []string{"state-a", "state-b"}))
			if mutating {
				next := claim
				next.State = queue.Applying
				next.ApplyStarted = true
				claim, err = store.Save(ctx, claim, next, "apply_intent")
				require.NoError(t, err)
			}
			_, err = db.Database().ExecContext(ctx, `UPDATE apply_queue_entries SET heartbeat_at=now()-interval '10 minutes' WHERE id=$1`, entry.ID)
			require.NoError(t, err)
			recovered, err := store.RecoverStale(ctx, time.Minute)
			require.NoError(t, err)
			require.Len(t, recovered, 1)
			e, err := store.Get(ctx, entry.ID)
			require.NoError(t, err)
			require.ErrorIs(t, store.CheckClaim(ctx, claim), queue.ErrConflict)
			_, err = store.AcquireMutation(ctx, []string{"state-a"}, "", "", 0, "normal")
			require.ErrorIs(t, err, queue.ErrReserved)
			if mutating {
				require.Equal(t, queue.ApplyOutcomeUnknown, e.State)
				require.NoError(t, engine.Tick(ctx))
				_, err = store.Action(ctx, e.ID, e.Generation, "dequeue", "user", "dequeue", "")
				require.Error(t, err)
			} else {
				require.Equal(t, queue.Queued, e.State)
				require.Equal(t, entry.Generation+1, e.Generation)
				require.NoError(t, engine.Tick(ctx))
			}
		})
	}
}

type queueLostCheckpoint struct{ queue.Store }

func (s queueLostCheckpoint) Save(ctx context.Context, old, next queue.Entry, event string) (queue.Entry, error) {
	if event == "project_applied" {
		return queue.Entry{}, errors.New("database unavailable after successful apply")
	}
	return s.Store.Save(ctx, old, next, event)
}
func TestQueueRestartAfterApplyBeforeCheckpoint(t *testing.T) {
	ctx, db, store := queueFixture(t)
	engine, driver, entry := queuedEngine(t, ctx, store)
	engine.Store = queueLostCheckpoint{store}
	require.ErrorContains(t, engine.Tick(ctx), "after successful apply")
	require.Equal(t, 1, driver.applies)
	_, err := db.Database().ExecContext(ctx, `UPDATE apply_queue_entries SET heartbeat_at=now()-interval '10 minutes' WHERE id=$1`, entry.ID)
	require.NoError(t, err)
	// Reconstruct both scheduler and adapter as after process restart.
	restarted := &queue.Engine{Store: postgres.NewQueueStore(db), Driver: driver, Worker: "replacement", Lease: time.Minute}
	require.NoError(t, restarted.Tick(ctx))
	actual, err := store.Get(ctx, entry.ID)
	require.NoError(t, err)
	require.Equal(t, queue.ApplyOutcomeUnknown, actual.State)
	require.True(t, actual.Plans[0].ApplyIntent)
	require.False(t, actual.Plans[0].Applied)
	require.Equal(t, 1, driver.applies, "ambiguous success must never replay")
	_, err = store.AcquireMutation(ctx, []string{"state-a"}, "", "", 0, "other")
	require.ErrorIs(t, err, queue.ErrReserved)
}
func TestQueuePartialSuccessAndChangedPreconditions(t *testing.T) {
	for _, change := range []bool{false, true} {
		t.Run(fmt.Sprint(change), func(t *testing.T) {
			ctx, _, store := queueFixture(t)
			engine, driver, entry := queuedEngine(t, ctx, store)
			if change {
				driver.changeAfter = 1
			} else {
				driver.failAt = 2
			}
			require.NoError(t, engine.Tick(ctx))
			actual, err := store.Get(ctx, entry.ID)
			require.NoError(t, err)
			require.True(t, actual.Plans[0].Applied)
			require.False(t, actual.Plans[1].Applied)
			if change {
				require.Equal(t, queue.RecoveryRequired, actual.State)
			} else {
				require.Equal(t, queue.PartiallyApplied, actual.State)
			}
			require.Zero(t, driver.merges)
		})
	}
}
func TestQueueHeadAndBranchInvalidateGeneration(t *testing.T) {
	for _, applying := range []bool{false, true} {
		t.Run(fmt.Sprint(applying), func(t *testing.T) {
			ctx, _, store := queueFixture(t)
			_, _, entry := queuedEngine(t, ctx, store)
			claim, err := store.ClaimNext(ctx, "worker", time.Minute)
			require.NoError(t, err)
			if applying {
				next := claim
				next.ApplyStarted = true
				next.State = queue.Applying
				claim, err = store.Save(ctx, claim, next, "apply_intent")
				require.NoError(t, err)
			}
			request := queueRequest(1, "release")
			request.HeadSHA = "new-head"
			request.DeliveryID = "head-update"
			actual, err := store.Enqueue(ctx, request)
			require.NoError(t, err)
			require.Equal(t, entry.CreatedAt, actual.CreatedAt)
			require.Empty(t, actual.ApprovedDigest)
			require.ErrorIs(t, store.CheckClaim(ctx, claim), queue.ErrConflict)
			_, err = store.Action(ctx, entry.ID, entry.Generation, "approve", "reviewer", "late-approval", "")
			require.Error(t, err)
			if applying {
				require.Equal(t, queue.RecoveryRequired, actual.State)
				require.Equal(t, "main", actual.TargetBranch)
				require.Equal(t, "head", actual.HeadSHA)
			} else {
				require.Equal(t, entry.Generation+1, actual.Generation)
				require.Equal(t, "release", actual.TargetBranch)
			}
			_, err = store.AcquireMutation(ctx, []string{"state-a"}, "", "", 0, "other")
			require.ErrorIs(t, err, queue.ErrReserved)
		})
	}
}
func TestQueueActiveEvidenceSurvivesRetention(t *testing.T) {
	ctx, db, store := queueFixture(t)
	entry, err := store.Enqueue(ctx, queueRequest(1, "main"))
	require.NoError(t, err)
	claim, err := store.ClaimNext(ctx, "worker", time.Minute)
	require.NoError(t, err)
	now := time.Now().UTC().Add(-time.Hour)
	runID := mustID(t)
	projectID := mustID(t)
	require.NoError(t, db.CreateRun(ctx, runs.Run{ID: runID, Repository: entry.Repository, Command: runs.CommandPlan, Trigger: runs.TriggerComment, Status: runs.StatusPending, CreatedAt: now}))
	require.NoError(t, db.StartRun(ctx, runID, now))
	require.NoError(t, db.CreateProjectRun(ctx, runs.ProjectRun{ID: projectID, RunID: runID, Directory: ".", Workspace: "default", Status: runs.StatusPending}))
	require.NoError(t, db.AppendOutput(ctx, []runs.OutputChunk{{ProjectRunID: projectID, Sequence: 0, Stream: runs.OutputStdout, Content: "plan evidence", CreatedAt: now}}))
	require.NoError(t, db.CompleteRun(ctx, runs.RunCompletion{ID: runID, Status: runs.StatusSucceeded, CompletedAt: now.Add(time.Second)}))
	require.NoError(t, store.LinkRun(ctx, claim, runID, ""))
	cutoff := time.Now().Add(time.Hour)
	policy := runs.RetentionPolicy{RunMetadataBefore: &cutoff, OutputBefore: &cutoff, AuditEventsBefore: &cutoff}
	result, err := db.ApplyRetention(ctx, policy)
	require.NoError(t, err)
	require.Zero(t, result.RunsDeleted)
	require.Zero(t, result.OutputChunksDeleted)
	require.Zero(t, result.AuditEventsDeleted)
	_, err = store.Action(ctx, entry.ID, entry.Generation, "dequeue", "user", "cancel", "")
	require.NoError(t, err)
	result, err = db.ApplyRetention(ctx, policy)
	require.NoError(t, err)
	require.EqualValues(t, 1, result.RunsDeleted)
	require.EqualValues(t, 1, result.OutputChunksDeleted)
}

func TestQueueContentionWaitsAndUnrelatedWorkProceeds(t *testing.T) {
	ctx, _, store := queueFixture(t)
	_, _, first := queuedEngine(t, ctx, store)
	// Claim the first ready entry without starting its apply.
	// A separate branch sharing its state must wait without exhausting retries.
	_, err := store.Enqueue(ctx, queueRequest(2, "other"))
	require.NoError(t, err)
	claim, err := store.ClaimNext(ctx, "first-worker", time.Minute)
	require.NoError(t, err)
	require.Equal(t, first.ID, claim.ID)
	driver := &queueTestDriver{store: store, projects: []queue.Project{{Name: "alias", Directory: "alias", Workspace: "default", StateID: "state-a"}}}
	engine := &queue.Engine{Store: store, Driver: driver, Worker: "second-worker", Lease: time.Minute, MaxTransientAttempts: 0}
	require.NoError(t, engine.Tick(ctx))
	waiting, err := store.Find(ctx, first.Repository, 2)
	require.NoError(t, err)
	require.Equal(t, queue.Replanning, waiting.State)
	require.Zero(t, waiting.RetryCount)
	_, err = store.Enqueue(ctx, queueRequest(3, "unrelated"))
	require.NoError(t, err)
	driver.projects = []queue.Project{{Name: "safe", Directory: "safe", Workspace: "default", StateID: "state-safe"}}
	require.NoError(t, engine.Tick(ctx))
	safe, err := store.Find(ctx, first.Repository, 3)
	require.NoError(t, err)
	require.Equal(t, queue.AwaitingApproval, safe.State)
}

func TestQueueManualRetryInvalidatesDelayedApproval(t *testing.T) {
	ctx, _, store := queueFixture(t)
	engine, driver, entry := queuedEngine(t, ctx, store)
	engine.MaxTransientAttempts = 0
	driver.changed = true
	require.NoError(t, engine.Tick(ctx))
	blocked, err := store.Get(ctx, entry.ID)
	require.NoError(t, err)
	require.Equal(t, queue.Blocked, blocked.State)
	retried, err := store.Action(ctx, entry.ID, entry.Generation, "retry", "user", "retry", "")
	require.NoError(t, err)
	require.Equal(t, entry.Generation+1, retried.Generation)
	_, err = store.Action(ctx, entry.ID, entry.Generation, "approve", "reviewer", "delayed-approval", "")
	require.ErrorIs(t, err, queue.ErrConflict)
}
