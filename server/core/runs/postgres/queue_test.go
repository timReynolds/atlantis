// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/core/queue"
	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/core/runs/postgres"
	. "github.com/runatlantis/atlantis/testing"
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
		Ok(t, err)
	}
	id := ""
	for value := range ids {
		if id == "" {
			id = value
		}
		Equals(t, id, value)
	}
	first, err := store.ClaimNext(ctx, "worker-a", 30*time.Second)
	Ok(t, err)
	_, err = store.Enqueue(ctx, queueRequest(2, "main"))
	Ok(t, err)
	_, err = store.ClaimNext(ctx, "worker-b", 30*time.Second)
	Assert(t, errors.Is(err, queue.ErrNotFound), "expected error matching %v", queue.ErrNotFound)
	_, err = store.Enqueue(ctx, queueRequest(3, "other"))
	Ok(t, err)
	other, err := store.ClaimNext(ctx, "worker-b", 30*time.Second)
	Ok(t, err)
	Ok(t, store.Reserve(ctx, first, []string{"state-b", "state-a"}))
	Assert(t, errors.Is(store.Reserve(ctx, other, []string{"state-c", "state-a"}), queue.ErrReserved), "expected error matching %v", queue.ErrReserved)
	// The losing all-or-nothing acquisition must not hold state-c.
	Ok(t, store.Reserve(ctx, first, []string{"state-c"}))
	_, err = store.AcquireMutation(ctx, []string{"state-a"}, "", "", 0, "normal-apply")
	Assert(t, errors.Is(err, queue.ErrReserved), "expected error matching %v", queue.ErrReserved)
	token, err := store.AcquireMutation(ctx, []string{"external"}, "", "", 0, "normal-apply")
	Ok(t, err)
	Assert(t, errors.Is(store.Reserve(ctx, other, []string{"external"}), queue.ErrReserved), "expected error matching %v", queue.ErrReserved)
	Ok(t, store.ReleaseMutation(ctx, token))
	Ok(t, store.Reserve(ctx, other, []string{"external"}))
	cancelled, err := store.Action(ctx, first.ID, first.Generation, "dequeue", "user", "cancel-1", "")
	Ok(t, err)
	Equals(t, queue.Cancelled, cancelled.State)
	Assert(t, errors.Is(store.CheckClaim(ctx, first), queue.ErrConflict), "expected error matching %v", queue.ErrConflict)
	Ok(t, store.Reserve(ctx, other, []string{"state-a"}))
	replay, err := store.Enqueue(ctx, request)
	Ok(t, err)
	Equals(t, first.ID, replay.ID)
	Equals(t, queue.Cancelled, replay.State)
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
	Ok(t, err)
	driver := &queueTestDriver{store: store, equivalent: true, projects: []queue.Project{{Name: "a", Directory: "a", Workspace: "default", StateID: "state-a"}, {Name: "b", Directory: "b", Workspace: "default", StateID: "state-b", DependsOn: []string{"a"}}}}
	engine := &queue.Engine{Store: store, Driver: driver, Worker: "worker", Lease: time.Minute, MaxTransientAttempts: 2}
	Ok(t, engine.Tick(ctx))
	e, err = store.Get(ctx, e.ID)
	Ok(t, err)
	Equals(t, queue.AwaitingApproval, e.State)
	Assert(t, driver.applies == 0, "expected zero")
	_, err = store.Action(ctx, e.ID, e.Generation-1, "approve", "reviewer", "stale-approval", "")
	Assert(t, errors.Is(err, queue.ErrConflict), "expected error matching %v", queue.ErrConflict)
	e, err = store.Action(ctx, e.ID, e.Generation, "approve", "reviewer", "approval", "")
	Ok(t, err)
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
			Ok(t, engine.Tick(ctx))
			e, err := store.Get(ctx, entry.ID)
			Ok(t, err)
			switch scenario {
			case "success":
				Equals(t, queue.Completed, e.State)
				Equals(t, 2, driver.applies)
				token, err := store.AcquireMutation(ctx, []string{"state-a"}, "", "", 0, "normal")
				Ok(t, err)
				Ok(t, store.ReleaseMutation(ctx, token))
			case "apply_failure":
				Equals(t, queue.PartiallyApplied, e.State)
				Equals(t, 1, driver.applies)
				Assert(t, driver.merges == 0, "expected zero")
			case "merge_timeout":
				Equals(t, queue.AppliedUnmerged, e.State)
				_, err = store.Action(ctx, e.ID, e.Generation, "recover", "operator", "recover", "")
				Ok(t, err)
				driver.mergeErr = nil
				Ok(t, engine.Tick(ctx))
				Assert(t, 2 == driver.applies, "merge recovery must never apply again")
				e, err = store.Get(ctx, e.ID)
				Ok(t, err)
				Equals(t, queue.Completed, e.State)
			case "merge_mismatch":
				Equals(t, queue.RecoveryRequired, e.State)
			case "state_changed":
				Equals(t, queue.Replanning, e.State)
				Assert(t, driver.applies == 0, "expected zero")
			}
			if scenario == "apply_failure" || scenario == "merge_mismatch" {
				_, err = store.AcquireMutation(ctx, []string{"state-a"}, "", "", 0, "normal")
				Assert(t, errors.Is(err, queue.ErrReserved), "expected error matching %v", queue.ErrReserved)
				_, err = store.Action(ctx, e.ID, e.Generation, "retry", "user", "retry", "")
				Assert(t, err != nil, "expected an error")
				Ok(t, engine.Tick(ctx))
				Assert(t, driver.applies <= 2, "expected value within limit")
			}
			events, err := store.Events(ctx, e.ID, 200)
			Ok(t, err)
			Assert(t, len(events) > 0, "expected nonempty value")
		})
	}
}
func TestQueueLeaseExpiryPreservesFences(t *testing.T) {
	for _, mutating := range []bool{false, true} {
		t.Run(fmt.Sprint(mutating), func(t *testing.T) {
			ctx, db, store := queueFixture(t)
			engine, _, entry := queuedEngine(t, ctx, store)
			claim, err := store.ClaimNext(ctx, "dead-worker", time.Minute)
			Ok(t, err)
			Ok(t, store.Reserve(ctx, claim, []string{"state-a", "state-b"}))
			if mutating {
				next := claim
				next.State = queue.Applying
				next.ApplyStarted = true
				claim, err = store.Save(ctx, claim, next, "apply_intent")
				Ok(t, err)
			}
			_, err = db.Database().ExecContext(ctx, `UPDATE apply_queue_entries SET heartbeat_at=now()-interval '10 minutes' WHERE id=$1`, entry.ID)
			Ok(t, err)
			recovered, err := store.RecoverStale(ctx, time.Minute)
			Ok(t, err)
			Equals(t, 1, len(recovered))
			e, err := store.Get(ctx, entry.ID)
			Ok(t, err)
			Assert(t, errors.Is(store.CheckClaim(ctx, claim), queue.ErrConflict), "expected error matching %v", queue.ErrConflict)
			_, err = store.AcquireMutation(ctx, []string{"state-a"}, "", "", 0, "normal")
			Assert(t, errors.Is(err, queue.ErrReserved), "expected error matching %v", queue.ErrReserved)
			if mutating {
				Equals(t, queue.ApplyOutcomeUnknown, e.State)
				Ok(t, engine.Tick(ctx))
				_, err = store.Action(ctx, e.ID, e.Generation, "dequeue", "user", "dequeue", "")
				Assert(t, err != nil, "expected an error")
			} else {
				Equals(t, queue.Queued, e.State)
				Equals(t, entry.Generation+1, e.Generation)
				Ok(t, engine.Tick(ctx))
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
	ErrContains(t, "after successful apply", engine.Tick(ctx))
	Equals(t, 1, driver.applies)
	_, err := db.Database().ExecContext(ctx, `UPDATE apply_queue_entries SET heartbeat_at=now()-interval '10 minutes' WHERE id=$1`, entry.ID)
	Ok(t, err)
	// Reconstruct both scheduler and adapter as after process restart.
	restarted := &queue.Engine{Store: postgres.NewQueueStore(db), Driver: driver, Worker: "replacement", Lease: time.Minute}
	Ok(t, restarted.Tick(ctx))
	actual, err := store.Get(ctx, entry.ID)
	Ok(t, err)
	Equals(t, queue.ApplyOutcomeUnknown, actual.State)
	Assert(t, actual.Plans[0].ApplyIntent, "expected true")
	Assert(t, !actual.Plans[0].Applied, "expected false")
	Assert(t, 1 == driver.applies, "ambiguous success must never replay")
	_, err = store.AcquireMutation(ctx, []string{"state-a"}, "", "", 0, "other")
	Assert(t, errors.Is(err, queue.ErrReserved), "expected error matching %v", queue.ErrReserved)
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
			Ok(t, engine.Tick(ctx))
			actual, err := store.Get(ctx, entry.ID)
			Ok(t, err)
			Assert(t, actual.Plans[0].Applied, "expected true")
			Assert(t, !actual.Plans[1].Applied, "expected false")
			if change {
				Equals(t, queue.RecoveryRequired, actual.State)
			} else {
				Equals(t, queue.PartiallyApplied, actual.State)
			}
			Assert(t, driver.merges == 0, "expected zero")
		})
	}
}
func TestQueueHeadAndBranchInvalidateGeneration(t *testing.T) {
	for _, applying := range []bool{false, true} {
		t.Run(fmt.Sprint(applying), func(t *testing.T) {
			ctx, _, store := queueFixture(t)
			_, _, entry := queuedEngine(t, ctx, store)
			claim, err := store.ClaimNext(ctx, "worker", time.Minute)
			Ok(t, err)
			if applying {
				next := claim
				next.ApplyStarted = true
				next.State = queue.Applying
				claim, err = store.Save(ctx, claim, next, "apply_intent")
				Ok(t, err)
			}
			request := queueRequest(1, "release")
			request.HeadSHA = "new-head"
			request.DeliveryID = "head-update"
			actual, err := store.Enqueue(ctx, request)
			Ok(t, err)
			Equals(t, entry.CreatedAt, actual.CreatedAt)
			Assert(t, len(actual.ApprovedDigest) == 0, "expected empty value")
			Assert(t, errors.Is(store.CheckClaim(ctx, claim), queue.ErrConflict), "expected error matching %v", queue.ErrConflict)
			_, err = store.Action(ctx, entry.ID, entry.Generation, "approve", "reviewer", "late-approval", "")
			Assert(t, err != nil, "expected an error")
			if applying {
				Equals(t, queue.RecoveryRequired, actual.State)
				Equals(t, "main", actual.TargetBranch)
				Equals(t, "head", actual.HeadSHA)
			} else {
				Equals(t, entry.Generation+1, actual.Generation)
				Equals(t, "release", actual.TargetBranch)
			}
			_, err = store.AcquireMutation(ctx, []string{"state-a"}, "", "", 0, "other")
			Assert(t, errors.Is(err, queue.ErrReserved), "expected error matching %v", queue.ErrReserved)
		})
	}
}
func TestQueueActiveEvidenceSurvivesRetention(t *testing.T) {
	ctx, db, store := queueFixture(t)
	entry, err := store.Enqueue(ctx, queueRequest(1, "main"))
	Ok(t, err)
	claim, err := store.ClaimNext(ctx, "worker", time.Minute)
	Ok(t, err)
	now := time.Now().UTC().Add(-time.Hour)
	runID := mustID(t)
	projectID := mustID(t)
	Ok(t, db.CreateRun(ctx, runs.Run{ID: runID, Repository: entry.Repository, Command: runs.CommandPlan, Trigger: runs.TriggerComment, Status: runs.StatusPending, CreatedAt: now}))
	Ok(t, db.StartRun(ctx, runID, now))
	Ok(t, db.CreateProjectRun(ctx, runs.ProjectRun{ID: projectID, RunID: runID, Directory: ".", Workspace: "default", Status: runs.StatusPending}))
	Ok(t, db.AppendOutput(ctx, []runs.OutputChunk{{ProjectRunID: projectID, Sequence: 0, Stream: runs.OutputStdout, Content: "plan evidence", CreatedAt: now}}))
	Ok(t, db.CompleteRun(ctx, runs.RunCompletion{ID: runID, Status: runs.StatusSucceeded, CompletedAt: now.Add(time.Second)}))
	Ok(t, store.LinkRun(ctx, claim, runID, ""))
	cutoff := time.Now().Add(time.Hour)
	policy := runs.RetentionPolicy{RunMetadataBefore: &cutoff, OutputBefore: &cutoff, AuditEventsBefore: &cutoff}
	result, err := db.ApplyRetention(ctx, policy)
	Ok(t, err)
	Assert(t, result.RunsDeleted == 0, "expected zero")
	Assert(t, result.OutputChunksDeleted == 0, "expected zero")
	Assert(t, result.AuditEventsDeleted == 0, "expected zero")
	_, err = store.Action(ctx, entry.ID, entry.Generation, "dequeue", "user", "cancel", "")
	Ok(t, err)
	result, err = db.ApplyRetention(ctx, policy)
	Ok(t, err)
	Assert(t, 1 == result.RunsDeleted, "expected equal values")
	Assert(t, 1 == result.OutputChunksDeleted, "expected equal values")
}

func TestQueueContentionWaitsAndUnrelatedWorkProceeds(t *testing.T) {
	ctx, _, store := queueFixture(t)
	_, _, first := queuedEngine(t, ctx, store)
	// Claim the first ready entry without starting its apply.
	// A separate branch sharing its state must wait without exhausting retries.
	_, err := store.Enqueue(ctx, queueRequest(2, "other"))
	Ok(t, err)
	claim, err := store.ClaimNext(ctx, "first-worker", time.Minute)
	Ok(t, err)
	Equals(t, first.ID, claim.ID)
	driver := &queueTestDriver{store: store, projects: []queue.Project{{Name: "alias", Directory: "alias", Workspace: "default", StateID: "state-a"}}}
	engine := &queue.Engine{Store: store, Driver: driver, Worker: "second-worker", Lease: time.Minute, MaxTransientAttempts: 0}
	Ok(t, engine.Tick(ctx))
	waiting, err := store.Find(ctx, first.Repository, 2)
	Ok(t, err)
	Equals(t, queue.Replanning, waiting.State)
	Assert(t, waiting.RetryCount == 0, "expected zero")
	_, err = store.Enqueue(ctx, queueRequest(3, "unrelated"))
	Ok(t, err)
	driver.projects = []queue.Project{{Name: "safe", Directory: "safe", Workspace: "default", StateID: "state-safe"}}
	Ok(t, engine.Tick(ctx))
	safe, err := store.Find(ctx, first.Repository, 3)
	Ok(t, err)
	Equals(t, queue.AwaitingApproval, safe.State)
}

func TestQueueManualRetryInvalidatesDelayedApproval(t *testing.T) {
	ctx, _, store := queueFixture(t)
	engine, driver, entry := queuedEngine(t, ctx, store)
	engine.MaxTransientAttempts = 0
	driver.changed = true
	Ok(t, engine.Tick(ctx))
	blocked, err := store.Get(ctx, entry.ID)
	Ok(t, err)
	Equals(t, queue.Blocked, blocked.State)
	retried, err := store.Action(ctx, entry.ID, entry.Generation, "retry", "user", "retry", "")
	Ok(t, err)
	Equals(t, entry.Generation+1, retried.Generation)
	_, err = store.Action(ctx, entry.ID, entry.Generation, "approve", "reviewer", "delayed-approval", "")
	Assert(t, errors.Is(err, queue.ErrConflict), "expected error matching %v", queue.ErrConflict)
}

func TestQueueGenerationSurvivesTerminalEntry(t *testing.T) {
	for _, terminal := range []queue.State{queue.Cancelled, queue.Completed} {
		t.Run(string(terminal), func(t *testing.T) {
			ctx, _, store := queueFixture(t)
			engine, driver, previous := queuedEngine(t, ctx, store)
			// Exercise a generation that has already advanced within this entry.
			changed := queueRequest(1, "main")
			changed.HeadSHA, changed.DeliveryID = "changed-head", "head-update"
			previous, err := store.Enqueue(ctx, changed)
			Ok(t, err)
			Assert(t, 2 == previous.Generation, "expected equal values")
			Ok(t, engine.Tick(ctx))
			if terminal == queue.Completed {
				_, err = store.Action(ctx, previous.ID, previous.Generation, "approve", "reviewer", "approve-generation-2", "")
				Ok(t, err)
				Ok(t, engine.Tick(ctx))
			} else {
				_, err = store.Action(ctx, previous.ID, previous.Generation, "dequeue", "user", "cancel-generation-2", "")
				Ok(t, err)
			}
			previous, err = store.Get(ctx, previous.ID)
			Ok(t, err)
			Equals(t, terminal, previous.State)

			type enqueueResult struct {
				entry queue.Entry
				err   error
			}
			results := make(chan enqueueResult, 8)
			var requests sync.WaitGroup
			for i := range 8 {
				requests.Go(func() {
					request := queueRequest(1, "release")
					request.HeadSHA = "next-lifecycle"
					request.DeliveryID = fmt.Sprintf("reenqueue-%d", i)
					entry, err := store.Enqueue(ctx, request)
					results <- enqueueResult{entry, err}
				})
			}
			requests.Wait()
			close(results)
			var current queue.Entry
			for result := range results {
				Ok(t, result.err)
				Equals(t, previous.Generation+1, result.entry.Generation)
				Assert(t, previous.ID != result.entry.ID, "expected different values")
				if current.ID != "" {
					Equals(t, current.ID, result.entry.ID)
				}
				current = result.entry
			}
			Ok(t, engine.Tick(ctx))
			_, err = store.Action(ctx, current.ID, previous.Generation, "approve", "reviewer", "delayed-old-approval", "")
			Assert(t, errors.Is(err, queue.ErrConflict), "expected error matching %v", queue.ErrConflict)
			actual, err := store.Get(ctx, current.ID)
			Ok(t, err)
			Equals(t, queue.AwaitingApproval, actual.State)
			approved, err := store.Action(ctx, current.ID, current.Generation, "approve", "reviewer", "current-approval", "")
			Ok(t, err)
			Equals(t, queue.Ready, approved.State)
			replayed, err := store.Enqueue(ctx, changed)
			Ok(t, err)
			Assert(t, previous.ID == replayed.ID, "old delivery must still resolve to its terminal entry")
			Equals(t, terminal, replayed.State)
			if terminal == queue.Cancelled {
				Assert(t, driver.applies == 0, "delayed approval cannot apply either lifecycle")
			}

			otherPull, err := store.Enqueue(ctx, queueRequest(2, "other"))
			Ok(t, err)
			Assert(t, 1 == otherPull.Generation, "expected equal values")
			otherRepoRequest := queueRequest(1, "main")
			otherRepoRequest.Repository = "github.com/o/other"
			otherRepo, err := store.Enqueue(ctx, otherRepoRequest)
			Ok(t, err)
			Assert(t, 1 == otherRepo.Generation, "expected equal values")
		})
	}
}

func TestQueueReportsRemainPendingAfterCompletionAndRestart(t *testing.T) {
	ctx, db, store := queueFixture(t)
	engine, _, entry := queuedEngine(t, ctx, store)
	Ok(t, engine.Tick(ctx))
	completed, err := store.Get(ctx, entry.ID)
	Ok(t, err)
	Equals(t, queue.Completed, completed.State)
	// A fresh adapter reconstructs the unfinished publication from PostgreSQL,
	// even though terminal entries are no longer in the active queue inventory.
	restarted := postgres.NewQueueStore(db)
	active, err := restarted.List(ctx, 200)
	Ok(t, err)
	Assert(t, len(active) == 0, "expected empty value")
	pending, err := restarted.PendingReports(ctx, 200)
	Ok(t, err)
	Equals(t, 1, len(pending))
	Equals(t, completed.ID, pending[0].ID)
	Equals(t, completed.Version, pending[0].Version)
	Ok(t, restarted.MarkReported(ctx, completed.ID, completed.Version))
	acknowledged, err := restarted.Get(ctx, completed.ID)
	Ok(t, err)
	Equals(t, completed.Version, acknowledged.Version)
	Equals(t, completed.Version, acknowledged.ReportedVersion)
	pending, err = restarted.PendingReports(ctx, 200)
	Ok(t, err)
	Assert(t, len(pending) == 0, "expected empty value")
}

func TestQueueReportAcknowledgementCannotHideNewerRevision(t *testing.T) {
	ctx, _, store := queueFixture(t)
	entry, err := store.Enqueue(ctx, queueRequest(1, "main"))
	Ok(t, err)
	claimed, err := store.ClaimNext(ctx, "worker", time.Minute)
	Ok(t, err)
	Assert(t, errors.Is(store.MarkReported(ctx, entry.ID, entry.Version), queue.ErrConflict), "expected error matching %v", queue.ErrConflict)
	pending, err := store.PendingReports(ctx, 200)
	Ok(t, err)
	Equals(t, 1, len(pending))
	Equals(t, claimed.Version, pending[0].Version)
	Ok(t, store.MarkReported(ctx, claimed.ID, claimed.Version))
	// Save uses a snapshot taken before the acknowledgement, but preserves the
	// acknowledgement while making the new state visible as another revision.
	next := claimed
	next.State = queue.Blocked
	saved, err := store.Save(ctx, claimed, next, "preparation_blocked")
	Ok(t, err)
	Equals(t, claimed.Version, saved.ReportedVersion)
	Assert(t, errors.Is(store.MarkReported(ctx, claimed.ID, claimed.Version), queue.ErrConflict), "expected error matching %v", queue.ErrConflict)
	pending, err = store.PendingReports(ctx, 200)
	Ok(t, err)
	Equals(t, 1, len(pending))
	Equals(t, saved.Version, pending[0].Version)
	Ok(t, store.MarkReported(ctx, saved.ID, saved.Version))
	pending, err = store.PendingReports(ctx, 200)
	Ok(t, err)
	Assert(t, len(pending) == 0, "expected empty value")
}

func TestQueueAuditRetainsIntegrationAndReservationsAfterInvalidation(t *testing.T) {
	ctx, _, store := queueFixture(t)
	entry, err := store.Enqueue(ctx, queueRequest(1, "main"))
	Ok(t, err)
	claimed, err := store.ClaimNext(ctx, "worker", time.Minute)
	Ok(t, err)
	// Reservation happens before the prepared manifest is committed, so its
	// event must explicitly retain the complete sorted set that was acquired.
	Ok(t, store.Reserve(ctx, claimed, []string{"state-z", "state-a", "state-a"}))
	prepared := queue.PreparedPlan{
		BaseSHA: "base-v1", HeadSHA: claimed.HeadSHA, IntegrationSHA: "integration-v1",
		TreeSHA: "tree-v1", Configuration: "configuration-v1", ArtifactGeneration: mustID(t),
		Projects: []queue.Project{{Name: "network", Directory: "network", Workspace: "default", StateID: "state-a"}},
	}
	next := claimed
	next.State, next.Checkpoint, next.Prepared = queue.Planning, "planning", prepared
	_, err = store.Save(ctx, claimed, next, "planning")
	Ok(t, err)
	changed := queueRequest(1, "release")
	changed.HeadSHA, changed.DeliveryID = "head-v2", "head-update"
	current, err := store.Enqueue(ctx, changed)
	Ok(t, err)
	Equals(t, entry.Generation+1, current.Generation)
	Assert(t, len(current.Prepared.IntegrationSHA) == 0, "expected empty value")

	events, err := store.Events(ctx, entry.ID, 200)
	Ok(t, err)
	var foundPlan, foundReservation bool
	for _, event := range events {
		if event.Type != "queue.planning" && event.Type != "queue.states_reserved" {
			continue
		}
		var recorded struct {
			Prepared     queue.PreparedPlan `json:"prepared"`
			HeadSHA      string             `json:"head_sha"`
			TargetBranch string             `json:"target_branch"`
			States       []string           `json:"states"`
		}
		Ok(t, json.Unmarshal(event.Payload, &recorded))
		Equals(t, entry.Generation, event.Generation)
		Equals(t, entry.HeadSHA, recorded.HeadSHA)
		Equals(t, entry.TargetBranch, recorded.TargetBranch)
		if event.Type == "queue.planning" {
			foundPlan = true
			Equals(t, prepared, recorded.Prepared)
		} else {
			foundReservation = true
			Equals(t, []string{"state-a", "state-z"}, recorded.States)
		}
	}
	Assert(t, foundPlan, "expected true")
	Assert(t, foundReservation, "expected true")
}
