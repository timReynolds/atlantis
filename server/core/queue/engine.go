// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
)

// Driver adapts the existing project builders, workflow runners and VCS client.
// Store remains authoritative at every side-effect boundary.
type Driver interface {
	Prepare(context.Context, Entry) (PreparedPlan, error)
	Plan(context.Context, Entry) ([]Plan, error)
	Validate(context.Context, Entry) error
	Apply(context.Context, Entry, int) (ApplyResult, error)
	Merge(context.Context, Entry) (MergeResult, error)
	Report(context.Context, Entry)
}
type ApplyResult struct {
	RunID, AttemptID runs.ID
	State            StateVersion
}
type MergeResult struct {
	SHA        string
	Equivalent bool
}
type TransientError struct{ Err error }

func (e TransientError) Error() string { return e.Err.Error() }
func (e TransientError) Unwrap() error { return e.Err }

// Engine executes one claimed PR at a time per call. PostgreSQL, not a local
// goroutine or a lease timeout, arbitrates branch and state admission.
type Engine struct {
	Store                Store
	Driver               Driver
	Worker               string
	Lease                time.Duration
	MaxTransientAttempts int
	Observe              func(string, time.Duration)
}

func (w *Engine) observe(event string, duration time.Duration) {
	if w.Observe != nil {
		w.Observe(event, duration)
	}
}
func (w *Engine) Tick(ctx context.Context) error {
	if w.Lease < time.Second {
		return errors.New("queue lease must be at least one second")
	}
	recovered, err := w.Store.RecoverStale(ctx, w.Lease)
	if err != nil {
		return err
	}
	for _, entry := range recovered {
		if entry.State.Recoverable() {
			w.observe("recovery", 0)
		}
		w.Driver.Report(ctx, entry)
	}
	entry, err := w.Store.ClaimNext(ctx, w.Worker, w.Lease)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	w.observe("wait", time.Since(entry.CreatedAt))
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	defer func() { cancel(); <-done }()
	go func() {
		defer close(done)
		ticker := time.NewTicker(w.Lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-workCtx.Done():
				return
			case <-ticker.C:
				if err := w.Store.Heartbeat(workCtx, entry); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	return w.execute(workCtx, entry)
}
func (w *Engine) save(ctx context.Context, e *Entry, next Entry, event string) error {
	next.Checkpoint = event
	saved, err := w.Store.Save(ctx, *e, next, event)
	if err != nil {
		return err
	}
	if saved.State != e.State {
		switch saved.State {
		case Replanning:
			w.observe("retry", 0)
		case Blocked:
			w.observe("blocked", 0)
		case PartiallyApplied, AppliedUnmerged, RecoveryRequired, ApplyOutcomeUnknown:
			w.observe("recovery", 0)
		}
	}
	*e = saved
	w.Driver.Report(ctx, saved)
	return nil
}
func (w *Engine) execute(ctx context.Context, e Entry) error {
	w.Driver.Report(ctx, e)
	if e.State == Preparing {
		prepared, err := w.Driver.Prepare(ctx, e)
		if err != nil {
			return w.beforeApplyError(ctx, &e, err)
		}
		if len(prepared.Projects) == 0 {
			return w.beforeApplyError(ctx, &e, errors.New("no affected Terraform projects"))
		}
		states := make([]string, 0, len(prepared.Projects))
		seen := map[string]bool{}
		for _, p := range prepared.Projects {
			if seen[p.StateID] {
				return w.beforeApplyError(ctx, &e, errors.New("multiple affected projects share a state; combine them into one execution unit before queueing"))
			}
			seen[p.StateID] = true
			states = append(states, p.StateID)
			if p.BackendID != "" {
				states = append(states, p.BackendID)
			}
		}
		if err := w.Store.Reserve(ctx, e, states); err != nil {
			return w.beforeApplyError(ctx, &e, err)
		}
		next := e
		next.Prepared = prepared
		next.State = Planning
		if err := w.save(ctx, &e, next, "planning"); err != nil {
			return err
		}
		started := time.Now()
		plans, err := w.Driver.Plan(ctx, e)
		w.observe("planning", time.Since(started))
		if err != nil {
			return w.beforeApplyError(ctx, &e, err)
		}
		if len(plans) != len(prepared.Projects) {
			return w.beforeApplyError(ctx, &e, errors.New("incomplete project plan set"))
		}
		plans, err = OrderPlans(prepared.Projects, plans)
		if err != nil {
			return w.beforeApplyError(ctx, &e, err)
		}
		next = e
		next.Plans = plans
		next.State = Ready
		next.Reason = ""
		if e.ApprovedDigest == "" || PlanDigest(plans) != e.ApprovedDigest {
			next.State = AwaitingApproval
			next.Reason = "review the refreshed plans and approve this generation"
		} else {
			next.ApprovedGeneration = e.Generation
		}
		if err := w.save(ctx, &e, next, "plans_saved"); err != nil {
			return err
		}
		if e.State == AwaitingApproval {
			return nil
		}
	}
	if e.State == Ready {
		states := make([]string, 0, len(e.Plans))
		for _, plan := range e.Plans {
			states = append(states, plan.Project.StateID)
			if plan.Project.BackendID != "" {
				states = append(states, plan.Project.BackendID)
			}
		}
		if err := w.Store.Reserve(ctx, e, states); err != nil {
			return w.beforeApplyError(ctx, &e, err)
		}
		if e.ApprovedGeneration != e.Generation || e.ApprovedDigest == "" || PlanDigest(e.Plans) != e.ApprovedDigest {
			return w.beforeApplyError(ctx, &e, errors.New("plan generation has not been approved"))
		}
		if err := w.Driver.Validate(ctx, e); err != nil {
			return w.beforeApplyError(ctx, &e, err)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		next := e
		next.State = Applying
		next.ApplyStarted = true
		if err := w.save(ctx, &e, next, "apply_intent"); err != nil {
			return err
		}
		for i := range e.Plans {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := w.Driver.Validate(ctx, e); err != nil {
				next = e
				next.State = RecoveryRequired
				next.Reason = err.Error()
				return w.save(ctx, &e, next, "apply_precondition_changed")
			}
			next = cloneEntry(e)
			next.Plans[i].ApplyIntent = true
			if err := w.save(ctx, &e, next, "project_apply_intent"); err != nil {
				return err
			}
			started := time.Now()
			result, applyErr := w.Driver.Apply(ctx, e, i)
			w.observe("apply", time.Since(started))
			next = cloneEntry(e)
			next.Plans[i].ApplyRunID = result.RunID
			next.Plans[i].ApplyAttemptID = result.AttemptID
			if applyErr != nil {
				next.Plans[i].Error = applyErr.Error()
				next.State = PartiallyApplied
				next.Reason = applyErr.Error()
				return w.save(ctx, &e, next, "project_apply_failed")
			}
			next.Plans[i].Applied = true
			next.Plans[i].ResultState = &result.State
			if err := w.save(ctx, &e, next, "project_applied"); err != nil {
				return err
			}
		}
		next = e
		next.State = Merging
		id, err := runs.NewID()
		if err != nil {
			return err
		}
		next.Merge = MergeAttempt{ID: id, ExpectedHeadSHA: e.HeadSHA, IntegrationSHA: e.Prepared.IntegrationSHA}
		if err := w.save(ctx, &e, next, "merge_intent"); err != nil {
			return err
		}
	}
	if e.State == Merging {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := w.Store.CheckClaim(ctx, e); err != nil {
			return err
		}
		started := time.Now()
		result, err := w.Driver.Merge(ctx, e)
		w.observe("merge", time.Since(started))
		next := e
		if err != nil {
			next.State = AppliedUnmerged
			next.Reason = err.Error()
			return w.save(ctx, &e, next, "merge_unconfirmed")
		}
		now := time.Now().UTC()
		next.Merge.ResultSHA = result.SHA
		next.Merge.ObservedAt = &now
		if !result.Equivalent {
			next.State = RecoveryRequired
			next.Reason = "merged configuration does not match applied configuration"
			return w.save(ctx, &e, next, "merge_configuration_mismatch")
		}
		next.State = Completed
		next.Reason = ""
		return w.save(ctx, &e, next, "completed")
	}
	return nil
}
func cloneEntry(e Entry) Entry { e.Plans = append([]Plan(nil), e.Plans...); return e }
func (w *Engine) beforeApplyError(ctx context.Context, e *Entry, cause error) error {
	if errors.Is(cause, ErrConflict) || ctx.Err() != nil {
		return cause
	}
	next := *e
	next.State = Blocked
	next.Reason = cause.Error()
	var transient TransientError
	if errors.Is(cause, ErrReserved) {
		// Contention is normal queue waiting, not a failed execution attempt.
		next.State = Replanning
		next.Generation++
		next.RetryAt = time.Now().UTC().Add(5 * time.Second)
	} else if errors.Is(cause, ErrReplan) || errors.As(cause, &transient) {
		next.RetryCount++
		if next.RetryCount <= w.MaxTransientAttempts {
			next.State = Replanning
			next.Generation++
			next.RetryAt = time.Now().UTC().Add(time.Duration(next.RetryCount) * 5 * time.Second)
		}
	}
	if err := w.save(ctx, e, next, "preparation_blocked"); err != nil {
		return fmt.Errorf("recording queue failure after %v: %w", cause, err)
	}
	return nil
}
