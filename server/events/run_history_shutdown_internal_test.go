// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/jobs"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/require"
)

func TestRunHistoryInterruptActivePrioritizesAttemptsOverOutput(t *testing.T) {
	writer := &shutdownRunWriter{}
	history := NewRunHistory(writer, logging.NewNoopLogger(t))
	output := jobs.NewPersistentProjectCommandOutputHandler(&jobs.NoopProjectOutputHandler{}, &slowShutdownOutputWriter{}, logging.NewNoopLogger(t))
	history.SetOutputFinalizer(output)
	for i := range 3 {
		ctx := routedRunContext(t)
		runCommand := runs.CommandPlan
		if i == 2 {
			runCommand = runs.CommandApply
		}
		history.Begin(ctx, runCommand, runs.TriggerComment)
		if runCommand == runs.CommandApply {
			require.NoError(t, ctx.SideEffectMarker.MarkSideEffectStarted(context.Background()))
		}
		projectCtx := history.beginProject(command.ProjectContext{
			RunID: ctx.RunID, ProjectName: "network", RepoRelDir: "network", Workspace: "default", Log: ctx.Log,
		})
		output.SendStream(projectCtx, strings.Repeat("x", 32*1024-1), runs.OutputStdout, false)
	}

	budget, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	require.Equal(t, 3, history.InterruptActive(budget, "shutdown deadline"))
	require.Less(t, time.Since(start), 250*time.Millisecond)
	require.Len(t, writer.attemptsCompleted, 3, "optional output must not consume the budget for attempt classification")
	statuses := make([]runs.AttemptStatus, 0, 3)
	for _, attempt := range writer.attemptsCompleted {
		statuses = append(statuses, attempt.Status)
	}
	require.ElementsMatch(t, []runs.AttemptStatus{runs.AttemptInterrupted, runs.AttemptInterrupted, runs.AttemptUnknown}, statuses)
	require.Empty(t, writer.runsCompleted, "abandoned output must keep history incomplete")
}

func TestRunHistoryInterruptActiveSkipsDeferredComments(t *testing.T) {
	writer := &recordingRunWriter{}
	history := newTestRunHistory(t, writer)
	ctx := routedRunContext(t)
	history.Begin(ctx, runs.CommandPlan, runs.TriggerComment)
	commented := false
	require.True(t, history.DeferRunComment(ctx.RunID, func(bool) {
		commented = true
		time.Sleep(300 * time.Millisecond)
	}))

	budget, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	require.Equal(t, 1, history.InterruptActive(budget, "shutdown deadline"))
	require.Less(t, time.Since(start), 250*time.Millisecond)
	require.False(t, commented, "uncancelable VCS callbacks must not run during bounded classification")
	require.Len(t, writer.attemptsCompleted, 1)
}

func TestRunHistoryInterruptCancelsConcurrentOutputFinalization(t *testing.T) {
	writer := &shutdownRunWriter{}
	history := NewRunHistory(writer, logging.NewNoopLogger(t))
	output := &notifyingOutputFinalizer{
		PersistentProjectCommandOutputHandler: jobs.NewPersistentProjectCommandOutputHandler(&jobs.NoopProjectOutputHandler{}, &slowShutdownOutputWriter{}, logging.NewNoopLogger(t)),
		entered:                               make(chan struct{}),
	}
	history.SetOutputFinalizer(output)
	ctx := routedRunContext(t)
	lifecycle := history.Begin(ctx, runs.CommandPlan, runs.TriggerComment)
	projectCtx := history.beginProject(command.ProjectContext{
		RunID: ctx.RunID, ProjectName: "network", RepoRelDir: "network", Workspace: "default", Log: ctx.Log,
	})
	output.SendStream(projectCtx, strings.Repeat("x", 32*1024-1), runs.OutputStdout, false)
	finished := make(chan struct{})
	go func() {
		lifecycle.Finish()
		close(finished)
	}()
	<-output.entered

	budget, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	require.Equal(t, 1, history.InterruptActive(budget, "shutdown deadline"))
	require.Less(t, time.Since(start), 250*time.Millisecond)
	<-finished
	require.Len(t, writer.attemptsCompleted, 1)
	require.Equal(t, runs.AttemptInterrupted, writer.attemptsCompleted[0].Status)
}

func TestRunHistoryInterruptDoesNotWaitForCommentAlreadyPublishing(t *testing.T) {
	writer := &recordingRunWriter{}
	history := newTestRunHistory(t, writer)
	ctx := routedRunContext(t)
	lifecycle := history.Begin(ctx, runs.CommandPlan, runs.TriggerComment)
	commenting, release := make(chan struct{}), make(chan struct{})
	require.True(t, history.DeferRunComment(ctx.RunID, func(bool) {
		close(commenting)
		<-release
	}))
	finished := make(chan struct{})
	go func() {
		lifecycle.Finish()
		close(finished)
	}()
	<-commenting
	budget, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	interrupted := make(chan int, 1)
	go func() { interrupted <- history.InterruptActive(budget, "shutdown deadline") }()
	var waited bool
	select {
	case count := <-interrupted:
		require.Zero(t, count, "durable work finished before the comment started")
	case <-time.After(250 * time.Millisecond):
		waited = true
	}
	close(release)
	<-finished
	if waited {
		<-interrupted
	}
	require.False(t, waited, "comment publishing must be outside durable completion serialization")
}

func TestRunHistoryInterruptSharesBudgetWithTerminalAudit(t *testing.T) {
	writer := &shutdownAuditWriter{}
	history := NewRunHistory(writer, logging.NewNoopLogger(t))
	ctx := routedRunContext(t)
	history.Begin(ctx, runs.CommandPlan, runs.TriggerComment)
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Minute))
	defer cancel()
	require.Equal(t, 1, history.InterruptActive(expired, "shutdown deadline"))
	require.ErrorIs(t, writer.terminalContextErr, context.DeadlineExceeded)
}

func TestRunHistoryInterruptCancelsConcurrentDurableFinalization(t *testing.T) {
	writer := &shutdownProjectWriter{entered: make(chan struct{})}
	history := NewRunHistory(writer, logging.NewNoopLogger(t))
	ctx := routedRunContext(t)
	lifecycle := history.Begin(ctx, runs.CommandPlan, runs.TriggerComment)
	history.beginProject(command.ProjectContext{
		RunID: ctx.RunID, ProjectName: "network", RepoRelDir: "network", Workspace: "default", Log: ctx.Log,
	})
	finished := make(chan struct{})
	go func() {
		lifecycle.Finish()
		close(finished)
	}()
	<-writer.entered
	budget, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.Equal(t, 1, history.InterruptActive(budget, "shutdown deadline"))
	<-finished
	require.NoError(t, budget.Err(), "an in-flight normal write must be cancelled before spending the classification budget")
	require.Len(t, writer.attemptsCompleted, 1)
	require.Equal(t, runs.AttemptInterrupted, writer.attemptsCompleted[0].Status)
}

func TestRunHistoryInterruptStopsHeartbeatBeforeClassifying(t *testing.T) {
	writer := &shutdownHeartbeatWriter{entered: make(chan struct{}), stopped: make(chan struct{})}
	history := NewRunHistory(writer, logging.NewNoopLogger(t))
	history.attemptHeartbeat = time.Millisecond
	ctx := routedRunContext(t)
	history.Begin(ctx, runs.CommandPlan, runs.TriggerComment)
	<-writer.entered
	budget, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.Equal(t, 1, history.InterruptActive(budget, "shutdown deadline"))
	require.NoError(t, budget.Err())
	require.Len(t, writer.attemptsCompleted, 1)
	require.False(t, writer.completedBeforeHeartbeatStopped)
}

type shutdownProjectWriter struct {
	shutdownRunWriter
	entered chan struct{}
}

func (w *shutdownProjectWriter) CompleteProjectRun(ctx context.Context, _ runs.ProjectRunCompletion) error {
	close(w.entered)
	<-ctx.Done()
	return ctx.Err()
}

type shutdownHeartbeatWriter struct {
	shutdownRunWriter
	entered, stopped                chan struct{}
	completedBeforeHeartbeatStopped bool
	enteredOnce, stoppedOnce        sync.Once
}

func (w *shutdownHeartbeatWriter) HeartbeatAttempt(ctx context.Context, _ runs.ID, _ time.Time) error {
	w.enteredOnce.Do(func() { close(w.entered) })
	<-ctx.Done()
	w.stoppedOnce.Do(func() { close(w.stopped) })
	return ctx.Err()
}

func (w *shutdownHeartbeatWriter) CompleteAttempt(ctx context.Context, completion runs.AttemptCompletion) error {
	select {
	case <-w.stopped:
	default:
		w.completedBeforeHeartbeatStopped = true
	}
	return w.shutdownRunWriter.CompleteAttempt(ctx, completion)
}

type shutdownRunWriter struct{ recordingRunWriter }

func (w *shutdownRunWriter) CompleteAttempt(ctx context.Context, completion runs.AttemptCompletion) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return w.recordingRunWriter.CompleteAttempt(ctx, completion)
}

type slowShutdownOutputWriter struct{ recordingRunWriter }

func (w *slowShutdownOutputWriter) AppendOutput(ctx context.Context, _ []runs.OutputChunk) error {
	select {
	case <-time.After(300 * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type notifyingOutputFinalizer struct {
	*jobs.PersistentProjectCommandOutputHandler
	entered chan struct{}
	once    sync.Once
}

func (f *notifyingOutputFinalizer) FinishRun(runID runs.ID) bool {
	f.once.Do(func() { close(f.entered) })
	return f.PersistentProjectCommandOutputHandler.FinishRun(runID)
}

func (f *notifyingOutputFinalizer) FinishRunContext(ctx context.Context, runID runs.ID) bool {
	f.once.Do(func() { close(f.entered) })
	return f.PersistentProjectCommandOutputHandler.FinishRunContext(ctx, runID)
}

type shutdownAuditWriter struct {
	recordingRunWriter
	terminalContextErr error
}

func (w *shutdownAuditWriter) AppendAuditEvent(ctx context.Context, event runs.AuditEvent) error {
	if event.EventType == "plan.interrupted" {
		w.terminalContextErr = ctx.Err()
	}
	return w.recordingRunWriter.AppendAuditEvent(ctx, event)
}
