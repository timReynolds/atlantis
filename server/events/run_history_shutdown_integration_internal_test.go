// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
	runpostgres "github.com/runatlantis/atlantis/server/core/runs/postgres"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/require"
)

func TestRunHistoryInterruptRetriesCancelledAttemptCompletion(t *testing.T) {
	postgresURL := os.Getenv(haPlanPostgresEnv)
	if postgresURL == "" {
		t.Skip("set ATLANTIS_HA_FAILURE_POSTGRES_URL")
	}
	for _, runCommand := range []runs.Command{runs.CommandPlan, runs.CommandApply} {
		for _, outcome := range []struct{ committed, acknowledged bool }{{false, false}, {true, false}, {true, true}} {
			committed := outcome.committed
			name := string(runCommand) + "/cancel_before_commit"
			if committed {
				name = string(runCommand) + "/cancel_after_commit"
			}
			if outcome.acknowledged {
				name = string(runCommand) + "/acknowledge_after_commit"
			}
			t.Run(name, func(t *testing.T) {
				testURL, cleanup := isolatedHAPlanDatabase(t, postgresURL)
				defer cleanup()
				store, err := runpostgres.New(context.Background(), runpostgres.Config{URL: testURL})
				require.NoError(t, err)
				defer store.Close()
				writer := &cancelledAttemptStore{Store: store, entered: make(chan struct{}), commitFirst: committed, acknowledge: outcome.acknowledged}
				history := NewRunHistory(writer, logging.NewNoopLogger(t))
				ctx := routedRunContext(t)
				now := time.Now().UTC()
				require.NoError(t, store.RegisterInstance(context.Background(), runs.ExecutionInstance{
					ID: ctx.ExecutionInstanceID, ReplicaID: "shutdown-test", DeploymentID: ctx.ExecutionDeploymentID,
					StartedAt: now, HeartbeatAt: now,
				}))
				lifecycle := history.Begin(ctx, runCommand, runs.TriggerComment)
				require.True(t, lifecycle.CanExecute(), "%v", lifecycle.AdmissionError())
				require.True(t, history.IsRunHistoryComplete(ctx.RunID), "the cancellation test must start with complete history")
				if runCommand == runs.CommandApply {
					require.NoError(t, ctx.SideEffectMarker.MarkSideEffectStarted(context.Background()))
				}
				finished := make(chan struct{})
				go func() {
					lifecycle.Finish()
					close(finished)
				}()
				<-writer.entered
				budget, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				require.Equal(t, 1, history.InterruptActive(budget, "shutdown deadline"))
				<-finished
				require.NoError(t, budget.Err())
				expectedCalls := 2
				if outcome.acknowledged {
					expectedCalls = 1
				}
				require.Equal(t, expectedCalls, writer.calls)
				attempt, err := store.GetAttempt(context.Background(), ctx.AttemptID)
				require.NoError(t, err)
				if committed {
					if outcome.acknowledged {
						storedRun, err := store.GetRun(context.Background(), ctx.RunID)
						require.NoError(t, err)
						require.Equal(t, runs.StatusSucceeded, storedRun.Status, "acknowledged completion must win over concurrent interruption")
					} else {
						require.ErrorIs(t, writer.retryErr, runs.ErrConflict)
					}
					require.Equal(t, runs.AttemptSucceeded, attempt.Status, "an ambiguous cancellation must not rewrite an already committed outcome")
				} else {
					require.NoError(t, writer.retryErr)
					expected := runs.AttemptInterrupted
					if runCommand == runs.CommandApply {
						expected = runs.AttemptUnknown
					}
					require.Equal(t, expected, attempt.Status)
					storedRun, err := store.GetRun(context.Background(), ctx.RunID)
					require.NoError(t, err)
					if runCommand == runs.CommandApply {
						require.Equal(t, runs.StatusUnknown, storedRun.Status)
					} else {
						require.Equal(t, runs.StatusRunning, storedRun.Status, "interrupted plans remain retryable")
					}
				}
			})
		}
	}
}

type cancelledAttemptStore struct {
	*runpostgres.Store
	entered     chan struct{}
	once        sync.Once
	commitFirst bool
	acknowledge bool
	calls       int
	retryErr    error
}

func (w *cancelledAttemptStore) CompleteAttempt(ctx context.Context, completion runs.AttemptCompletion) error {
	w.calls++
	first := false
	w.once.Do(func() { first = true })
	if first {
		if w.commitFirst {
			if err := w.Store.CompleteAttempt(ctx, completion); err != nil {
				close(w.entered)
				return err
			}
		}
		close(w.entered)
		<-ctx.Done()
		if w.acknowledge {
			return nil
		}
		return ctx.Err()
	}
	w.retryErr = w.Store.CompleteAttempt(ctx, completion)
	return w.retryErr
}

func (w *cancelledAttemptStore) AppendAuditEvent(ctx context.Context, event runs.AuditEvent) error {
	// Isolate cancellation from the existing nil recovery metadata bug, which
	// otherwise makes requested audit events invalid before shutdown starts.
	if string(event.Metadata) == "null" {
		event.Metadata = runs.Metadata(`{}`)
	}
	return w.Store.AppendAuditEvent(ctx, event)
}
