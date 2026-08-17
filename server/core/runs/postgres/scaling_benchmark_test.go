// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package postgres_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/stretchr/testify/require"
)

// BenchmarkStoreProjectHistoryFanout measures the Stage 1 attempt-scoped
// durable metadata path. It deliberately excludes Git checkout, Terraform,
// providers, S3, Redis, and VCS APIs so those bottlenecks remain visible in the
// deployment benchmark rather than being hidden inside a synthetic score.
func BenchmarkStoreProjectHistoryFanout(b *testing.B) {
	rawURL := os.Getenv("ATLANTIS_POSTGRES_BENCHMARK_URL")
	if rawURL == "" {
		b.Skip("set ATLANTIS_POSTGRES_BENCHMARK_URL")
	}
	for _, projectCount := range []int{10, 50, 100, 300, 600} {
		b.Run(fmt.Sprintf("projects-%03d", projectCount), func(b *testing.B) {
			ctx := context.Background()
			store, cleanup := newIsolatedStore(b, ctx, rawURL)
			defer cleanup()
			store.Database().SetMaxOpenConns(10)
			store.Database().SetMaxIdleConns(5)
			instanceID, err := runs.NewID()
			require.NoError(b, err)
			instanceStartedAt := time.Now().UTC()
			require.NoError(b, store.RegisterInstance(ctx, runs.ExecutionInstance{
				ID: instanceID, ReplicaID: "benchmark-replica", DeploymentID: "benchmark",
				StartedAt: instanceStartedAt, HeartbeatAt: instanceStartedAt,
			}))
			b.ReportAllocs()
			b.ResetTimer()

			for iteration := 0; iteration < b.N; iteration++ {
				startedAt := time.Now().UTC()
				pullNumber := iteration + 1
				headRef := fmt.Sprintf("benchmark-%d", iteration)
				headSHA := fmt.Sprintf("%040x", iteration+1)
				concurrencyKey := fmt.Sprintf("benchmark/pull/%d", pullNumber)
				ownershipClaimID := fmt.Sprintf("benchmark-claim-%d", iteration)

				// Every routed production command passes through
				// RunHistory.Begin, which calls PrepareAttemptTakeover before
				// CreateRun. Excluding that mandatory serial transaction from
				// the timed section would understate control-plane latency.
				_, err := store.PrepareAttemptTakeover(ctx, runs.AttemptTakeoverRequest{
					DeploymentID: "benchmark", ConcurrencyKey: concurrencyKey, OwnershipClaimID: ownershipClaimID,
					HeartbeatBefore: startedAt, RecoveredAt: startedAt,
					Repository: "benchmark/infrastructure", PullNumber: &pullNumber, Command: runs.CommandPlan,
					Trigger: runs.TriggerAPI, Actor: "benchmark", BaseRef: "main",
					HeadRef: headRef, HeadSHA: headSHA,
				})
				require.NoError(b, err)

				runID, err := runs.NewID()
				require.NoError(b, err)
				require.NoError(b, store.CreateRun(ctx, runs.Run{
					ID: runID, Repository: "benchmark/infrastructure", PullNumber: &pullNumber,
					Command: runs.CommandPlan, Trigger: runs.TriggerAPI, Actor: "benchmark",
					BaseRef: "main", HeadRef: headRef,
					HeadSHA: headSHA, Status: runs.StatusRunning,
					CreatedAt: startedAt, StartedAt: &startedAt,
				}))
				// production audits plan.requested immediately after CreateRun.
				require.NoError(b, store.AppendAuditEvent(ctx, runs.AuditEvent{
					ID: mustBenchmarkID(b), Repository: "benchmark/infrastructure", PullNumber: &pullNumber,
					RunID: &runID, Actor: "benchmark", EventType: "plan.requested", CreatedAt: startedAt,
				}))
				attemptID, err := runs.NewID()
				require.NoError(b, err)
				require.NoError(b, store.CreateAttempt(ctx, runs.RunAttempt{
					ID: attemptID, RunID: runID, InstanceID: instanceID, DeploymentID: "benchmark",
					ConcurrencyKey:   concurrencyKey,
					OwnershipClaimID: ownershipClaimID,
					Status:           runs.AttemptClaimed, ClaimedAt: startedAt, HeartbeatAt: startedAt,
				}, startedAt.Add(24*time.Hour)))
				require.NoError(b, store.StartAttempt(ctx, attemptID, startedAt))
				// production audits plan.attempt_started once the attempt starts.
				require.NoError(b, store.AppendAuditEvent(ctx, runs.AuditEvent{
					ID: mustBenchmarkID(b), Repository: "benchmark/infrastructure", PullNumber: &pullNumber,
					RunID: &runID, Actor: "benchmark", EventType: "plan.attempt_started", CreatedAt: startedAt,
				}))

				errors := make(chan error, projectCount)
				var group sync.WaitGroup
				for projectIndex := 0; projectIndex < projectCount; projectIndex++ {
					group.Add(1)
					go func(index int) {
						defer group.Done()
						projectID, err := runs.NewID()
						if err != nil {
							errors <- err
							return
						}
						project := runs.ProjectRun{
							ID: projectID, RunID: runID, AttemptID: &attemptID,
							ProjectName: fmt.Sprintf("project-%03d", index),
							Directory:   fmt.Sprintf("terraform/project-%03d", index), Workspace: "default",
							Status: runs.StatusRunning, StartedAt: &startedAt,
						}
						if err := store.CreateProjectRun(ctx, project); err != nil {
							errors <- err
							return
						}
						if err := store.CompleteProjectRun(ctx, runs.ProjectRunCompletion{
							ID: projectID, Status: runs.StatusSucceeded, CompletedAt: time.Now().UTC(),
						}); err != nil {
							errors <- err
						}
					}(projectIndex)
				}
				group.Wait()
				close(errors)
				for err := range errors {
					require.NoError(b, err)
				}
				completedAt := time.Now().UTC()
				require.NoError(b, store.CompleteAttempt(ctx, runs.AttemptCompletion{
					ID: attemptID, Status: runs.AttemptSucceeded, CompletedAt: completedAt,
				}))
				require.NoError(b, store.CompleteRun(ctx, runs.RunCompletion{
					ID: runID, Status: runs.StatusSucceeded, CompletedAt: completedAt,
				}))
				// production audits plan.completed once the run finishes.
				require.NoError(b, store.AppendAuditEvent(ctx, runs.AuditEvent{
					ID: mustBenchmarkID(b), Repository: "benchmark/infrastructure", PullNumber: &pullNumber,
					RunID: &runID, Actor: "benchmark", EventType: "plan.completed", CreatedAt: completedAt,
				}))
			}
			b.ReportMetric(float64(projectCount), "projects/op")
			// 1 takeover admission + CreateRun + CreateAttempt + StartAttempt +
			// CompleteAttempt + CompleteRun + 3 mandatory audit events, plus
			// CreateProjectRun/CompleteProjectRun for every project.
			b.ReportMetric(float64(projectCount*2+9), "postgres_writes/op")
		})
	}
}

func mustBenchmarkID(b *testing.B) runs.ID {
	b.Helper()
	id, err := runs.NewID()
	require.NoError(b, err)
	return id
}
