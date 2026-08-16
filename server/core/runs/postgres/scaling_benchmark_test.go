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
				runID, err := runs.NewID()
				require.NoError(b, err)
				pullNumber := iteration + 1
				require.NoError(b, store.CreateRun(ctx, runs.Run{
					ID: runID, Repository: "benchmark/infrastructure", PullNumber: &pullNumber,
					Command: runs.CommandPlan, Trigger: runs.TriggerAPI, Actor: "benchmark",
					BaseRef: "main", HeadRef: fmt.Sprintf("benchmark-%d", iteration),
					HeadSHA: fmt.Sprintf("%040x", iteration+1), Status: runs.StatusRunning,
					CreatedAt: startedAt, StartedAt: &startedAt,
				}))
				attemptID, err := runs.NewID()
				require.NoError(b, err)
				require.NoError(b, store.CreateAttempt(ctx, runs.RunAttempt{
					ID: attemptID, RunID: runID, InstanceID: instanceID, DeploymentID: "benchmark",
					ConcurrencyKey:   fmt.Sprintf("benchmark/pull/%d", pullNumber),
					OwnershipClaimID: fmt.Sprintf("benchmark-claim-%d", iteration),
					Status:           runs.AttemptClaimed, ClaimedAt: startedAt, HeartbeatAt: startedAt,
				}))
				require.NoError(b, store.StartAttempt(ctx, attemptID, startedAt))

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
			}
			b.ReportMetric(float64(projectCount), "projects/op")
			b.ReportMetric(float64(projectCount*2+5), "postgres_writes/op")
		})
	}
}
