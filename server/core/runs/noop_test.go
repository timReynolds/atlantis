// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package runs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
	. "github.com/runatlantis/atlantis/testing"
)

func TestNoopStorePreservesDisabledBehavior(t *testing.T) {
	store := runs.NoopStore{}
	ctx := context.Background()

	Ok(t, store.CreateRun(ctx, runs.Run{}))
	Ok(t, store.StartRun(ctx, "", time.Time{}))
	Ok(t, store.CompleteRun(ctx, runs.RunCompletion{}))
	Ok(t, store.CreateProjectRun(ctx, runs.ProjectRun{}))
	Ok(t, store.StartProjectRun(ctx, "", time.Time{}))
	Ok(t, store.CompleteProjectRun(ctx, runs.ProjectRunCompletion{}))
	Ok(t, store.AppendOutput(ctx, []runs.OutputChunk{{}}))
	Ok(t, store.AppendAuditEvent(ctx, runs.AuditEvent{}))

	_, err := store.GetRun(ctx, "missing")
	Assert(t, errors.Is(err, runs.ErrNotFound), "missing run should return ErrNotFound")
	runPage, err := store.ListRuns(ctx, runs.RunFilter{}, runs.PageRequest{})
	Ok(t, err)
	Equals(t, 0, len(runPage.Runs))

	_, err = store.GetProjectRun(ctx, "missing")
	Assert(t, errors.Is(err, runs.ErrNotFound), "missing project run should return ErrNotFound")
	projectPage, err := store.ListProjectRuns(ctx, "missing", runs.ProjectRunFilter{}, runs.PageRequest{})
	Ok(t, err)
	Equals(t, 0, len(projectPage.ProjectRuns))
	summary, err := store.SummarizeProjectRuns(ctx, "missing")
	Ok(t, err)
	Equals(t, runs.ProjectRunSummary{}, summary)

	outputPage, err := store.GetOutput(ctx, "missing", 0, 100)
	Ok(t, err)
	Equals(t, 0, len(outputPage.Chunks))
	auditPage, err := store.ListAuditEvents(ctx, runs.AuditFilter{}, runs.PageRequest{})
	Ok(t, err)
	Equals(t, 0, len(auditPage.Events))

	retention, err := store.ApplyRetention(ctx, runs.RetentionPolicy{})
	Ok(t, err)
	Equals(t, runs.RetentionResult{}, retention)
}
