// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"errors"
	"strings"
	"testing"

	. "github.com/petergtz/pegomock/v4"
	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	vcsmocks "github.com/runatlantis/atlantis/server/events/vcs/mocks"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/require"
)

func TestPullUpdaterUsesLargeRunSummaryOnlyWhenDurableUIIsAvailable(t *testing.T) {
	runID, err := runs.NewID()
	require.NoError(t, err)
	updater := &PullUpdater{
		RunHistoryURLGenerator:   staticRunHistoryURLGenerator{},
		RunHistoryFinalizer:      &staticRunHistoryFinalizer{},
		LargeRunSummaryThreshold: 50,
	}
	ctx := &command.Context{RunID: runID}
	result := command.Result{ProjectResults: make([]command.ProjectResult, 50)}
	cmd := &CommentCommand{Name: command.Plan}

	require.True(t, updater.shouldUseLargeRunSummary(ctx, result, cmd))
	result.ProjectResults = result.ProjectResults[:49]
	require.False(t, updater.shouldUseLargeRunSummary(ctx, result, cmd))
	result.ProjectResults = make([]command.ProjectResult, 50)
	result.ProjectResults[0].StateRmSuccess = &models.StateRmSuccess{RePlanCmd: "atlantis plan -p network"}
	require.False(t, updater.shouldUseLargeRunSummary(ctx, result, cmd))
	result.ProjectResults[0].StateRmSuccess = nil
	result.ProjectResults[0].ImportSuccess = &models.ImportSuccess{RePlanCmd: "atlantis plan -p network"}
	require.False(t, updater.shouldUseLargeRunSummary(ctx, result, cmd))
	result.ProjectResults[0].ImportSuccess = nil
	cmd.Verbose = true
	require.False(t, updater.shouldUseLargeRunSummary(ctx, result, cmd))
	cmd.Verbose = false
	result.Error = errors.New("command failed")
	require.False(t, updater.shouldUseLargeRunSummary(ctx, result, cmd))
	result.Error = nil
	updater.RunHistoryFinalizer = nil
	require.False(t, updater.shouldUseLargeRunSummary(ctx, result, cmd))
	updater.RunHistoryFinalizer = &staticRunHistoryFinalizer{}
	ctx.RunID = ""
	require.False(t, updater.shouldUseLargeRunSummary(ctx, result, cmd))
}

func TestPullUpdaterPostsOneBoundedLargeRunComment(t *testing.T) {
	RegisterMockTestingT(t)
	client := vcsmocks.NewMockClient()
	When(client.CreateComment(Any[logging.SimpleLogging](), Any[models.Repo](), Any[int](), Any[string](), Any[string]())).ThenReturn(nil)
	renderer := NewMarkdownRenderer(false, false, false, false, false, false, "", "atlantis", false, false)
	finalizer := &staticRunHistoryFinalizer{}
	updater := &PullUpdater{
		VCSClient: client, MarkdownRenderer: renderer,
		RunHistoryURLGenerator: staticRunHistoryURLGenerator{},
		RunHistoryFinalizer:    finalizer, LargeRunSummaryThreshold: 50,
	}
	runID, err := runs.NewID()
	require.NoError(t, err)
	ctx := &command.Context{
		RunID: runID, Log: logging.NewNoopLogger(t).WithHistory(),
		Pull: models.PullRequest{Num: 42, BaseRepo: models.Repo{
			Owner: "org", Name: "repo", FullName: "org/repo",
			VCSHost: models.VCSHost{Type: models.Github},
		}},
	}
	results := make([]command.ProjectResult, 50)
	for index := range results {
		results[index] = command.ProjectResult{
			RepoRelDir: "terraform/project", Workspace: "default",
			ProjectCommandOutput: command.ProjectCommandOutput{
				PlanSuccess: &models.PlanSuccess{TerraformOutput: "sensitive project output\nPlan: 1 to add, 0 to change, 0 to destroy."},
			},
		}
	}

	updater.updatePull(ctx, &CommentCommand{Name: command.Plan}, command.Result{ProjectResults: results})
	client.VerifyWasCalled(Never()).CreateComment(
		Any[logging.SimpleLogging](), Any[models.Repo](), Any[int](), Any[string](), Any[string]())
	finalizer.finish(true)

	_, _, _, comment, commandName := client.VerifyWasCalledOnce().CreateComment(
		Any[logging.SimpleLogging](), Eq(ctx.Pull.BaseRepo), Eq(42), AnyString(), Eq("plan"),
	).GetCapturedArguments()
	require.Equal(t, "plan", commandName)
	require.Contains(t, comment, "Ran Plan for 50 projects.")
	require.Contains(t, comment, "https://atlantis.example.test/runs/id")
	require.NotContains(t, comment, "sensitive project output")
	require.Less(t, len(comment), 1500)
	require.Equal(t, 1, strings.Count(comment, "View full project results"))
}

func TestPullUpdaterRetainsFullOutputWhenFinalHistoryPersistenceFails(t *testing.T) {
	RegisterMockTestingT(t)
	client := vcsmocks.NewMockClient()
	When(client.CreateComment(Any[logging.SimpleLogging](), Any[models.Repo](), Any[int](), Any[string](), Any[string]())).ThenReturn(nil)
	finalizer := &staticRunHistoryFinalizer{}
	updater := &PullUpdater{
		VCSClient:              client,
		MarkdownRenderer:       NewMarkdownRenderer(false, false, false, false, false, false, "", "atlantis", false, false),
		RunHistoryURLGenerator: staticRunHistoryURLGenerator{},
		RunHistoryFinalizer:    finalizer, LargeRunSummaryThreshold: 1,
	}
	runID, err := runs.NewID()
	require.NoError(t, err)
	ctx := &command.Context{RunID: runID, Log: logging.NewNoopLogger(t).WithHistory(), Pull: models.PullRequest{
		Num: 42, BaseRepo: models.Repo{Owner: "org", Name: "repo", FullName: "org/repo"},
	}}
	result := command.Result{ProjectResults: []command.ProjectResult{{
		RepoRelDir: "terraform/project", Workspace: "default",
		ProjectCommandOutput: command.ProjectCommandOutput{
			PlanSuccess: &models.PlanSuccess{TerraformOutput: "only durable copy\nPlan: 1 to add, 0 to change, 0 to destroy."},
		},
	}}}

	updater.updatePull(ctx, &CommentCommand{Name: command.Plan}, result)
	finalizer.finish(false)

	_, _, _, comment, _ := client.VerifyWasCalledOnce().CreateComment(
		Any[logging.SimpleLogging](), Eq(ctx.Pull.BaseRepo), Eq(42), AnyString(), Eq("plan"),
	).GetCapturedArguments()
	require.Contains(t, comment, "only durable copy")
	require.NotContains(t, comment, "View full project results")
}

type staticRunHistoryURLGenerator struct{}

func (staticRunHistoryURLGenerator) GenerateRunHistoryURL(runs.ID) (string, error) {
	return "https://atlantis.example.test/runs/id", nil
}

var _ RunHistoryURLGenerator = staticRunHistoryURLGenerator{}

type staticRunHistoryFinalizer struct {
	callback func(bool)
}

func (s *staticRunHistoryFinalizer) DeferRunComment(_ runs.ID, callback func(bool)) bool {
	s.callback = callback
	return true
}

func (s *staticRunHistoryFinalizer) finish(complete bool) {
	s.callback(complete)
}

var _ RunHistoryCommentFinalizer = (*staticRunHistoryFinalizer)(nil)
