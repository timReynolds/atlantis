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
		RunHistoryCompleteness:   staticRunHistoryCompleteness{complete: true},
		LargeRunSummaryThreshold: 50,
	}
	ctx := &command.Context{RunID: runID}
	result := command.Result{ProjectResults: make([]command.ProjectResult, 50)}
	cmd := &CommentCommand{Name: command.Plan}

	require.True(t, updater.shouldUseLargeRunSummary(ctx, result, cmd))
	result.ProjectResults = result.ProjectResults[:49]
	require.False(t, updater.shouldUseLargeRunSummary(ctx, result, cmd))
	result.ProjectResults = make([]command.ProjectResult, 50)
	cmd.Verbose = true
	require.False(t, updater.shouldUseLargeRunSummary(ctx, result, cmd))
	cmd.Verbose = false
	result.Error = errors.New("command failed")
	require.False(t, updater.shouldUseLargeRunSummary(ctx, result, cmd))
	result.Error = nil
	updater.RunHistoryCompleteness = staticRunHistoryCompleteness{complete: false}
	require.False(t, updater.shouldUseLargeRunSummary(ctx, result, cmd))
	updater.RunHistoryCompleteness = staticRunHistoryCompleteness{complete: true}
	ctx.RunID = ""
	require.False(t, updater.shouldUseLargeRunSummary(ctx, result, cmd))
}

func TestPullUpdaterPostsOneBoundedLargeRunComment(t *testing.T) {
	RegisterMockTestingT(t)
	client := vcsmocks.NewMockClient()
	When(client.CreateComment(Any[logging.SimpleLogging](), Any[models.Repo](), Any[int](), Any[string](), Any[string]())).ThenReturn(nil)
	renderer := NewMarkdownRenderer(false, false, false, false, false, false, "", "atlantis", false, false)
	updater := &PullUpdater{
		VCSClient: client, MarkdownRenderer: renderer,
		RunHistoryURLGenerator: staticRunHistoryURLGenerator{},
		RunHistoryCompleteness: staticRunHistoryCompleteness{complete: true}, LargeRunSummaryThreshold: 50,
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

type staticRunHistoryURLGenerator struct{}

func (staticRunHistoryURLGenerator) GenerateRunHistoryURL(runs.ID) (string, error) {
	return "https://atlantis.example.test/runs/id", nil
}

var _ RunHistoryURLGenerator = staticRunHistoryURLGenerator{}

type staticRunHistoryCompleteness struct{ complete bool }

func (s staticRunHistoryCompleteness) IsRunHistoryComplete(runs.ID) bool { return s.complete }

var _ RunHistoryCompletenessChecker = staticRunHistoryCompleteness{}
