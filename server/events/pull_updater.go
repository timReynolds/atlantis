// Copyright 2025 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"slices"

	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/vcs"
)

// RunHistoryURLGenerator constructs the authenticated detail URL for a Run.
type RunHistoryURLGenerator interface {
	GenerateRunHistoryURL(runID runs.ID) (string, error)
}

type PullUpdater struct {
	HidePrevPlanComments     bool
	VCSClient                vcs.Client
	MarkdownRenderer         *MarkdownRenderer
	RunHistoryURLGenerator   RunHistoryURLGenerator
	LargeRunSummaryThreshold int
}

func (c *PullUpdater) updatePull(ctx *command.Context, cmd PullCommand, res command.Result) {
	// Log if we got any errors or failures.
	if res.Error != nil {
		ctx.CommandHasErrors = true
		ctx.Log.Err("%s", res.Error.Error())
	} else if res.Failure != "" {
		ctx.CommandHasErrors = true
		ctx.Log.Warn("%s", res.Failure)
	}

	// HidePrevCommandComments will hide old comments left from previous runs to reduce
	// clutter in a pull/merge request. This will not delete the comment, since the
	// comment trail may be useful in auditing or backtracing problems.
	if c.HidePrevPlanComments {
		ctx.Log.Debug("hiding previous plan comments for command: '%v', directory: '%v'", cmd.CommandName().TitleString(), cmd.Dir())
		if err := c.VCSClient.HidePrevCommandComments(ctx.Log, ctx.Pull.BaseRepo, ctx.Pull.Num, cmd.CommandName().TitleString(), cmd.Dir()); err != nil {
			ctx.Log.Err("unable to hide old comments: %s", err)
		}
	}

	if len(res.ProjectResults) > 0 {
		var commentOnProjects []command.ProjectResult
		for _, result := range res.ProjectResults {
			if slices.Contains(result.SilencePRComments, cmd.CommandName().String()) {
				ctx.Log.Debug("silenced command '%s' comment for project '%s'", cmd.CommandName().String(), result.ProjectName)
				continue
			}
			commentOnProjects = append(commentOnProjects, result)
		}

		if len(commentOnProjects) == 0 {
			return
		}

		res.ProjectResults = commentOnProjects
	}

	var comment string
	if c.shouldUseLargeRunSummary(ctx, res, cmd) {
		historyURL, err := c.RunHistoryURLGenerator.GenerateRunHistoryURL(ctx.RunID)
		if err != nil {
			ctx.Log.Err("generating run history URL: %v", err)
		} else {
			comment = c.MarkdownRenderer.RenderRunSummary(ctx, res, cmd, historyURL)
		}
	}
	if comment == "" {
		comment = c.MarkdownRenderer.Render(ctx, res, cmd)
	}
	if err := c.VCSClient.CreateComment(ctx.Log, ctx.Pull.BaseRepo, ctx.Pull.Num, comment, cmd.CommandName().String()); err != nil {
		ctx.Log.Err("unable to comment: %s", err)
	}
}

func (c *PullUpdater) shouldUseLargeRunSummary(ctx *command.Context, result command.Result, cmd PullCommand) bool {
	return c.LargeRunSummaryThreshold > 0 &&
		c.RunHistoryURLGenerator != nil &&
		ctx.RunID != "" &&
		result.Error == nil && result.Failure == "" &&
		len(result.ProjectResults) >= c.LargeRunSummaryThreshold &&
		!cmd.IsVerbose()
}
