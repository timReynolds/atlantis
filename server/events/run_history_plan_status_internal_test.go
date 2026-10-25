// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"errors"
	"fmt"
	"testing"

	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/events/vcs"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
)

type historyPlanStatusClient struct {
	vcs.Client
	statuses []models.CommitStatus
}

func (c *historyPlanStatusClient) UpdateStatus(_ logging.SimpleLogging, _ models.Repo, _ models.PullRequest, status models.CommitStatus, _, _, _ string) error {
	c.statuses = append(c.statuses, status)
	return nil
}

type historySuccessfulPlanRunner struct{ ProjectCommandRunner }

func (historySuccessfulPlanRunner) Plan(command.ProjectContext) command.ProjectCommandOutput {
	return command.ProjectCommandOutput{PlanSuccess: &models.PlanSuccess{}}
}

type historyPlanJobStatus struct{ updater DefaultCommitStatusUpdater }

func (s historyPlanJobStatus) SetJobURLWithStatus(ctx command.ProjectContext, name command.Name, status models.CommitStatus, output *command.ProjectCommandOutput) error {
	return s.updater.UpdateProject(ctx, name, status, "", output)
}

func TestRunHistoryForwardsDeferredPlanStatusesThroughProductionDecorators(t *testing.T) {
	for _, status := range []models.CommitStatus{models.SuccessCommitStatus, models.FailedCommitStatus} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			writer := &recordingRunWriter{}
			history := newTestRunHistory(t, writer)
			ctx := testRunContext(t)
			life := history.Begin(ctx, runs.CommandPlan, runs.TriggerComment)
			defer life.Finish()
			client := &historyPlanStatusClient{}
			output := &ProjectOutputWrapper{
				ProjectCommandRunner: historySuccessfulPlanRunner{},
				JobURLSetter:         historyPlanJobStatus{DefaultCommitStatusUpdater{Client: client, StatusName: "atlantis"}},
			}
			// Match server wiring: metrics -> run history -> project output.
			runner := NewInstrumentedProjectCommandRunner(tally.NoopScope, NewRunHistoryProjectCommandRunner(history, output))
			project := command.ProjectContext{RunID: ctx.RunID, Log: ctx.Log, CommandName: command.Plan,
				ProjectName: "network", RepoRelDir: ".", Workspace: "default", SuppressJobOutput: true}
			result := runner.Plan(project)
			require.NotNil(t, result.PlanSuccess)
			require.Equal(t, []models.CommitStatus{models.PendingCommitStatus}, client.statuses,
				"project success must remain deferred until the orchestrator persists results")
			commandResult := command.Result{ProjectResults: []command.ProjectResult{{
				Command: command.Plan, ProjectName: project.ProjectName, RepoRelDir: project.RepoRelDir,
				Workspace: project.Workspace, ProjectCommandOutput: result,
			}}}
			if status == models.FailedCommitStatus {
				commandResult.Error = errors.New("persisting plan results")
			}
			orchestrator := &PlanCommandRunner{prjCmdRunner: runner}
			orchestrator.publishPlanStatuses([]command.ProjectContext{project}, commandResult, status)
			require.Equal(t, []models.CommitStatus{models.PendingCommitStatus, status}, client.statuses)
			project.SuppressVCSStatus = true
			orchestrator.publishPlanStatuses([]command.ProjectContext{project}, commandResult, status)
			require.Len(t, client.statuses, 2, "suppressed contexts must remain suppressed through decorators")
		})
	}
}
