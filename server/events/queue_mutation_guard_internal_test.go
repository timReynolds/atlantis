// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"errors"
	"testing"

	"github.com/runatlantis/atlantis/server/core/config/valid"
	"github.com/runatlantis/atlantis/server/core/queue"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	. "github.com/runatlantis/atlantis/testing"
)

type queueGuardWorkingDir struct{ WorkingDir }

func (queueGuardWorkingDir) GitReadLock(models.Repo, models.PullRequest, string) func() {
	return func() {}
}

func TestQueueReservationBlocksEveryMutatingWorkflowStep(t *testing.T) {
	for _, step := range []valid.Step{
		{StepName: "apply"},
		{StepName: "import"},
		{StepName: "state_rm"},
		{StepName: "run", RunCommand: "terraform apply"},
		{StepName: "env", EnvVarName: "RESULT", RunCommand: "terraform apply"},
		{StepName: "multienv", RunCommand: "terraform apply"},
	} {
		t.Run(step.StepName, func(t *testing.T) {
			calls := 0
			runner := &DefaultProjectCommandRunner{
				WorkingDir: queueGuardWorkingDir{},
				MutationGuard: func(command.ProjectContext, string) (func(), error) {
					calls++
					return nil, queue.ErrReserved
				},
			}
			// A custom step may mutate even within a nominally read-only plan.
			// No step runners are installed: admission must stop before execution.
			_, err := runner.runSteps([]valid.Step{step}, command.ProjectContext{CommandName: command.Plan}, t.TempDir())
			Assert(t, errors.Is(err, queue.ErrReserved), "expected reservation to block mutation, got %v", err)
			Equals(t, 1, calls)
		})
	}
}
