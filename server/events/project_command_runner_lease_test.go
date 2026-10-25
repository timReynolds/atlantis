// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events_test

import (
	"context"
	"errors"
	"regexp"
	"testing"

	. "github.com/petergtz/pegomock/v4"
	"github.com/runatlantis/atlantis/server/core/config/valid"
	"github.com/runatlantis/atlantis/server/events"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/mocks"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/logging"
	. "github.com/runatlantis/atlantis/testing"
)

func TestDefaultProjectCommandRunner_ApplyRechecksLeaseBetweenSteps(t *testing.T) {
	for _, routingEnabled := range []bool{true, false} {
		name := "routing disabled"
		if routingEnabled {
			name = "ownership lost during custom run"
		}
		t.Run(name, func(t *testing.T) {
			RegisterMockTestingT(t)
			workingDir := mocks.NewMockWorkingDir()
			locker := mocks.NewMockProjectLocker()
			custom := mocks.NewMockCustomStepRunner()
			apply := mocks.NewMockStepRunner()
			lost := false
			runner := &events.DefaultProjectCommandRunner{
				Locker: locker, LockURLGenerator: mockURLGenerator{},
				ApplyStepRunner: apply, RunStepRunner: custom,
				WorkingDir: workingDir, WorkingDirLocker: events.NewDefaultWorkingDirLocker(),
				CommandRequirementHandler: &events.DefaultCommandRequirementHandler{WorkingDir: workingDir},
			}
			dir := t.TempDir()
			ctx := command.ProjectContext{
				Log: logging.NewNoopLogger(t), CommandName: command.Apply,
				Steps:     []valid.Step{{StepName: "run"}, {StepName: "apply"}},
				Workspace: "default", RepoRelDir: ".",
				Pull: models.PullRequest{Num: 1, BaseRepo: models.Repo{FullName: "org/repo"}},
			}
			if routingEnabled {
				ctx.ExecutionLease = executionLeaseFunc(func(context.Context) error {
					if lost {
						return events.ErrOwnershipChanged
					}
					return nil
				})
			}
			When(workingDir.GetWorkingDir(ctx.Pull.BaseRepo, ctx.Pull, ctx.Workspace)).ThenReturn(dir, nil)
			When(workingDir.GitReadLock(ctx.Pull.BaseRepo, ctx.Pull, ctx.Workspace)).ThenReturn(func() {})
			When(locker.TryLock(Any[logging.SimpleLogging](), Eq(ctx.Pull), Any[models.User](), Eq(ctx.Workspace), Any[models.Project](), AnyBool())).
				ThenReturn(&events.TryLockResponse{LockAcquired: true, LockKey: "lock"}, nil)
			When(custom.Run(Any[command.ProjectContext](), Any[*valid.CommandShell](), AnyString(), AnyString(), Any[map[string]string](), AnyBool(), Any[[]valid.PostProcessRunOutputOption](), Any[[]*regexp.Regexp]())).
				Then(func([]Param) ReturnValues {
					lost = true
					return ReturnValues{"run", nil}
				})

			result := runner.Apply(ctx)

			Assert(t, lost, "expected the custom run step to finish")
			if routingEnabled {
				Assert(t, errors.Is(result.Error, events.ErrOwnershipChanged), "got %v", result.Error)
				Assert(t, result.OwnershipLost, "expected publication suppression after lease loss")
				apply.VerifyWasCalled(Never()).Run(Any[command.ProjectContext](), Any[[]string](), AnyString(), Any[map[string]string]())
			} else {
				Ok(t, result.Error)
				apply.VerifyWasCalledOnce().Run(Any[command.ProjectContext](), Any[[]string](), AnyString(), Any[map[string]string]())
			}
		})
	}
}
