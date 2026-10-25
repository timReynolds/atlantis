// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events_test

import (
	"errors"
	"regexp"
	"testing"

	. "github.com/petergtz/pegomock/v4"
	"github.com/runatlantis/atlantis/server/core/config/valid"
	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/events"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/mocks"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/events/models/testdata"
	"github.com/runatlantis/atlantis/server/logging"
	. "github.com/runatlantis/atlantis/testing"
)

func TestPlanStepsRespectUnknownAttemptFence(t *testing.T) {
	for _, tc := range []struct {
		name    string
		step    valid.Step
		blocked bool
	}{
		{"custom run", valid.Step{StepName: "run", RunCommand: "terraform apply"}, true},
		{"dynamic env", valid.Step{StepName: "env", RunCommand: "terraform apply", EnvVarName: "OUTPUT"}, true},
		{"dynamic multienv", valid.Step{StepName: "multienv", RunCommand: "terraform apply"}, true},
		{"built-in plan", valid.Step{StepName: "plan"}, false},
		{"built-in init", valid.Step{StepName: "init"}, false},
		{"static env", valid.Step{StepName: "env", EnvVarName: "NAME", EnvVarValue: "literal", RunCommand: "ignored when value is set"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			RegisterMockTestingT(t)
			custom := mocks.NewMockCustomStepRunner()
			workingDir := mocks.NewMockWorkingDir()
			locker := mocks.NewMockProjectLocker()
			marker := &countingSideEffectMarker{err: runs.ErrConflict}
			builtin := mocks.NewMockStepRunner()
			env := mocks.NewMockEnvStepRunner()
			multienv := &recordingFencedMultiEnvRunner{}
			runner := events.DefaultProjectCommandRunner{
				Locker: locker, LockURLGenerator: mockURLGenerator{}, RunStepRunner: custom,
				PlanStepRunner: builtin, InitStepRunner: builtin, EnvStepRunner: env, MultiEnvStepRunner: multienv,
				WorkingDir: workingDir, WorkingDirLocker: events.NewDefaultWorkingDirLocker(),
				CommandRequirementHandler: mocks.NewMockCommandRequirementHandler(),
			}
			dir := t.TempDir()
			When(workingDir.Clone(Any[logging.SimpleLogging](), Any[models.Repo](), Any[models.PullRequest](), AnyString())).ThenReturn(dir, nil)
			When(workingDir.GitReadLock(Any[models.Repo](), Any[models.PullRequest](), AnyString())).ThenReturn(func() {})
			When(locker.TryLock(Any[logging.SimpleLogging](), Any[models.PullRequest](), Any[models.User](), AnyString(), Any[models.Project](), AnyBool())).
				ThenReturn(&events.TryLockResponse{LockAcquired: true, LockKey: "lock", UnlockFn: func() error { return nil }}, nil)
			ctx := command.ProjectContext{
				Log: logging.NewNoopLogger(t), CommandName: command.Plan, Steps: []valid.Step{tc.step},
				Workspace: "default", RepoRelDir: ".", SideEffectMarker: marker,
			}

			if tc.step.StepName == "plan" {
				When(builtin.Run(Any[command.ProjectContext](), Any[[]string](), AnyString(), Any[map[string]string]())).Then(func([]Param) ReturnValues {
					out, err := (&planFileWritingStepRunner{contents: []byte("plan")}).Run(ctx, nil, dir, nil)
					return ReturnValues{out, err}
				})
			}

			result := runner.Plan(ctx)

			if tc.blocked {
				Assert(t, errors.Is(result.Error, runs.ErrConflict), "got %v", result.Error)
				Equals(t, 1, marker.calls)
				env.VerifyWasCalled(Never()).Run(Any[command.ProjectContext](), Any[*valid.CommandShell](), AnyString(), AnyString(), AnyString(), Any[map[string]string]())
			} else {
				Ok(t, result.Error)
				Assert(t, result.PlanSuccess != nil, "expected a read-only plan to remain available for reconciliation")
				Equals(t, 0, marker.calls)
				if tc.step.StepName == "env" {
					env.VerifyWasCalledOnce().Run(Any[command.ProjectContext](), Any[*valid.CommandShell](), AnyString(), Eq("literal"), AnyString(), Any[map[string]string]())
				} else {
					builtin.VerifyWasCalledOnce().Run(Any[command.ProjectContext](), Any[[]string](), AnyString(), Any[map[string]string]())
				}
			}
			custom.VerifyWasCalled(Never()).Run(Any[command.ProjectContext](), Any[*valid.CommandShell](), AnyString(), AnyString(), Any[map[string]string](), AnyBool(), Any[[]valid.PostProcessRunOutputOption](), Any[[]*regexp.Regexp]())
			Equals(t, 0, multienv.calls)
		})
	}
}

type recordingFencedMultiEnvRunner struct{ calls int }

func (r *recordingFencedMultiEnvRunner) Run(command.ProjectContext, *valid.CommandShell, string, string, map[string]string, []valid.PostProcessRunOutputOption) (string, error) {
	r.calls++
	return "", nil
}

func TestPlanWorkflowHooksStopAtUnknownAttemptFence(t *testing.T) {
	for _, post := range []bool{false, true} {
		name := "pre"
		if post {
			name = "post"
		}
		t.Run(name, func(t *testing.T) {
			pull := testdata.Pull
			pull.BaseRepo = testdata.GithubRepo
			marker := &countingSideEffectMarker{err: runs.ErrConflict}
			ctx := &command.Context{
				Pull: pull, HeadRepo: testdata.GithubRepo, User: testdata.User,
				Log: logging.NewNoopLogger(t), SideEffectMarker: marker,
			}
			hook := &valid.WorkflowHook{StepName: "run", RunCommand: "terraform apply"}
			cfg := valid.GlobalCfg{Repos: []valid.Repo{{
				ID: testdata.GithubRepo.ID(), PreWorkflowHooks: []*valid.WorkflowHook{hook}, PostWorkflowHooks: []*valid.WorkflowHook{hook},
			}}}
			var workingDir *mocks.MockWorkingDir
			var locker *mocks.MockWorkingDirLocker
			var run func(*command.Context, *events.CommentCommand) error
			if post {
				postWorkflowHooksSetup(t)
				postWh.GlobalCfg = cfg
				workingDir, locker, run = postWhWorkingDir, postWhWorkingDirLocker, postWh.RunPostHooks
			} else {
				preWorkflowHooksSetup(t)
				preWh.GlobalCfg = cfg
				workingDir, locker, run = preWhWorkingDir, preWhWorkingDirLocker, preWh.RunPreHooks
			}
			When(locker.TryLock(pull.BaseRepo.FullName, pull.Num, events.DefaultWorkspace, events.DefaultRepoRelDir, "", command.Plan, events.WorkingDirLockMetadataForPull(pull))).ThenReturn(func() {}, nil)
			When(workingDir.Clone(ctx.Log, ctx.HeadRepo, ctx.Pull, events.DefaultWorkspace)).ThenReturn(t.TempDir(), nil)

			err := run(ctx, &events.CommentCommand{Name: command.Plan})

			Assert(t, errors.Is(err, runs.ErrConflict), "got %v", err)
			Equals(t, 1, marker.calls)
			if post {
				whPostWorkflowHookRunner.VerifyWasCalled(Never()).Run(Any[models.WorkflowHookCommandContext](), AnyString(), AnyString(), AnyString(), AnyString())
			} else {
				whPreWorkflowHookRunner.VerifyWasCalled(Never()).Run(Any[models.WorkflowHookCommandContext](), AnyString(), AnyString(), AnyString(), AnyString())
			}
		})
	}
}
