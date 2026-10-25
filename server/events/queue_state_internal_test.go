// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/go-version"
	"github.com/runatlantis/atlantis/server/core/config/valid"
	"github.com/runatlantis/atlantis/server/core/queue"
	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/core/runtime"
	"github.com/runatlantis/atlantis/server/core/terraform"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/logging"
	. "github.com/runatlantis/atlantis/testing"
)

type queueStateTerraform struct {
	runtime.TerraformExec
	output string
	err    error
	envs   map[string]string
	onRun  func()
}

func (f *queueStateTerraform) RunCommandWithVersion(_ command.ProjectContext, _ string, _ []string, envs map[string]string, _ terraform.Distribution, _ *version.Version, _ string) (string, error) {
	if f.onRun != nil {
		f.onRun()
	}
	f.envs = envs
	return f.output, f.err
}

func TestQueueStateUsesProjectWorkspaceAndAcceptsNewBackend(t *testing.T) {
	tf := &queueStateTerraform{}
	driver := &QueueDriver{Terraform: tf}
	state, err := driver.state(command.ProjectContext{Workspace: "production"}, t.TempDir())
	Ok(t, err)
	Equals(t, queue.StateVersion{}, state)
	Equals(t, "production", tf.envs["TF_WORKSPACE"])
	Equals(t, "backend", queueStateIdentity("backend", state))
	tf.err = errors.New("backend unavailable")
	_, err = driver.state(command.ProjectContext{Workspace: "production"}, t.TempDir())
	Assert(t, errors.Is(err, tf.err), "backend failures must reject mutation admission")
}

type queueMutationRecorder struct {
	queue.Store
	states    []string
	released  bool
	err       error
	onAcquire func()
}

func (s *queueMutationRecorder) AcquireMutation(_ context.Context, states []string, _ runs.ID, _ runs.ID, _ int64, _ string) (runs.ID, error) {
	s.states = states
	if s.onAcquire != nil {
		s.onAcquire()
	}
	return "mutation", s.err
}
func (s *queueMutationRecorder) ReleaseMutation(context.Context, runs.ID) error {
	s.released = true
	return nil
}

func TestQueueMutationGuardAdmitsFirstApplyOutsideOptIn(t *testing.T) {
	root := t.TempDir()
	Ok(t, os.Mkdir(filepath.Join(root, ".terraform"), 0700))
	backend := []byte(`{"backend":{"type":"s3","config":{"bucket":"states","key":"new.tfstate"}}}`)
	Ok(t, os.WriteFile(filepath.Join(root, ".terraform", "terraform.tfstate"), backend, 0600))
	store := &queueMutationRecorder{}
	driver := &QueueDriver{Terraform: &queueStateTerraform{}, Store: store, Log: logging.NewNoopLogger(t)}
	p := command.ProjectContext{Workspace: "default"}
	release, err := driver.MutationGuard(p, root)
	Ok(t, err)
	id, err := queue.BackendStateID(backend, p.Workspace)
	Ok(t, err)
	Equals(t, []string{id}, store.states)
	release()
	Assert(t, store.released, "ordinary mutation releases its reservation")
	store.err = queue.ErrReserved
	_, err = driver.MutationGuard(p, root)
	Assert(t, errors.Is(err, queue.ErrReserved), "an empty backend must still respect queue reservations")
}

type queueWorkspaceDir struct{ WorkingDir }

func (queueWorkspaceDir) GitReadLock(models.Repo, models.PullRequest, string) func() {
	return func() {}
}

type queueWorkspaceStep struct{ workspace string }

func (s *queueWorkspaceStep) Run(_ command.ProjectContext, _ []string, _ string, envs map[string]string) (string, error) {
	s.workspace = envs["TF_WORKSPACE"]
	return "", nil
}

func TestQueueRestoredApplyPinsWorkspace(t *testing.T) {
	step := &queueWorkspaceStep{}
	runner := &DefaultProjectCommandRunner{WorkingDir: queueWorkspaceDir{}, ApplyStepRunner: step}
	p := command.ProjectContext{CommandName: command.Apply, QueueEntryID: "entry", Workspace: "production"}
	_, err := runner.runSteps([]valid.Step{{StepName: "apply"}}, p, t.TempDir())
	Ok(t, err)
	Equals(t, "production", step.workspace)
	p.QueueEntryID = ""
	_, err = runner.runSteps([]valid.Step{{StepName: "apply"}}, p, t.TempDir())
	Ok(t, err)
	Equals(t, "", step.workspace)
}

type queuePlanStatus struct{ err error }

func (s queuePlanStatus) FetchPullStatus(logging.SimpleLogging, models.PullRequest) (models.PullReqStatus, error) {
	return models.PullReqStatus{ApprovalStatus: models.ApprovalStatus{IsApproved: true}, MergeableStatus: models.MergeableStatus{IsMergeable: true}}, s.err
}

type queuePlanBuilder struct {
	ProjectCommandBuilder
	status models.PullReqStatus
	called bool
}

func (b *queuePlanBuilder) BuildPlanCommands(c *command.Context, _ *CommentCommand) ([]command.ProjectContext, error) {
	b.status, b.called = c.PullRequestStatus, true
	return nil, nil
}

func TestQueuePlanReceivesCurrentPullRequirements(t *testing.T) {
	builder := &queuePlanBuilder{}
	driver := &QueueDriver{Status: queuePlanStatus{}, Builder: builder}
	_, err := driver.planContexts(&command.Context{})
	Ok(t, err)
	Assert(t, builder.status.ApprovalStatus.IsApproved && builder.status.MergeableStatus.IsMergeable, "plan requirements must receive fetched status")
	builder.called = false
	driver.Status = queuePlanStatus{err: errors.New("status unavailable")}
	_, err = driver.planContexts(&command.Context{})
	Assert(t, err != nil && !builder.called, "status failures must block planning")
}

func TestQueueMutationGuardRechecksStateAfterAdmission(t *testing.T) {
	for _, tc := range []struct {
		name, before, after string
		readErr             error
		rejected            bool
	}{
		{name: "empty state acquires lineage before admission", after: `{"lineage":"created-by-queue","serial":1}`, rejected: true},
		{name: "existing state changes before admission", before: `{"lineage":"existing","serial":1}`, after: `{"lineage":"existing","serial":2}`, rejected: true},
		{name: "state reread fails", readErr: errors.New("backend unavailable"), rejected: true},
		{name: "unchanged empty state"},
		{name: "unchanged existing state", before: `{"lineage":"existing","serial":1}`, after: `{"lineage":"existing","serial":1}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			Ok(t, os.Mkdir(filepath.Join(root, ".terraform"), 0700))
			Ok(t, os.WriteFile(filepath.Join(root, ".terraform", "terraform.tfstate"), []byte(`{"backend":{"type":"s3","config":{"bucket":"states","key":"alias.tfstate"}}}`), 0600))
			tf := &queueStateTerraform{output: tc.before}
			store := &queueMutationRecorder{}
			reads := 0
			tf.onRun = func() {
				reads++
				if reads > 1 {
					Assert(t, store.states != nil && !store.released, "reread must hold mutation reservation")
				}
			}
			// Model a queue first apply completing after the guard's initial state
			// read but before this alias's admission transaction starts.
			store.onAcquire = func() { tf.output = tc.after; tf.err = tc.readErr }
			driver := &QueueDriver{Terraform: tf, Store: store, Log: logging.NewNoopLogger(t)}
			release, err := driver.MutationGuard(command.ProjectContext{Workspace: "default"}, root)
			Equals(t, 2, reads)
			if tc.rejected {
				Assert(t, err != nil && release == nil, "changed or unreadable state must reject mutation")
				Assert(t, store.released, "rejected admission must release ordinary token")
				if tc.readErr != nil {
					Assert(t, errors.Is(err, tc.readErr), "preserve read failure")
				} else {
					Assert(t, errors.Is(err, queue.ErrReplan), "state change requires fresh admission")
				}
			} else {
				Ok(t, err)
				Assert(t, !store.released, "successful admission keeps token until mutation ends")
				release()
				Assert(t, store.released, "completion releases token")
			}
		})
	}
}
