// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	gh "github.com/google/go-github/v88/github"
	"github.com/runatlantis/atlantis/server/core/boltdb"
	"github.com/runatlantis/atlantis/server/core/config/valid"
	"github.com/runatlantis/atlantis/server/core/queue"
	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/events/vcs"
	"github.com/runatlantis/atlantis/server/logging"
	. "github.com/runatlantis/atlantis/testing"
	"github.com/uber-go/tally/v4"
)

type queueReportingStore struct {
	queue.Store
	entry        queue.Entry
	currentID    runs.ID
	getErr       error
	findErr      error
	commentErr   error
	acknowledged []int64
	pendingReads int
}

func (s *queueReportingStore) Get(context.Context, runs.ID) (queue.Entry, error) {
	return s.entry, s.getErr
}
func (s *queueReportingStore) Find(context.Context, string, int) (queue.Entry, error) {
	entry := s.entry
	entry.ID = s.currentID
	return entry, s.findErr
}
func (s *queueReportingStore) SetComment(_ context.Context, _ runs.ID, id int64) error {
	if s.commentErr == nil {
		s.entry.CommentID = id
	}
	return s.commentErr
}
func (s *queueReportingStore) MarkReported(_ context.Context, _ runs.ID, version int64) error {
	s.acknowledged = append(s.acknowledged, version)
	s.entry.ReportedVersion = version
	return nil
}
func (s *queueReportingStore) PendingReports(context.Context, int) ([]queue.Entry, error) {
	s.pendingReads++
	if s.entry.ReportedVersion < s.entry.Version {
		return []queue.Entry{s.entry}, nil
	}
	return nil, nil
}
func (s *queueReportingStore) List(context.Context, int) ([]queue.Entry, error) {
	// Terminal entries have left the active scheduling list.
	return nil, nil
}
func (s *queueReportingStore) CheckClaim(context.Context, queue.Entry) error { return nil }

type queueReportingGithub struct {
	*queueMetadataGithub
	commentErr error
	comments   int
}

func (g *queueReportingGithub) QueueComment(context.Context, models.Repo, int, int64, string, string) (int64, error) {
	g.comments++
	return 12, g.commentErr
}

type queueReportingVCS struct {
	vcs.Client
	statusErr error
	statuses  []models.CommitStatus
	calls     []string
	onMerge   func()
}

func (v *queueReportingVCS) UpdateStatus(_ logging.SimpleLogging, _ models.Repo, _ models.PullRequest, status models.CommitStatus, _, _, _ string) error {
	v.statuses = append(v.statuses, status)
	v.calls = append(v.calls, "status")
	return v.statusErr
}
func (v *queueReportingVCS) MergePull(logging.SimpleLogging, models.PullRequest, models.PullRequestOptions) error {
	v.calls = append(v.calls, "merge")
	v.onMerge()
	return nil
}

func newQueueReportingDriver(t *testing.T) (*QueueDriver, *queueReportingStore, *queueReportingGithub, *queueReportingVCS) {
	t.Helper()
	parser := &EventParser{GithubUser: "bot", GithubToken: "test"}
	repo, err := parser.ParseGithubRepo(&gh.Repository{FullName: gh.Ptr("o/r"), CloneURL: gh.Ptr("https://github.com/o/r.git")})
	Ok(t, err)
	ref, err := NewRepoRef(repo)
	Ok(t, err)
	source, err := json.Marshal(ref)
	Ok(t, err)
	entry := queue.Entry{ID: "entry", Repository: repo.ID(), PullNumber: 1, HeadSHA: "head", TargetBranch: "main", Actor: "operator", Source: source, State: queue.Completed, Version: 7, CommentID: 12}
	store := &queueReportingStore{entry: entry, currentID: entry.ID}
	github := &queueReportingGithub{}
	vcsClient := &queueReportingVCS{}
	driver := &QueueDriver{Store: store, Github: github, VCS: vcsClient, Parser: parser, Log: logging.NewNoopLogger(t), Scope: tally.NoopScope, URL: "https://atlantis.example", StatusName: "atlantis", Global: valid.GlobalCfg{Queue: queue.Config{Enabled: true, Repositories: []string{repo.ID()}}}}
	return driver, store, github, vcsClient
}

func TestQueueReportFailureDoesNotAcknowledgeRevision(t *testing.T) {
	failure := errors.New("unavailable")
	for _, stage := range []string{"read", "comment", "comment persistence", "latest lifecycle", "status"} {
		t.Run(stage, func(t *testing.T) {
			driver, store, github, vcsClient := newQueueReportingDriver(t)
			switch stage {
			case "read":
				store.getErr = failure
			case "comment":
				github.commentErr = failure
			case "comment persistence":
				store.entry.CommentID = 0
				store.commentErr = failure
			case "latest lifecycle":
				store.findErr = failure
			case "status":
				vcsClient.statusErr = failure
			}
			driver.Report(context.Background(), store.entry)
			Equals(t, 0, len(store.acknowledged))
			Equals(t, int64(0), store.entry.ReportedVersion)
		})
	}
}

func TestQueueRefreshRetriesTerminalReport(t *testing.T) {
	driver, store, github, vcsClient := newQueueReportingDriver(t)
	vcsClient.statusErr = errors.New("status unavailable")
	driver.Report(context.Background(), store.entry)
	Equals(t, 0, len(store.acknowledged))
	vcsClient.statusErr = nil
	service := &QueueService{Driver: driver}
	Ok(t, service.refresh(context.Background()))
	Equals(t, []int64{store.entry.Version}, store.acknowledged)
	Equals(t, 2, github.comments)
	Equals(t, []models.CommitStatus{models.SuccessCommitStatus, models.SuccessCommitStatus}, vcsClient.statuses)
	Ok(t, service.refresh(context.Background()))
	Equals(t, 2, github.comments)
	Equals(t, 2, store.pendingReads)
}

func TestQueueReportOldLifecycleDoesNotOverwriteCurrentStatus(t *testing.T) {
	driver, store, github, vcsClient := newQueueReportingDriver(t)
	store.currentID = "new-lifecycle"
	driver.Report(context.Background(), store.entry)
	Equals(t, 1, github.comments)
	Equals(t, 0, len(vcsClient.statuses))
	Equals(t, []int64{store.entry.Version}, store.acknowledged)
}

func TestQueueMergingStatusRequiresAllPlansApplied(t *testing.T) {
	for _, tc := range []struct {
		name    string
		started bool
		plans   []queue.Plan
		status  models.CommitStatus
	}{
		{name: "not started", plans: []queue.Plan{{Applied: true}}, status: models.PendingCommitStatus},
		{name: "no plans", started: true, status: models.PendingCommitStatus},
		{name: "partial", started: true, plans: []queue.Plan{{Applied: true}, {}}, status: models.PendingCommitStatus},
		{name: "complete", started: true, plans: []queue.Plan{{Applied: true}, {Applied: true}}, status: models.SuccessCommitStatus},
	} {
		t.Run(tc.name, func(t *testing.T) {
			Equals(t, tc.status, queueCommitStatus(queue.Entry{State: queue.Merging, ApplyStarted: tc.started, Plans: tc.plans}))
		})
	}
}

type queueReportingWorkingDir struct {
	WorkingDir
	path string
}

func (w queueReportingWorkingDir) GetWorkingDir(models.Repo, models.PullRequest, string) (string, error) {
	return w.path, nil
}

type queueReportingBuilder struct {
	ProjectCommandBuilder
	project command.ProjectContext
}

func (b queueReportingBuilder) BuildApplyCommands(*command.Context, *CommentCommand) ([]command.ProjectContext, error) {
	return []command.ProjectContext{b.project}, nil
}

func TestQueueMergePublishesSuccessBeforeMerge(t *testing.T) {
	for _, statusFails := range []bool{false, true} {
		name := "success"
		if statusFails {
			name = "status failure"
		}
		t.Run(name, func(t *testing.T) {
			driver, store, github, vcsClient := newQueueReportingDriver(t)
			root := t.TempDir()
			for _, args := range [][]string{{"init"}, {"-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "--allow-empty", "-m", "fixture"}} {
				cmd := exec.Command("git", args...)
				cmd.Dir = root
				output, err := cmd.CombinedOutput()
				Assert(t, err == nil, "creating checkout: %v %s", err, output)
			}
			tree, err := queueGit(context.Background(), root, "rev-parse", "HEAD^{tree}")
			Ok(t, err)
			Ok(t, os.Mkdir(filepath.Join(root, ".terraform"), 0700))
			backend := []byte(`{"backend":{"type":"s3","config":{"bucket":"states","key":"terraform.tfstate","use_lockfile":true}}}`)
			Ok(t, os.WriteFile(filepath.Join(root, ".terraform", "terraform.tfstate"), backend, 0600))
			project := command.ProjectContext{RepoRelDir: ".", Workspace: "default", CommandName: command.Apply, Steps: []valid.Step{{StepName: "apply"}}}
			backendID, err := queue.BackendStateID(backend, project.Workspace)
			Ok(t, err)
			inputs, err := driver.inputFingerprint(project, root)
			Ok(t, err)
			driver.WorkingDir = queueReportingWorkingDir{path: root}
			driver.Builder = queueReportingBuilder{project: project}
			driver.Terraform = &queueStateTerraform{}
			driver.Status = queuePlanStatus{}
			database, err := boltdb.New(t.TempDir())
			Ok(t, err)
			t.Cleanup(func() { Ok(t, database.Close()) })
			driver.DB = &DBUpdater{Database: database}
			repo := &gh.Repository{FullName: gh.Ptr("o/r"), CloneURL: gh.Ptr("https://github.com/o/r.git")}
			raw := &gh.PullRequest{Number: gh.Ptr(1), HTMLURL: gh.Ptr("https://github.com/o/r/pull/1"), User: &gh.User{Login: gh.Ptr("author")}, State: gh.Ptr("open"), Head: &gh.PullRequestBranch{SHA: gh.Ptr("head"), Ref: gh.Ptr("feature"), Repo: repo}, Base: &gh.PullRequestBranch{SHA: gh.Ptr("base"), Ref: gh.Ptr("main"), Repo: repo}}
			github.queueMetadataGithub = &queueMetadataGithub{pull: raw, tree: tree}
			vcsClient.onMerge = func() {
				raw.Merged = gh.Ptr(true)
				raw.State = gh.Ptr("closed")
				raw.MergeCommitSHA = gh.Ptr("merged")
			}
			entry := store.entry
			entry.State, entry.ApplyStarted = queue.Merging, true
			entry.Prepared = queue.PreparedPlan{BaseSHA: "base", TreeSHA: tree, Configuration: queueHash(driver.Global)}
			entry.Plans = []queue.Plan{{Project: queue.Project{Directory: ".", Workspace: "default", BackendID: backendID, Inputs: inputs}, Applied: true, ResultState: &queue.StateVersion{}}}
			statusErr := errors.New("status unavailable")
			if statusFails {
				vcsClient.statusErr = statusErr
			}
			result, err := driver.Merge(context.Background(), entry)
			if statusFails {
				Assert(t, errors.Is(err, statusErr), "status failure must prevent merge: %v", err)
				Equals(t, []string{"status"}, vcsClient.calls)
				Assert(t, !raw.GetMerged(), "a failed status write must leave the pull unmerged")
			} else {
				Ok(t, err)
				Equals(t, []string{"status", "merge"}, vcsClient.calls)
				Equals(t, queue.MergeResult{SHA: "merged", Equivalent: true}, result)
			}
			Equals(t, []models.CommitStatus{models.SuccessCommitStatus}, vcsClient.statuses)
		})
	}
}
