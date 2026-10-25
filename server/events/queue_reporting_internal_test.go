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
	"slices"
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
	statusErr        error
	statusContextErr string
	contexts         []string
	urls             []string
	statuses         []models.CommitStatus
	calls            []string
	onMerge          func()
}

func (v *queueReportingVCS) UpdateStatus(_ logging.SimpleLogging, _ models.Repo, _ models.PullRequest, status models.CommitStatus, statusContext, _, url string) error {
	v.urls = append(v.urls, url)
	v.contexts = append(v.contexts, statusContext)
	v.statuses = append(v.statuses, status)
	v.calls = append(v.calls, "status")
	if v.statusContextErr == "" || v.statusContextErr == statusContext {
		return v.statusErr
	}
	return nil
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
	store.entry.ApplyStarted = true
	store.entry.Plans = []queue.Plan{{Project: queue.Project{Name: "production"}, Applied: true, ResultState: &queue.StateVersion{}}}
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

type queueSuccessfulApplyRunner struct{ ProjectCommandRunner }

func (queueSuccessfulApplyRunner) Apply(command.ProjectContext) command.ProjectCommandOutput {
	return command.ProjectCommandOutput{ApplySuccess: "Apply complete!"}
}

type queueApplyJobStatus struct{ updater DefaultCommitStatusUpdater }

func (s queueApplyJobStatus) SetJobURLWithStatus(p command.ProjectContext, name command.Name, status models.CommitStatus, result *command.ProjectCommandOutput) error {
	return s.updater.UpdateProject(p, name, status, "", result)
}

func (*queueReportingStore) LinkRun(context.Context, queue.Entry, runs.ID, runs.ID) error { return nil }

func TestQueueMergePublishesSuccessBeforeMerge(t *testing.T) {
	for _, failureContext := range []string{"", "atlantis/apply: ./default", "atlantis/apply", "atlantis/queue"} {
		statusFails := failureContext != ""
		name := "success"
		if statusFails {
			name = failureContext
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
			project := command.ProjectContext{Log: driver.Log, SuppressJobOutput: true, RepoRelDir: ".", Workspace: "default", CommandName: command.Apply, Steps: []valid.Step{{StepName: "apply"}}}
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
				Equals(t, []string{"atlantis/apply: ./default", "atlantis/apply: ./default", "atlantis/apply", "atlantis/queue"}, vcsClient.contexts)
				raw.Merged = gh.Ptr(true)
				raw.State = gh.Ptr("closed")
				raw.MergeCommitSHA = gh.Ptr("merged")
			}
			entry := store.entry
			entry.State, entry.ApplyStarted = queue.Merging, true
			entry.Prepared = queue.PreparedPlan{BaseSHA: "base", TreeSHA: tree, Configuration: queueHash(driver.Global)}
			entry.Plans = []queue.Plan{{Project: queue.Project{Directory: ".", Workspace: "default", BackendID: backendID, Inputs: inputs}, Applied: true, ResultState: &queue.StateVersion{}}}
			driver.History = NewRunHistory(runs.NoopStore{}, driver.Log)
			driver.Instance, driver.Deployment = "instance", "deployment"
			entry.Claim = "claim"
			driver.Runner = &ProjectOutputWrapper{ProjectCommandRunner: queueSuccessfulApplyRunner{}, JobURLSetter: queueApplyJobStatus{DefaultCommitStatusUpdater{Client: vcsClient, StatusName: driver.StatusName}}}
			applying := entry
			applying.State = queue.Applying
			_, err = driver.Apply(context.Background(), applying, 0)
			Ok(t, err)
			Equals(t, []models.CommitStatus{models.PendingCommitStatus}, vcsClient.statuses)
			// An apply return alone must not publish success before its durable
			// checkpoint. Merge receives the saved checkpoint above.
			driver.Runner = nil
			statusErr := errors.New("status unavailable")
			if statusFails {
				vcsClient.statusErr = statusErr
				vcsClient.statusContextErr = failureContext
			}
			result, err := driver.Merge(context.Background(), entry)
			if statusFails {
				Assert(t, errors.Is(err, statusErr), "status failure must prevent merge: %v", err)
				Equals(t, failureContext, vcsClient.contexts[len(vcsClient.contexts)-1])
				Assert(t, !slices.Contains(vcsClient.calls, "merge"), "status failure must not attempt merge")
				Assert(t, !raw.GetMerged(), "a failed status write must leave the pull unmerged")
			} else {
				Ok(t, err)
				Equals(t, []string{"status", "status", "status", "status", "merge"}, vcsClient.calls)
				Equals(t, queue.MergeResult{SHA: "merged", Equivalent: true}, result)
			}
			if !statusFails {
				Equals(t, []models.CommitStatus{models.PendingCommitStatus, models.SuccessCommitStatus, models.SuccessCommitStatus, models.SuccessCommitStatus}, vcsClient.statuses)
			}
		})
	}
}

func TestQueueApplyStatusPublicationRetriesFromDurableCheckpoints(t *testing.T) {
	for _, failedContext := range []string{"atlantis/apply: production", "atlantis/apply"} {
		t.Run(failedContext, func(t *testing.T) {
			driver, store, _, client := newQueueReportingDriver(t)
			store.entry.State = queue.AppliedUnmerged
			store.entry.ApplyStarted = true
			store.entry.Plans = []queue.Plan{{Project: queue.Project{Name: "production", Directory: "infra", Workspace: "default"}, Applied: true, ApplyRunID: "apply-run", ResultState: &queue.StateVersion{Lineage: "state", Serial: 2}}}
			client.statusErr = errors.New("temporary GitHub error")
			client.statusContextErr = failedContext
			driver.Report(context.Background(), store.entry)
			Equals(t, 0, len(store.acknowledged))

			// Recreate the adapter from serialized storage, with no execution
			// dependencies. Retrying publication must not run Terraform.
			payload, err := json.Marshal(store.entry)
			Ok(t, err)
			store.entry = queue.Entry{}
			Ok(t, json.Unmarshal(payload, &store.entry))
			restarted, _, _, _ := newQueueReportingDriver(t)
			restarted.Store = store
			restarted.VCS = client
			client.statusErr = nil
			client.contexts, client.statuses, client.urls = nil, nil, nil
			Ok(t, (&QueueService{Driver: restarted}).refresh(context.Background()))
			Equals(t, []int64{store.entry.Version}, store.acknowledged)
			Equals(t, []string{"atlantis/apply: production", "atlantis/apply", "atlantis/queue"}, client.contexts)
			Equals(t, "https://atlantis.example/runs/apply-run", client.urls[0])
			Equals(t, []models.CommitStatus{models.SuccessCommitStatus, models.SuccessCommitStatus, models.FailedCommitStatus}, client.statuses)
		})
	}
}

func TestQueueApplyStatusRequiresDurableSuccess(t *testing.T) {
	for _, tc := range []struct {
		name       string
		started    bool
		applied    bool
		checkpoint *queue.StateVersion
		wantError  bool
	}{
		{name: "before apply"},
		{name: "intent only", started: true},
		{name: "missing state checkpoint", started: true, applied: true, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			driver, store, _, client := newQueueReportingDriver(t)
			e := store.entry
			e.ApplyStarted = tc.started
			e.Plans = []queue.Plan{{Project: queue.Project{Name: "production"}, ApplyIntent: tc.started, Applied: tc.applied, ResultState: tc.checkpoint}}
			err := driver.updateAppliedStatuses(models.Repo{}, e)
			Equals(t, tc.wantError, err != nil)
			Equals(t, 0, len(client.statuses))
		})
	}
}

func TestQueueApplyStatusDoesNotCompletePartialApply(t *testing.T) {
	driver, store, _, client := newQueueReportingDriver(t)
	e := store.entry
	e.ApplyStarted = true
	e.State = queue.PartiallyApplied
	e.Plans = []queue.Plan{
		{Project: queue.Project{Name: "applied"}, Applied: true, ResultState: &queue.StateVersion{}},
		{Project: queue.Project{Name: "failed"}, ApplyIntent: true, Error: "provider failed"},
	}
	Ok(t, driver.updateAppliedStatuses(models.Repo{}, e))
	Equals(t, []string{"atlantis/apply: applied"}, client.contexts)
	Equals(t, []models.CommitStatus{models.SuccessCommitStatus}, client.statuses)
}

// Use the production wrapper/updater: successful non-API plans deliberately
// remain pending until their orchestrator publishes the durable result.
type queueSuccessfulPlanRunner struct{ ProjectCommandRunner }

func (queueSuccessfulPlanRunner) Plan(command.ProjectContext) command.ProjectCommandOutput {
	return command.ProjectCommandOutput{PlanSuccess: &models.PlanSuccess{}}
}
func TestQueuePlanStatusPublicationFromSavedGeneration(t *testing.T) {
	driver, store, _, client := newQueueReportingDriver(t)
	project := queue.Project{Name: "production", Directory: ".", Workspace: "default"}
	wrapper := &ProjectOutputWrapper{ProjectCommandRunner: queueSuccessfulPlanRunner{}, JobURLSetter: queueApplyJobStatus{DefaultCommitStatusUpdater{Client: client, StatusName: driver.StatusName}}}
	result := wrapper.Plan(command.ProjectContext{Log: driver.Log, ProjectName: project.Name, RepoRelDir: project.Directory, Workspace: project.Workspace, SuppressJobOutput: true})
	Assert(t, result.PlanSuccess != nil, "plan succeeds")
	Equals(t, []models.CommitStatus{models.PendingCommitStatus}, client.statuses)
	store.entry.State = queue.AwaitingApproval
	store.entry.Prepared = queue.PreparedPlan{HeadSHA: store.entry.HeadSHA, ArtifactGeneration: "generation", Projects: []queue.Project{project}}
	store.entry.Plans = []queue.Plan{{Project: project, Artifact: runs.PlanArtifactExpectation{RunID: "plan-run", Artifact: runs.ArtifactReference{Key: "prefix/generations/generation/plan"}}}}
	client.statusErr = errors.New("status unavailable")
	client.statusContextErr = "atlantis/plan: production"
	driver.Report(context.Background(), store.entry)
	Equals(t, 0, len(store.acknowledged))
	client.statusContextErr = "atlantis/plan"
	driver.Report(context.Background(), store.entry)
	Equals(t, 0, len(store.acknowledged))
	// Persist/reload and retry without a runner: publication must not replan.
	payload, err := json.Marshal(store.entry)
	Ok(t, err)
	store.entry = queue.Entry{}
	Ok(t, json.Unmarshal(payload, &store.entry))
	restarted, _, _, _ := newQueueReportingDriver(t)
	restarted.Store = store
	restarted.VCS = client
	client.statusErr = nil
	client.contexts = nil
	client.statuses = nil
	client.urls = nil
	Ok(t, (&QueueService{Driver: restarted}).refresh(context.Background()))
	Equals(t, []string{"atlantis/plan: production", "atlantis/plan", "atlantis/queue"}, client.contexts)
	Equals(t, models.SuccessCommitStatus, client.statuses[0])
	Equals(t, "https://atlantis.example/runs/plan-run", client.urls[0])
	Equals(t, []int64{store.entry.Version}, store.acknowledged)
	for _, state := range []queue.State{queue.Queued, queue.Blocked, queue.Planning, queue.Preparing, queue.Replanning, queue.Cancelled} {
		entry := store.entry
		entry.State = state
		client.statuses = nil
		Ok(t, driver.updatePlannedStatuses(models.Repo{}, entry))
		Equals(t, 0, len(client.statuses))
	}
	// Retrying preserves the prior payload while incrementing generation.
	for _, state := range []queue.State{queue.Queued, queue.Blocked} {
		entry := store.entry
		entry.Generation++
		entry.State = state
		client.statuses = nil
		Ok(t, driver.updatePlannedStatuses(models.Repo{}, entry))
		Equals(t, 0, len(client.statuses))
	}
	entry := store.entry
	entry.State, entry.ApplyStarted = queue.RecoveryRequired, true
	entry.HeadSHA = "new-head"
	client.statuses = nil
	Ok(t, driver.updatePlannedStatuses(models.Repo{}, entry))
	Equals(t, 0, len(client.statuses))
	entry = store.entry
	entry.Prepared.ArtifactGeneration = "new-generation"
	client.statuses = nil
	Ok(t, driver.updatePlannedStatuses(models.Repo{}, entry))
	Equals(t, 0, len(client.statuses))
}

type queueStatusFetchFunc func(logging.SimpleLogging, models.PullRequest) (models.PullReqStatus, error)

func (f queueStatusFetchFunc) FetchPullStatus(log logging.SimpleLogging, pull models.PullRequest) (models.PullReqStatus, error) {
	return f(log, pull)
}

func TestQueueValidationPublishesSavedPlanBeforeRequiredChecks(t *testing.T) {
	driver, store, github, client := newQueueReportingDriver(t)
	database, err := boltdb.New(t.TempDir())
	Ok(t, err)
	t.Cleanup(func() { Ok(t, database.Close()) })
	driver.DB = &DBUpdater{Database: database}
	repo := &gh.Repository{FullName: gh.Ptr("o/r"), CloneURL: gh.Ptr("https://github.com/o/r.git")}
	github.queueMetadataGithub = &queueMetadataGithub{pull: &gh.PullRequest{
		Number: gh.Ptr(1), HTMLURL: gh.Ptr("https://github.com/o/r/pull/1"), User: &gh.User{Login: gh.Ptr("author")}, State: gh.Ptr("open"),
		Head: &gh.PullRequestBranch{SHA: gh.Ptr("head"), Ref: gh.Ptr("feature"), Repo: repo},
		Base: &gh.PullRequestBranch{SHA: gh.Ptr("base"), Ref: gh.Ptr("main"), Repo: repo},
	}}
	entry := store.entry
	entry.State = queue.Ready
	project := queue.Project{Name: "production", Directory: ".", Workspace: "default"}
	entry.Prepared = queue.PreparedPlan{HeadSHA: entry.HeadSHA, BaseSHA: "base", Configuration: queueHash(driver.Global), ArtifactGeneration: "generation", Projects: []queue.Project{project}}
	entry.Plans = []queue.Plan{{Project: project, Artifact: runs.PlanArtifactExpectation{RunID: "plan-run", Artifact: runs.ArtifactReference{Key: "generations/generation/plan"}}}}
	checked := false
	driver.Status = queueStatusFetchFunc(func(logging.SimpleLogging, models.PullRequest) (models.PullReqStatus, error) {
		checked = true
		Equals(t, []string{"atlantis/plan: production", "atlantis/plan"}, client.contexts)
		Equals(t, []models.CommitStatus{models.SuccessCommitStatus, models.SuccessCommitStatus}, client.statuses)
		// Even after its own check succeeds, another required check remains
		// mandatory. Do not bypass eligibility to repair deferred publication.
		return models.PullReqStatus{ApprovalStatus: models.ApprovalStatus{IsApproved: true}, MergeableStatus: models.MergeableStatus{IsMergeable: false}}, nil
	})
	err = driver.Validate(context.Background(), entry)
	Assert(t, checked, "required checks must be fetched after plan publication")
	Assert(t, err != nil, "other required check failure must still block apply")
	client.statusErr = errors.New("forge unavailable")
	checked = false
	err = driver.Validate(context.Background(), entry)
	Assert(t, errors.Is(err, client.statusErr) && !checked, "status failure must prevent execution admission")
}
