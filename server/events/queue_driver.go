// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	gh "github.com/google/go-github/v88/github"
	"github.com/google/shlex"
	"github.com/hashicorp/go-version"
	"github.com/runatlantis/atlantis/server/core/config/valid"
	"github.com/runatlantis/atlantis/server/core/queue"
	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/core/runtime"
	"github.com/runatlantis/atlantis/server/core/terraform"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/events/vcs"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/uber-go/tally/v4"
)

type QueueGithub interface {
	GetPullRequest(logging.SimpleLogging, models.Repo, int) (*gh.PullRequest, error)
	QueueCommitTree(context.Context, models.Repo, string) (string, error)
	QueueCanWrite(context.Context, models.Repo, string) (bool, error)
	QueueComment(context.Context, models.Repo, int, int64, string, string) (int64, error)
}

// QueueDriver delegates project execution to the existing builders and runners.
// The durable queue supplies admission and artifact identity, not a second executor.
type QueueDriver struct {
	TeamAllowlistChecker command.TeamAllowlistChecker
	reportMu             sync.Mutex
	PlanStore            runtime.PlanStore
	Store                queue.Store
	Github               QueueGithub
	VCS                  vcs.Client
	Parser               *EventParser
	Builder              ProjectCommandBuilder
	Runner               interface {
		ProjectPlanCommandRunner
		ProjectApplyCommandRunner
		ProjectPolicyCheckCommandRunner
	}
	History      *RunHistory
	WorkingDir   WorkingDir
	Init         StepRunner
	Terraform    runtime.TerraformExec
	Distribution terraform.Distribution
	Version      *version.Version
	DB           *DBUpdater
	Status       vcs.PullReqStatusFetcher
	Global       valid.GlobalCfg
	Log          logging.SimpleLogging
	Scope        tally.Scope
	Instance     runs.ID
	Deployment   string
	URL          string
	StatusName   string
	Authorize    func(models.Repo, *models.User, string) (bool, error)
	ApplyAllowed func() error
}

func queueHash(v any) string {
	b, _ := json.Marshal(v)
	return fmt.Sprintf("sha256:%x", sha256.Sum256(b))
}
func (d *QueueDriver) context(ctx context.Context, e queue.Entry) (*command.Context, *gh.PullRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if !d.Global.Queue.Allows(e.Repository) {
		return nil, nil, errors.New("queue is disabled for this repository")
	}
	var ref RepoRef
	if err := json.Unmarshal(e.Source, &ref); err != nil {
		return nil, nil, err
	}
	repo, err := d.Parser.HydrateRepo(ref)
	if err != nil {
		return nil, nil, err
	}
	if repo.ID() != e.Repository || repo.VCSHost.Type != models.Github {
		return nil, nil, errors.New("queue repository identity mismatch")
	}
	raw, err := d.Github.GetPullRequest(d.Log, repo, e.PullNumber)
	if err != nil {
		return nil, nil, queue.TransientError{Err: err}
	}
	pull, _, head, err := d.Parser.ParseGithubPull(d.Log, raw)
	if err != nil {
		return nil, nil, err
	}
	if raw.GetHead().GetSHA() != e.HeadSHA || raw.GetBase().GetRef() != e.TargetBranch {
		if !e.ApplyStarted {
			_, err = d.Store.Enqueue(ctx, queue.Request{Repository: e.Repository, PullNumber: e.PullNumber, TargetBranch: raw.GetBase().GetRef(), HeadSHA: raw.GetHead().GetSHA(), Actor: e.Actor, Source: e.Source})
			if err != nil {
				return nil, nil, err
			}
		}
		return nil, nil, queue.ErrReplan
	}
	matched := d.Global.MatchingRepo(e.Repository)
	if matched != nil && !matched.BranchMatches(e.TargetBranch) {
		return nil, nil, errors.New("queue target branch is no longer allowed")
	}
	user := models.User{Username: e.Actor}
	allowed, err := d.Github.QueueCanWrite(ctx, repo, user.Username)
	if err != nil {
		return nil, nil, err
	}
	if !allowed {
		return nil, nil, errors.New("queue requester no longer has repository write permission")
	}
	if d.Authorize != nil {
		allowed, err = d.Authorize(repo, &user, command.Apply.String())
		if err != nil {
			return nil, nil, err
		}
		if !allowed {
			return nil, nil, errors.New("queue requester is denied by team authorization")
		}
	}
	status, err := d.DB.Database.GetPullStatus(pull)
	if err != nil {
		return nil, nil, err
	}
	c := &command.Context{TeamAllowlistChecker: d.TeamAllowlistChecker, Pull: pull, HeadRepo: head, User: user, Log: d.Log, Scope: d.Scope, PullStatus: status, Trigger: command.CommentTrigger, FailOnMissingDependencies: true, FailOnTeamAllowlistDenied: true, RunPolicyChecks: true, ExecutionInstanceID: d.Instance, ExecutionDeploymentID: d.Deployment, ConcurrencyKey: "queue/" + string(e.ID), OwnershipClaimID: string(e.Claim), SuppressJobOutput: true}
	c.ExecutionLease = queueExecutionLease{ctx: ctx, store: d.Store, entry: e}
	return c, raw, nil
}

type queueExecutionLease struct {
	ctx   context.Context
	store queue.Store
	entry queue.Entry
}

func (l queueExecutionLease) Admit(ctx context.Context) error {
	if err := l.ctx.Err(); err != nil {
		return err
	}
	return l.store.CheckClaim(ctx, l.entry)
}
func (d *QueueDriver) planContexts(c *command.Context) ([]command.ProjectContext, error) {
	status, err := d.Status.FetchPullStatus(c.Log, c.Pull)
	if err != nil {
		return nil, queue.TransientError{Err: err}
	}
	c.PullRequestStatus = status

	for _, r := range d.Global.Repos {
		if r.IDMatches(c.Pull.BaseRepo.ID()) && (len(r.PreWorkflowHooks) > 0 || len(r.PostWorkflowHooks) > 0) {
			return nil, errors.New("queue cannot safely fingerprint workflow hooks")
		}
	}
	projects, err := d.Builder.BuildPlanCommands(c, &CommentCommand{Name: command.Plan})
	if err != nil {
		return nil, err
	}
	for _, p := range projects {
		if err := queueWorkflow(p); err != nil {
			return nil, err
		}
	}
	return projects, nil
}
func queueWorkflow(p command.ProjectContext) error {
	validateArgs := func(raw string) error {
		// Terraform runners join extra args into sh -c. Only literal arguments
		// can be checked before init, which runs before state reservations exist.
		if strings.ContainsAny(raw, "$`;|&<>\n\r#*?[]~()") {
			return errors.New("queue workflow requires literal arguments without shell expansion")
		}
		args, err := shlex.Split(raw)
		if err != nil {
			return fmt.Errorf("parsing queue workflow arguments: %w", err)
		}
		for _, arg := range args {
			if !strings.HasPrefix(arg, "-") {
				continue
			}
			name, value, _ := strings.Cut(strings.TrimLeft(arg, "-"), "=")
			switch name {
			case "lock", "refresh":
				if value != "true" {
					return errors.New("queue workflow requires locking and state refresh")
				}
			case "migrate-state", "force-copy", "target", "replace", "destroy", "refresh-only", "out", "state", "state-out", "backup":
				return fmt.Errorf("queue workflow cannot use %q because it changes state or the saved plan scope", name)
			}
		}
		return nil
	}
	if err := validateArgs(strings.Join(p.EscapedCommentArgs, " ")); err != nil {
		return err
	}
	expected := "plan"
	if p.CommandName == command.Apply {
		expected = "apply"
	}
	if p.CommandName == command.PolicyCheck {
		expected = "policy_check"
	}
	executions := 0
	for _, step := range p.Steps {
		if step.StepName == expected {
			executions++
		}
		if (p.CommandName == command.Apply && step.StepName == "plan") || (p.CommandName != command.Apply && step.StepName == "apply") {
			return errors.New("queue workflow must apply the exact saved plan")
		}
	}
	if executions != 1 {
		return errors.New("queue workflow must contain exactly one execution step")
	}

	for _, step := range p.Steps {
		switch step.StepName {
		case "init", "plan", "show", "apply", "policy_check":
		default:
			return fmt.Errorf("queue requires saved-plan workflows; step %q is not supported", step.StepName)
		}
		for _, args := range []string{strings.Join(step.ExtraArgs, " "), os.Getenv("TF_CLI_ARGS"), os.Getenv("TF_CLI_ARGS_" + step.StepName)} {
			if err := validateArgs(args); err != nil {
				return err
			}
		}
	}
	return nil
}
func (d *QueueDriver) path(p command.ProjectContext) (string, string, error) {
	root, err := d.WorkingDir.GetWorkingDir(p.BaseRepo, p.Pull, p.Workspace)
	if err != nil {
		return "", "", err
	}
	return root, filepath.Join(root, p.RepoRelDir), nil
}
func queueGit(ctx context.Context, root string, args ...string) (string, error) {
	if len(args) == 2 && args[0] == "rev-parse" && args[1] == "HEAD^{tree}" {
		check := exec.CommandContext(ctx, "git", "diff", "--quiet", "HEAD", "--")
		check.Dir = root
		if err := check.Run(); err != nil {
			return "", errors.New("queue checkout contains modified tracked configuration")
		}
		untracked := exec.CommandContext(ctx, "git", "ls-files", "--others", "-z")
		untracked.Dir = root
		files, err := untracked.Output()
		if err != nil {
			return "", fmt.Errorf("checking untracked queue inputs: %w", err)
		}
		for _, file := range strings.Split(string(files), "\x00") {
			parts := strings.Split(file, "/")
			if slices.Contains(parts, ".terraform") || slices.Contains(parts, ".terragrunt-cache") {
				continue
			}
			for _, suffix := range []string{".tf", ".tf.json", ".tfvars", ".tfvars.json", ".hcl"} {
				if strings.HasSuffix(file, suffix) {
					return "", fmt.Errorf("queue checkout contains untracked configuration %q", file)
				}
			}
		}
	}

	c := exec.CommandContext(ctx, "git", args...)
	c.Dir = root
	b, err := c.Output()
	if err != nil {
		return "", fmt.Errorf("reading queue integration checkout: %w", err)
	}
	return strings.TrimSpace(string(b)), nil
}
func (d *QueueDriver) tf(p command.ProjectContext, path string, args ...string) (string, error) {
	dist, v := d.Distribution, d.Version
	if p.TerraformDistribution != nil {
		dist = terraform.NewDistribution(*p.TerraformDistribution)
	}
	if p.TerraformVersion != nil {
		v = p.TerraformVersion
	}
	p.SuppressJobOutput = true
	// TerraformExec receives an argument vector, not shell source.
	return d.Terraform.RunCommandWithVersion(p, path, args, map[string]string{"TF_WORKSPACE": p.Workspace}, dist, v, p.Workspace)
}
func (d *QueueDriver) state(p command.ProjectContext, path string) (queue.StateVersion, error) {
	out, err := d.tf(p, path, "state", "pull")
	if err != nil {
		return queue.StateVersion{}, fmt.Errorf("reading backend state: %w", err)
	}
	// Terraform reports a missing state snapshot with a successful empty pull.
	// Keep backend admission even before a lineage exists.
	if strings.TrimSpace(out) == "" {
		return queue.StateVersion{}, nil
	}
	return queue.ParseStateVersion([]byte(out))
}
func queueStateID(p command.ProjectContext, path string) (string, error) {
	data, err := os.ReadFile(filepath.Join(path, ".terraform", "terraform.tfstate"))
	if err != nil {
		return "", err
	}
	if err = queue.ValidateBackendLocking(data); err != nil {
		return "", err
	}
	return queue.BackendStateID(data, p.Workspace)
}
func queueStateIdentity(backend string, state queue.StateVersion) string {
	return queue.StateIdentity(backend, state)
}
func queueProject(p command.ProjectContext, state string) queue.Project {
	return queue.Project{Name: p.ProjectName, Directory: p.RepoRelDir, Workspace: p.Workspace, Workflow: p.WorkflowIdentity, StateID: state, DependsOn: p.DependsOn}
}
func (d *QueueDriver) Prepare(ctx context.Context, e queue.Entry) (queue.PreparedPlan, error) {
	c, raw, err := d.context(ctx, e)
	if err != nil {
		return queue.PreparedPlan{}, err
	}
	if raw.GetState() != "open" || raw.GetDraft() {
		return queue.PreparedPlan{}, errors.New("queue requires an open, ready pull request")
	}
	projects, err := d.planContexts(c)
	if err != nil {
		return queue.PreparedPlan{}, err
	}
	generation, err := runs.NewID()
	if err != nil {
		return queue.PreparedPlan{}, err
	}
	prepared := queue.PreparedPlan{BaseSHA: raw.GetBase().GetSHA(), HeadSHA: e.HeadSHA, ArtifactGeneration: generation, Configuration: queueHash(d.Global)}
	for _, p := range projects {
		if p.CommandName != command.Plan {
			continue
		}
		if _, err = d.WorkingDir.MergeAgain(p.Log, p.HeadRepo, p.Pull, p.Workspace); err != nil {
			return prepared, err
		}
		root, path, err := d.path(p)
		if err != nil {
			return prepared, err
		}
		if err = d.initialize(p, path); err != nil {
			return prepared, err
		}
		state, err := queueStateID(p, path)
		if err != nil {
			return prepared, err
		}
		sha, err := queueGit(ctx, root, "rev-parse", "HEAD")
		if err != nil {
			return prepared, err
		}
		tree, err := queueGit(ctx, root, "rev-parse", "HEAD^{tree}")
		if err != nil {
			return prepared, err
		}
		if _, err = queueGit(ctx, root, "merge-base", "--is-ancestor", prepared.BaseSHA, "HEAD"); err != nil {
			return prepared, queue.ErrReplan
		}
		if _, err = queueGit(ctx, root, "merge-base", "--is-ancestor", e.HeadSHA, "HEAD"); err != nil {
			return prepared, queue.ErrReplan
		}
		if prepared.TreeSHA != "" && prepared.TreeSHA != tree {
			return prepared, queue.ErrReplan
		}
		prepared.TreeSHA, prepared.IntegrationSHA = tree, sha
		version, err := d.state(p, path)
		if err != nil {
			return prepared, err
		}
		project := queueProject(p, queueStateIdentity(state, version))
		project.BackendID = state
		project.Inputs, err = d.inputFingerprint(p, path)
		if err != nil {
			return prepared, err
		}
		prepared.Projects = append(prepared.Projects, project)
	}
	return prepared, nil
}
func (d *QueueDriver) record(c *command.Context, p command.ProjectContext, out command.ProjectCommandOutput) error {
	result := command.ProjectResult{ProjectCommandOutput: out, Command: p.CommandName, RepoRelDir: p.RepoRelDir, Workspace: p.Workspace, ProjectName: p.ProjectName}
	status, err := d.DB.updateDB(c, c.Pull, []command.ProjectResult{result})
	if err != nil {
		return err
	}
	c.PullStatus = &status
	if out.Error != nil {
		return out.Error
	}
	if out.Failure != "" {
		return errors.New(out.Failure)
	}
	if out.Cancelled || out.OwnershipLost {
		return queue.ErrConflict
	}
	if !result.IsSuccessful() {
		return errors.New("project execution did not return success")
	}
	return nil
}
func (d *QueueDriver) Plan(ctx context.Context, e queue.Entry) (plans []queue.Plan, err error) {
	c, raw, err := d.context(ctx, e)
	if err != nil {
		return nil, err
	}
	if raw.GetBase().GetSHA() != e.Prepared.BaseSHA {
		return nil, queue.ErrReplan
	}
	life := d.History.Begin(c, runs.CommandPlan, runs.TriggerComment)
	defer life.FinishRecovering()
	if !life.CanExecute() {
		return nil, life.AdmissionError()
	}
	if err = d.Store.LinkRun(ctx, e, c.RunID, c.AttemptID); err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			c.CommandHasErrors = true
		}
	}()
	projects, err := d.planContexts(c)
	if err != nil {
		return nil, err
	}
	for _, p := range projects {
		p.PlanGeneration = e.Prepared.ArtifactGeneration
		p.RetainPlan = true
		p.RunPolicyChecks = true
		p.PullStatus = c.PullStatus
		if p.CommandName == command.PolicyCheck {
			out := d.Runner.PolicyCheck(p)
			if err = d.record(c, p, out); err != nil {
				return nil, err
			}
			continue
		}
		if p.CommandName != command.Plan {
			continue
		}
		_, path, pathErr := d.path(p)
		if pathErr != nil {
			return nil, pathErr
		}
		before, readErr := d.state(p, path)
		if readErr != nil {
			return nil, readErr
		}
		out := d.Runner.Plan(p)
		if err = d.record(c, p, out); err != nil {
			return nil, err
		}
		after, readErr := d.state(p, path)
		if readErr != nil {
			return nil, readErr
		}
		if before != after {
			return nil, queue.ErrReplan
		}
		stateID, idErr := queueStateID(p, path)
		if idErr != nil {
			return nil, idErr
		}
		project := queueProject(p, queueStateIdentity(stateID, before))
		project.BackendID = stateID
		project.Inputs, err = d.inputFingerprint(p, path)
		if err != nil {
			return nil, err
		}
		expectedIndex := slices.IndexFunc(e.Prepared.Projects, func(candidate queue.Project) bool { return candidate.Key() == project.Key() })
		if expectedIndex < 0 || queueHash(e.Prepared.Projects[expectedIndex]) != queueHash(project) {
			return nil, queue.ErrReplan
		}
		artifact, readErr := d.History.planArtifacts.FindPlanArtifact(ctx, runs.PlanArtifactLookup{Repository: e.Repository, PullNumber: e.PullNumber, HeadSHA: e.HeadSHA, ProjectName: p.ProjectName, Directory: p.RepoRelDir, Workspace: p.Workspace})
		if readErr != nil {
			return nil, readErr
		}
		if !strings.Contains(artifact.Artifact.Key, "generations/"+string(e.Prepared.ArtifactGeneration)+"/") {
			return nil, errors.New("plan artifact generation mismatch")
		}
		jsonPlan, readErr := d.tf(p, path, "show", "-json", runtime.GetPlanFilename(p.Workspace, p.ProjectName))
		if readErr != nil {
			return nil, readErr
		}
		fingerprint, summary, destructive, readErr := queue.SemanticPlan([]byte(jsonPlan))
		if readErr != nil {
			return nil, readErr
		}
		plans = append(plans, queue.Plan{Project: project, Artifact: artifact, State: before, SemanticDigest: fingerprint, Summary: summary, Destructive: destructive})
	}
	e.Plans = plans
	if err = d.Validate(ctx, e); err != nil {
		return nil, err
	}
	return plans, nil
}
func (d *QueueDriver) applyContext(c *command.Context, e queue.Entry, i int) (command.ProjectContext, error) {
	plan := e.Plans[i]
	p := plan.Project
	projects, err := d.Builder.BuildApplyCommands(c, &CommentCommand{Name: command.Apply, RepoRelDir: p.Directory, Workspace: p.Workspace, ProjectName: p.Name})
	if err != nil {
		return command.ProjectContext{}, err
	}
	if len(projects) != 1 {
		return command.ProjectContext{}, errors.New("queued project selection changed")
	}
	project := projects[0]
	if err = queueWorkflow(project); err != nil {
		return project, err
	}
	if project.WorkflowIdentity != p.Workflow {
		return project, queue.ErrReplan
	}
	project.PlanGeneration = e.Prepared.ArtifactGeneration
	project.RetainPlan = true
	project.ExpectedPlanArtifact = &plan.Artifact
	project.ExpectedPlanHash = strings.TrimPrefix(plan.Artifact.Artifact.Checksum, "sha256:")
	project.QueueEntryID = e.ID
	project.QueueClaim = e.Claim
	project.QueueGeneration = e.Generation
	project.FailOnMissingDependencies = true
	project.PullStatus = c.PullStatus
	project.PullReqStatus = c.PullRequestStatus
	project.RunPolicyChecks = true
	return project, nil
}
func (d *QueueDriver) Validate(ctx context.Context, e queue.Entry) error {
	if err := d.Store.CheckClaim(ctx, e); err != nil {
		return err
	}
	c, raw, err := d.context(ctx, e)
	if err != nil {
		return err
	}
	if raw.GetState() != "open" || raw.GetDraft() {
		return errors.New("pull request is no longer open and ready")
	}
	if raw.GetBase().GetSHA() != e.Prepared.BaseSHA || e.Prepared.Configuration != queueHash(d.Global) {
		return queue.ErrReplan
	}
	if err = d.validateApplyRequirements(c); err != nil {
		return err
	}
	return d.validateStates(ctx, c, e)
}

// Fetch requirements at each execution boundary; do not reuse a cached result.
func (d *QueueDriver) validateApplyRequirements(c *command.Context) error {
	if d.ApplyAllowed != nil {
		if err := d.ApplyAllowed(); err != nil {
			return err
		}
	}
	status, err := d.Status.FetchPullStatus(c.Log, c.Pull)
	if err != nil {
		return err
	}
	c.PullRequestStatus = status
	if !status.ApprovalStatus.IsApproved || !status.MergeableStatus.IsMergeable {
		return errors.New("pull request approvals or required checks are not satisfied")
	}
	return nil
}
func (d *QueueDriver) validateStates(ctx context.Context, c *command.Context, e queue.Entry) error {
	for i, plan := range e.Plans {
		p, err := d.applyContext(c, e, i)
		if err != nil {
			return err
		}
		root, path, err := d.path(p)
		if err != nil {
			return err
		}
		if _, statErr := os.Stat(filepath.Join(path, ".terraform", "terraform.tfstate")); os.IsNotExist(statErr) {
			planCommands, buildErr := d.Builder.BuildPlanCommands(c, &CommentCommand{Name: command.Plan, RepoRelDir: p.RepoRelDir, Workspace: p.Workspace, ProjectName: p.ProjectName})
			if buildErr != nil {
				return buildErr
			}
			initialized := false
			for _, planCtx := range planCommands {
				if planCtx.CommandName == command.Plan && planCtx.ProjectName == p.ProjectName && planCtx.Workspace == p.Workspace {
					if err = d.initialize(planCtx, path); err != nil {
						return err
					}
					initialized = true
				}
			}
			if !initialized {
				return errors.New("cannot restore queue backend initialization")
			}
		}
		tree, err := queueGit(ctx, root, "rev-parse", "HEAD^{tree}")
		if err != nil {
			return err
		}
		if tree != e.Prepared.TreeSHA {
			return queue.ErrReplan
		}
		stateID, err := queueStateID(p, path)
		if err != nil {
			return err
		}
		if stateID != plan.Project.BackendID {
			return queue.ErrReplan
		}
		inputs, err := d.inputFingerprint(p, path)
		if err != nil {
			return err
		}
		if inputs != plan.Project.Inputs {
			return queue.ErrReplan
		}
		current, err := d.state(p, path)
		if err != nil {
			return err
		}
		expected := plan.State
		if plan.Applied {
			if plan.ResultState == nil {
				return queue.ErrConflict
			}
			expected = *plan.ResultState
		}
		if current != expected {
			return queue.ErrReplan
		}
	}
	return nil
}
func (d *QueueDriver) Apply(ctx context.Context, e queue.Entry, i int) (result queue.ApplyResult, err error) {
	c, _, err := d.context(ctx, e)
	if err != nil {
		return result, err
	}
	c.PullRequestStatus, err = d.Status.FetchPullStatus(c.Log, c.Pull)
	if err != nil {
		return result, err
	}
	life := d.History.Begin(c, runs.CommandApply, runs.TriggerComment)
	defer life.FinishRecovering()
	if !life.CanExecute() {
		return result, life.AdmissionError()
	}
	result.RunID, result.AttemptID = c.RunID, c.AttemptID
	if err = d.Store.LinkRun(ctx, e, c.RunID, c.AttemptID); err != nil {
		return result, err
	}
	defer func() {
		if err != nil {
			c.CommandHasErrors = true
		}
	}()
	p, err := d.applyContext(c, e, i)
	if err != nil {
		return result, err
	}
	out := d.Runner.Apply(p)
	if err = d.record(c, p, out); err != nil {
		return result, err
	}
	_, path, err := d.path(p)
	if err != nil {
		return result, err
	}
	result.State, err = d.state(p, path)
	return result, err
}
func (d *QueueDriver) Merge(ctx context.Context, e queue.Entry) (queue.MergeResult, error) {
	c, raw, err := d.context(ctx, e)
	if err != nil {
		return queue.MergeResult{}, err
	}
	if !raw.GetMerged() {
		if err = d.Validate(ctx, e); err != nil {
			return queue.MergeResult{}, err
		}
		if !queueApplyComplete(e) {
			return queue.MergeResult{}, errors.New("queue merge requires every project to be applied")
		}
		// A required queue check must succeed before GitHub can merge. The
		// durable entry remains MERGING, and reservations remain held.
		if err = d.updateQueueStatus(c.Pull.BaseRepo, e, models.SuccessCommitStatus); err != nil {
			return queue.MergeResult{}, err
		}
		// GitHub guards the PR head. A concurrent base update is detected by the
		// resulting tree and retains all state fences for operator reconciliation.
		mergeErr := d.VCS.MergePull(c.Log, c.Pull, models.PullRequestOptions{MergeMethod: "merge", ExpectedHeadSHA: e.HeadSHA})
		raw, err = d.Github.GetPullRequest(c.Log, c.Pull.BaseRepo, e.PullNumber)
		if err != nil {
			return queue.MergeResult{}, err
		}
		if !raw.GetMerged() {
			if mergeErr != nil {
				return queue.MergeResult{}, mergeErr
			}
			return queue.MergeResult{}, errors.New("merge outcome is not yet confirmed")
		}
	}
	// A confirmed GitHub merge still requires the recorded resulting states.
	// External Terraform writes or changed local inputs keep reservations held.
	if e.Prepared.Configuration != queueHash(d.Global) {
		return queue.MergeResult{}, queue.ErrReplan
	}
	if err = d.validateStates(ctx, c, e); err != nil {
		return queue.MergeResult{}, err
	}
	sha := raw.GetMergeCommitSHA()
	tree, err := d.Github.QueueCommitTree(ctx, c.Pull.BaseRepo, sha)
	if err != nil {
		return queue.MergeResult{}, err
	}
	return queue.MergeResult{SHA: sha, Equivalent: tree == e.Prepared.TreeSHA && raw.GetHead().GetSHA() == e.HeadSHA}, nil
}
func (d *QueueDriver) Report(ctx context.Context, e queue.Entry) {
	d.reportMu.Lock()
	defer d.reportMu.Unlock()
	latest, err := d.Store.Get(ctx, e.ID)
	if err != nil {
		d.Log.Warn("reading queue report %v", err)
		return
	}
	e = latest
	var ref RepoRef
	if json.Unmarshal(e.Source, &ref) != nil {
		return
	}
	repo, err := d.Parser.HydrateRepo(ref)
	if err != nil {
		return
	}
	url := strings.TrimRight(d.URL, "/") + "/queue/" + string(e.ID)
	body := fmt.Sprintf("### Apply queue: %s\n\nPosition **%d** · Generation **%d** · [Queue details](%s)\n\n%s\n", e.State, e.Position, e.Generation, url, e.Reason)
	for _, p := range e.Plans {
		body += fmt.Sprintf("\n- `%s` / `%s`: %s", p.Project.Directory, p.Project.Workspace, p.Summary)
		if p.Artifact.RunID != "" {
			body += fmt.Sprintf(" ([plan run](%s/runs/%s))", strings.TrimRight(d.URL, "/"), p.Artifact.RunID)
		}
	}
	if e.State == queue.AwaitingApproval {
		body += fmt.Sprintf("\n\nReview the plan runs, then use `atlantis queue approve %d`.", e.Generation)
	}
	marker := "<!-- atlantis-queue:" + string(e.ID) + " -->"
	reported := true
	id, err := d.Github.QueueComment(ctx, repo, e.PullNumber, e.CommentID, marker, body)
	if err != nil {
		reported = false
		d.Log.Warn("updating queue comment %v", err)
	} else if id != e.CommentID {
		if err = d.Store.SetComment(ctx, e.ID, id); err != nil {
			reported = false
			d.Log.Warn("recording queue comment %v", err)
		}
	}
	// An old lifecycle's delayed final report must not overwrite the newer
	// entry's shared commit status for the same pull request.
	current, err := d.Store.Find(ctx, e.Repository, e.PullNumber)
	if err != nil {
		reported = false
	} else if current.ID == e.ID {
		if err = d.updateQueueStatus(repo, e, queueCommitStatus(e)); err != nil {
			reported = false
			d.Log.Warn("updating queue status %v", err)
		}
	}
	if reported {
		if err = d.Store.MarkReported(ctx, e.ID, e.Version); err != nil {
			d.Log.Warn("recording queue report %v", err)
		}
	}
	d.Scope.Counter("reports").Inc(1)
}

func queueApplyComplete(e queue.Entry) bool {
	return e.ApplyStarted && len(e.Plans) > 0 && !slices.ContainsFunc(e.Plans, func(plan queue.Plan) bool { return !plan.Applied })
}
func queueCommitStatus(e queue.Entry) models.CommitStatus {
	if e.State == queue.Completed || e.State == queue.Cancelled || (e.State == queue.Merging && queueApplyComplete(e)) {
		return models.SuccessCommitStatus
	}
	if e.State == queue.Blocked || e.State.Recoverable() {
		return models.FailedCommitStatus
	}
	return models.PendingCommitStatus
}
func (d *QueueDriver) updateQueueStatus(repo models.Repo, e queue.Entry, status models.CommitStatus) error {
	url := strings.TrimRight(d.URL, "/") + "/queue/" + string(e.ID)
	return d.VCS.UpdateStatus(d.Log, repo, models.PullRequest{Num: e.PullNumber, BaseRepo: repo, HeadCommit: e.HeadSHA}, status, truncateContext(d.StatusName+"/queue"), "Apply queue: "+string(e.State), url)
}

var _ queue.Driver = (*QueueDriver)(nil)

func (d *QueueDriver) initialize(p command.ProjectContext, path string) error {
	// A committed lock file makes provider selection reproducible after restart.
	if _, err := os.Stat(filepath.Join(path, ".terraform.lock.hcl")); err != nil {
		return errors.New("queue requires a committed Terraform dependency lock file")
	}
	if _, err := queueGit(context.Background(), path, "ls-files", "--error-unmatch", ".terraform.lock.hcl"); err != nil {
		return errors.New("queue dependency lock file must be committed")
	}
	for _, step := range p.Steps {
		if step.StepName == "init" {
			_, err := d.Init.Run(p, step.ExtraArgs, path, map[string]string{})
			return err
		}
	}
	return errors.New("queue plan workflow requires an init step")
}

// ApprovedBaseline uses the checksum-verified saved plans the approved PR
// already exposed. If any project cannot be proven, the refreshed generation
// requires explicit approval instead of guessing equivalence.
func (d *QueueDriver) ApprovedBaseline(c *command.Context) string {
	status, err := d.Status.FetchPullStatus(c.Log, c.Pull)
	if err != nil || !status.ApprovalStatus.IsApproved {
		return ""
	}
	projects, err := d.Builder.BuildApplyCommands(c, &CommentCommand{Name: command.Apply})
	if err != nil || len(projects) == 0 {
		return ""
	}
	var plans []queue.Plan
	for _, p := range projects {
		if err = queueWorkflow(p); err != nil {
			return ""
		}
		_, path, err := d.path(p)
		if err != nil {
			return ""
		}
		version, err := d.state(p, path)
		if err != nil {
			return ""
		}
		backend, err := queueStateID(p, path)
		if err != nil {
			return ""
		}
		planPath := filepath.Join(path, runtime.GetPlanFilename(p.Workspace, p.ProjectName))
		if err = d.PlanStore.Load(p, planPath); err != nil {
			return ""
		}
		output, err := d.tf(p, path, "show", "-json", runtime.GetPlanFilename(p.Workspace, p.ProjectName))
		if err != nil {
			return ""
		}
		fingerprint, _, _, err := queue.SemanticPlan([]byte(output))
		if err != nil {
			return ""
		}
		project := queueProject(p, queueStateIdentity(backend, version))
		project.BackendID = backend
		plans = append(plans, queue.Plan{Project: project, SemanticDigest: fingerprint})
	}
	return queue.PlanDigest(plans)
}

// Keep mutable module downloads and process-provided variables pinned between
// planning, applying and recovery. Only the digest enters durable queue records.
func (d *QueueDriver) inputFingerprint(p command.ProjectContext, directory string) (string, error) {
	values := map[string]string{}
	for _, env := range os.Environ() {
		name, value, _ := strings.Cut(env, "=")
		if strings.HasPrefix(name, "TF_VAR_") || name == "TF_CLI_ARGS" || strings.HasPrefix(name, "TF_CLI_ARGS_") {
			values[name] = value
		}
	}
	if d.Distribution != nil {
		values["distribution"] = d.Distribution.BinName()
	}
	values["version"] = fmt.Sprint(d.Version)
	if p.TerraformDistribution != nil {
		values["distribution"] = terraform.NewDistribution(*p.TerraformDistribution).BinName()
	}
	if p.TerraformVersion != nil {
		values["version"] = p.TerraformVersion.String()
	}
	moduleRoot := filepath.Join(directory, ".terraform", "modules")
	var files []string
	err := filepath.WalkDir(moduleRoot, func(path string, entry fs.DirEntry, err error) error {
		if os.IsNotExist(err) && path == moduleRoot {
			return nil
		}
		if err != nil {
			return err
		}
		if entry.IsDir() && entry.Name() == ".git" {
			return filepath.SkipDir
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errors.New("queue cannot fingerprint symlinked module downloads")
		}
		if !entry.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(files)
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return "", err
		}
		rel, err := filepath.Rel(moduleRoot, file)
		if err != nil {
			return "", err
		}
		values["module/"+filepath.ToSlash(rel)] = queueHash(data)
	}
	return queueHash(values), nil
}

func (d *QueueDriver) ObserveQueue(event string, duration time.Duration) {
	switch event {
	case "wait", "planning", "apply", "merge":
		d.Scope.Timer(event + "_duration").Record(duration)
	default:
		d.Scope.Counter(event).Inc(1)
	}
}
