// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	gh "github.com/google/go-github/v88/github"
	"github.com/runatlantis/atlantis/server/core/boltdb"
	"github.com/runatlantis/atlantis/server/core/queue"
	"github.com/runatlantis/atlantis/server/core/terraform"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/logging"
	. "github.com/runatlantis/atlantis/testing"
	"github.com/uber-go/tally/v4"

	"github.com/runatlantis/atlantis/server/core/config/valid"
	"github.com/runatlantis/atlantis/server/events/command"
)

func TestQueueRejectsUnpinnedWorkflows(t *testing.T) {
	for _, steps := range [][]valid.Step{{{StepName: "apply"}}, {{StepName: "init"}, {StepName: "apply"}}} {
		Ok(t, queueWorkflow(command.ProjectContext{CommandName: command.Apply, Steps: steps}))
	}
	for _, steps := range [][]valid.Step{{{StepName: "plan"}, {StepName: "apply"}}, {{StepName: "run", RunCommand: "terraform apply"}}, {{StepName: "apply", ExtraArgs: []string{"-lock=false"}}}, {{StepName: "apply"}, {StepName: "apply"}}} {
		Assert(t, queueWorkflow(command.ProjectContext{CommandName: command.Apply, Steps: steps}) != nil, "expected an error")
	}
}

func TestQueueWorkflowRejectsStateMutationAndDynamicArguments(t *testing.T) {
	for _, name := range []string{"TF_CLI_ARGS", "TF_CLI_ARGS_init", "TF_CLI_ARGS_plan", "TF_CLI_ARGS_apply"} {
		t.Setenv(name, "")
	}
	for _, test := range []struct {
		name string
		step string
		args []string
	}{
		{"migrate before reservation", "init", []string{"-migrate-state"}},
		{"force copy before reservation", "init", []string{"--force-copy=true"}},
		{"quoted migration", "init", []string{`-migrate-"state"`}},
		{"split lock", "apply", []string{"-lock", "false"}},
		{"quoted lock", "apply", []string{`-lock="false"`}},
		{"numeric lock", "apply", []string{"--lock=0"}},
		{"split refresh", "plan", []string{"-refresh", "false"}},
		{"quoted target", "plan", []string{`-tar"get"=module.network`}},
		{"escaped replace", "plan", []string{`-re\place=aws_instance.example`}},
		{"destroy", "plan", []string{"--destroy"}},
		{"refresh only", "plan", []string{"-refresh-only"}},
		{"saved plan path override", "plan", []string{"-out=other.tfplan"}},
		{"state path override", "plan", []string{"-state=other.tfstate"}},
		{"environment expansion", "plan", []string{"${QUEUE_PLAN_ARGS}"}},
		{"command substitution", "init", []string{"$(terraform force-unlock -force state-lock)"}},
		{"command separator", "init", []string{"; terraform apply -auto-approve"}},
		{"saved plan argument comment", "apply", []string{"-auto-approve #"}},
		{"glob expansion", "apply", []string{"-loc*=false"}},
		{"invalid quoting", "plan", []string{"-var='unfinished"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			name := command.Plan
			steps := []valid.Step{{StepName: "init"}, {StepName: "plan"}}
			if test.step == "apply" {
				name, steps = command.Apply, []valid.Step{{StepName: "apply"}}
			}
			for i := range steps {
				if steps[i].StepName == test.step {
					steps[i].ExtraArgs = test.args
				}
			}
			Assert(t, queueWorkflow(command.ProjectContext{CommandName: name, Steps: steps}) != nil, "unsafe queue workflow must be rejected")
		})
	}
	for _, name := range []string{"TF_CLI_ARGS", "TF_CLI_ARGS_init", "TF_CLI_ARGS_plan"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "-lock=false")
			Assert(t, queueWorkflow(command.ProjectContext{CommandName: command.Plan, Steps: []valid.Step{{StepName: "init"}, {StepName: "plan"}}}) != nil, "inherited flags must preserve queue safety")
		})
	}
	Ok(t, queueWorkflow(command.ProjectContext{CommandName: command.Plan, Steps: []valid.Step{
		{StepName: "init", ExtraArgs: []string{"-backend-config=region=eu-west-1"}},
		{StepName: "plan", ExtraArgs: []string{"-lock=true", "-refresh=true", "-lock-timeout=1m", "-parallelism=5", `-var='name=an example'`, "-var-file=production.tfvars"}},
	}}))
}

type queueMetadataGithub struct {
	pull  *gh.PullRequest
	tree  string
	reads int
}

func (g *queueMetadataGithub) GetPullRequest(logging.SimpleLogging, models.Repo, int) (*gh.PullRequest, error) {
	g.reads++
	return g.pull, nil
}
func (g *queueMetadataGithub) QueueCommitTree(context.Context, models.Repo, string) (string, error) {
	return g.tree, nil
}
func (g *queueMetadataGithub) QueueCanWrite(context.Context, models.Repo, string) (bool, error) {
	return true, nil
}
func (g *queueMetadataGithub) QueueComment(context.Context, models.Repo, int, int64, string, string) (int64, error) {
	return 1, nil
}
func TestQueueMergeRecoveryObservesAlreadyMergedPull(t *testing.T) {
	for _, tree := range []string{"applied-tree", "different-tree"} {
		t.Run(tree, func(t *testing.T) {
			repo := &gh.Repository{FullName: gh.Ptr("o/r"), CloneURL: gh.Ptr("https://github.com/o/r.git")}
			raw := &gh.PullRequest{Number: gh.Ptr(1), HTMLURL: gh.Ptr("https://github.com/o/r/pull/1"), User: &gh.User{Login: gh.Ptr("author")}, State: gh.Ptr("closed"), Merged: gh.Ptr(true), MergeCommitSHA: gh.Ptr("actual-merge"), Head: &gh.PullRequestBranch{SHA: gh.Ptr("head"), Ref: gh.Ptr("feature"), Repo: repo}, Base: &gh.PullRequestBranch{SHA: gh.Ptr("new-base"), Ref: gh.Ptr("main"), Repo: repo}}
			parser := &EventParser{GithubUser: "bot", GithubToken: "test"}
			base, err := parser.ParseGithubRepo(repo)
			Ok(t, err)
			ref, err := NewRepoRef(base)
			Ok(t, err)
			source, err := json.Marshal(ref)
			Ok(t, err)
			database, err := boltdb.New(t.TempDir())
			Ok(t, err)
			defer database.Close()
			github := &queueMetadataGithub{pull: raw, tree: tree}
			driver := &QueueDriver{Github: github, Parser: parser, DB: &DBUpdater{Database: database}, Global: valid.GlobalCfg{Queue: queue.Config{Enabled: true, Repositories: []string{base.ID()}}}, Log: logging.NewNoopLogger(t), Scope: tally.NoopScope}
			// No VCS merge client is configured: trying to merge a second time would panic.
			result, err := driver.Merge(context.Background(), queue.Entry{Repository: base.ID(), PullNumber: 1, TargetBranch: "main", HeadSHA: "head", Actor: "operator", Source: source, ApplyStarted: true, Prepared: queue.PreparedPlan{TreeSHA: "applied-tree", Configuration: queueHash(driver.Global)}})
			Ok(t, err)
			Equals(t, "actual-merge", result.SHA)
			Equals(t, tree == "applied-tree", result.Equivalent)
			Equals(t, 1, github.reads)
		})
	}
}

func TestQueueCheckoutRejectsUntrackedConfiguration(t *testing.T) {
	root := t.TempDir()
	for _, args := range [][]string{{"init"}, {"-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "--allow-empty", "-m", "fixture"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		out, err := cmd.CombinedOutput()
		Assert(t, err == nil, "%s", out)
	}
	_, err := queueGit(context.Background(), root, "rev-parse", "HEAD^{tree}")
	Ok(t, err)
	Ok(t, os.WriteFile(filepath.Join(root, ".gitignore"), []byte("*.tfvars\n"), 0600))
	Ok(t, os.WriteFile(filepath.Join(root, "override.auto.tfvars"), []byte("value=2"), 0600))
	_, err = queueGit(context.Background(), root, "rev-parse", "HEAD^{tree}")
	ErrContains(t, "untracked configuration", err)
}
func TestQueueInputsInvalidateOnModulesAndEnvironment(t *testing.T) {
	driver := &QueueDriver{Distribution: terraform.NewDistributionTerraform()}
	root := t.TempDir()
	p := command.ProjectContext{}
	first, err := driver.inputFingerprint(p, root)
	Ok(t, err)
	driver.Distribution = terraform.NewDistributionTerraform()
	restarted, err := driver.inputFingerprint(p, root)
	Ok(t, err)
	Assert(t, first == restarted, "fresh process objects must not change input identity")
	t.Setenv("TF_VAR_queue_test", "new-value")
	second, err := driver.inputFingerprint(p, root)
	Ok(t, err)
	Assert(t, first != second, "expected different values")
	module := filepath.Join(root, ".terraform", "modules", "network")
	Ok(t, os.MkdirAll(module, 0700))
	Ok(t, os.WriteFile(filepath.Join(module, "main.tf"), []byte("locals { value = 1 }"), 0600))
	third, err := driver.inputFingerprint(p, root)
	Ok(t, err)
	Assert(t, second != third, "expected different values")
	Ok(t, os.WriteFile(filepath.Join(module, "main.tf"), []byte("locals { value = 2 }"), 0600))
	fourth, err := driver.inputFingerprint(p, root)
	Ok(t, err)
	Assert(t, third != fourth, "expected different values")
}

func TestQueueInputsInvalidateOnInheritedTerraformArguments(t *testing.T) {
	driver := &QueueDriver{Distribution: terraform.NewDistributionTerraform()}
	root := t.TempDir()
	for _, name := range []string{"TF_CLI_ARGS", "TF_CLI_ARGS_plan", "TF_CLI_ARGS_init"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "")
			before, err := driver.inputFingerprint(command.ProjectContext{}, root)
			Ok(t, err)
			t.Setenv(name, "-var-file=production.tfvars")
			after, err := driver.inputFingerprint(command.ProjectContext{}, root)
			Ok(t, err)
			Assert(t, before != after, "inherited Terraform CLI arguments must invalidate the prepared inputs")
		})
	}
}
