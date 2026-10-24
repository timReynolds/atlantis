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
	"github.com/uber-go/tally/v4"

	"github.com/runatlantis/atlantis/server/core/config/valid"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/stretchr/testify/require"
)

func TestQueueRejectsUnpinnedWorkflows(t *testing.T) {
	for _, steps := range [][]valid.Step{{{StepName: "apply"}}, {{StepName: "init"}, {StepName: "apply"}}} {
		require.NoError(t, queueWorkflow(command.ProjectContext{CommandName: command.Apply, Steps: steps}))
	}
	for _, steps := range [][]valid.Step{{{StepName: "plan"}, {StepName: "apply"}}, {{StepName: "run", RunCommand: "terraform apply"}}, {{StepName: "apply", ExtraArgs: []string{"-lock=false"}}}, {{StepName: "apply"}, {StepName: "apply"}}} {
		require.Error(t, queueWorkflow(command.ProjectContext{CommandName: command.Apply, Steps: steps}))
	}
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
			require.NoError(t, err)
			ref, err := NewRepoRef(base)
			require.NoError(t, err)
			source, err := json.Marshal(ref)
			require.NoError(t, err)
			database, err := boltdb.New(t.TempDir())
			require.NoError(t, err)
			defer database.Close()
			github := &queueMetadataGithub{pull: raw, tree: tree}
			driver := &QueueDriver{Github: github, Parser: parser, DB: &DBUpdater{Database: database}, Global: valid.GlobalCfg{Queue: queue.Config{Enabled: true, Repositories: []string{base.ID()}}}, Log: logging.NewNoopLogger(t), Scope: tally.NoopScope}
			// No VCS merge client is configured: trying to merge a second time would panic.
			result, err := driver.Merge(context.Background(), queue.Entry{Repository: base.ID(), PullNumber: 1, TargetBranch: "main", HeadSHA: "head", Actor: "operator", Source: source, ApplyStarted: true, Prepared: queue.PreparedPlan{TreeSHA: "applied-tree", Configuration: queueHash(driver.Global)}})
			require.NoError(t, err)
			require.Equal(t, "actual-merge", result.SHA)
			require.Equal(t, tree == "applied-tree", result.Equivalent)
			require.Equal(t, 1, github.reads)
		})
	}
}

func TestQueueCheckoutRejectsUntrackedConfiguration(t *testing.T) {
	root := t.TempDir()
	for _, args := range [][]string{{"init"}, {"-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "--allow-empty", "-m", "fixture"}} {
		cmd := exec.Command("git", args...)
		cmd.Dir = root
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, string(out))
	}
	_, err := queueGit(context.Background(), root, "rev-parse", "HEAD^{tree}")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, ".gitignore"), []byte("*.tfvars\n"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "override.auto.tfvars"), []byte("value=2"), 0600))
	_, err = queueGit(context.Background(), root, "rev-parse", "HEAD^{tree}")
	require.ErrorContains(t, err, "untracked configuration")
}
func TestQueueInputsInvalidateOnModulesAndEnvironment(t *testing.T) {
	driver := &QueueDriver{Distribution: terraform.NewDistributionTerraform()}
	root := t.TempDir()
	p := command.ProjectContext{}
	first, err := driver.inputFingerprint(p, root)
	require.NoError(t, err)
	driver.Distribution = terraform.NewDistributionTerraform()
	restarted, err := driver.inputFingerprint(p, root)
	require.NoError(t, err)
	require.Equal(t, first, restarted, "fresh process objects must not change input identity")
	t.Setenv("TF_VAR_queue_test", "new-value")
	second, err := driver.inputFingerprint(p, root)
	require.NoError(t, err)
	require.NotEqual(t, first, second)
	module := filepath.Join(root, ".terraform", "modules", "network")
	require.NoError(t, os.MkdirAll(module, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(module, "main.tf"), []byte("locals { value = 1 }"), 0600))
	third, err := driver.inputFingerprint(p, root)
	require.NoError(t, err)
	require.NotEqual(t, second, third)
	require.NoError(t, os.WriteFile(filepath.Join(module, "main.tf"), []byte("locals { value = 2 }"), 0600))
	fourth, err := driver.inputFingerprint(p, root)
	require.NoError(t, err)
	require.NotEqual(t, third, fourth)
}
