// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/petergtz/pegomock/v4"
	"github.com/runatlantis/atlantis/server/core/config"
	"github.com/runatlantis/atlantis/server/core/config/valid"
	"github.com/runatlantis/atlantis/server/core/runtime"
	tfclientmocks "github.com/runatlantis/atlantis/server/core/terraform/tfclient/mocks"
	"github.com/runatlantis/atlantis/server/events"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/events/mocks"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/runatlantis/atlantis/server/metrics/metricstest"
	. "github.com/runatlantis/atlantis/testing"
)

func TestGenericApplyDiscoversCustomWorkflowsFromStatus(t *testing.T) {
	for _, scenario := range []string{"nested custom plans", "custom without convention file", "multiple workspaces", "mixed managed and custom", "orphan managed plan", "missing managed plan", "stale status", "failed custom plan", "already applied"} {
		t.Run(scenario, func(t *testing.T) {
			RegisterMockTestingT(t)
			root := t.TempDir()
			write := func(path, content string) {
				t.Helper()
				Ok(t, os.MkdirAll(filepath.Dir(path), 0700))
				Ok(t, os.WriteFile(path, []byte(content), 0600))
			}
			for _, workspace := range []string{"default", "production"} {
				repo := filepath.Join(root, workspace)
				Ok(t, os.MkdirAll(repo, 0700))
				runCmd(t, repo, "git", "init")
				if scenario != "custom without convention file" {
					write(filepath.Join(repo, "custom/generated/dev/atlantis.tfplan"), "custom dev")
					write(filepath.Join(repo, "custom/generated/staging/atlantis.tfplan"), "custom staging")
				}
			}
			repo := filepath.Join(root, "default")
			write(filepath.Join(repo, "atlantis.yaml"), `version: 3
projects:
- name: custom-default
  dir: custom
  workspace: default
  workflow: custom
- name: custom-production
  dir: custom
  workspace: production
  workflow: custom
- name: managed
  dir: managed
  workspace: default
workflows:
  custom:
    plan:
      steps:
      - run: terraform plan -out=generated/dev/atlantis.tfplan
    apply:
      steps:
      - run: terraform apply generated/dev/atlantis.tfplan
`)
			projects := []models.ProjectStatus{{RepoRelDir: "custom", Workspace: "default", ProjectName: "custom-default", Status: models.PlannedPlanStatus}}
			expected := 1
			expectedError := ""
			switch scenario {
			case "multiple workspaces":
				projects = append(projects, models.ProjectStatus{RepoRelDir: "custom", Workspace: "production", ProjectName: "custom-production", Status: models.PlannedPlanStatus})
				expected = 2
			case "mixed managed and custom", "missing managed plan":
				projects = append(projects, models.ProjectStatus{RepoRelDir: "managed", Workspace: "default", ProjectName: "managed", Status: models.PlannedPlanStatus})
				expected = 2
				if scenario == "mixed managed and custom" {
					write(filepath.Join(repo, "managed/managed-default.tfplan"), "managed reviewed plan")
				} else {
					expectedError = "plan file is missing"
				}
			case "orphan managed plan":
				write(filepath.Join(repo, "orphan/default.tfplan"), "orphan")
				expectedError = "no matching plan status"
			case "stale status":
				expectedError = "current head"
			case "failed custom plan":
				projects[0].Status = models.ErroredPlanStatus
				expectedError = "errored"
			case "already applied":
				projects[0].Status = models.AppliedPlanStatus
				expected = 0
			}
			workingDir := mocks.NewMockWorkingDir()
			When(workingDir.GetPullDir(Any[models.Repo](), Any[models.PullRequest]())).ThenReturn(root, nil)
			When(workingDir.GetWorkingDir(Any[models.Repo](), Any[models.PullRequest](), AnyString())).Then(func(params []Param) ReturnValues { return ReturnValues{filepath.Join(root, params[2].(string)), nil} })
			logger := logging.NewNoopLogger(t)
			scope := metricstest.NewLoggingScope(t, logger, "atlantis")
			builder := events.NewProjectCommandBuilder(false, &config.ParserValidator{}, &events.DefaultProjectFinder{}, nil, workingDir, events.NewDefaultWorkingDirLocker(), valid.NewGlobalCfgFromArgs(valid.GlobalCfgArgs{AllowAllRepoSettings: true}), &events.DefaultPendingPlanFinder{}, &events.CommentParser{ExecutableName: "atlantis"}, false, false, false, false, false, "", "**/*.tf", false, "", false, false, "auto", scope, tfclientmocks.NewMockClient(), &runtime.LocalPlanStore{})
			pull := models.PullRequest{HeadCommit: "abc123", BaseBranch: "main"}
			ctx := &command.Context{Log: logger, Scope: scope, Pull: pull, PullStatus: &models.PullStatus{Pull: pull, Projects: projects}}
			if scenario == "stale status" {
				ctx.PullStatus.Pull.HeadCommit = "old123"
			}
			commands, err := builder.BuildApplyCommands(ctx, &events.CommentCommand{Name: command.Apply})
			if expectedError != "" {
				Assert(t, err != nil && strings.Contains(err.Error(), expectedError), "expected %q, got %v", expectedError, err)
				return
			}
			Ok(t, err)
			Equals(t, expected, len(commands))
			seen := map[string]bool{}
			for _, cmd := range commands {
				seen[cmd.ProjectName] = true
				if cmd.ProjectName == "managed" {
					Assert(t, cmd.ExpectedPlanHash != "", "managed plan must still be hashed")
				} else {
					Equals(t, "custom", cmd.RepoRelDir)
					Equals(t, "", cmd.ExpectedPlanHash)
					Assert(t, !cmd.RequiresAtlantisManagedPlanFile, "custom artifact remains user-managed")
				}
			}
			for _, project := range projects {
				if scenario != "already applied" {
					Assert(t, seen[project.ProjectName], "missing project %q", project.ProjectName)
				}
			}
		})
	}
}

func TestPendingPlanFinderLeavesCustomArtifactsUntouched(t *testing.T) {
	for _, separate := range []bool{false, true} {
		t.Run(map[bool]string{false: "workspace", true: "shared store"}[separate], func(t *testing.T) {
			data := t.TempDir()
			plans := data
			if separate {
				plans = t.TempDir()
			}
			pull := filepath.Join(data, "repos/org/repo/1")
			repo := filepath.Join(pull, "default")
			Ok(t, os.MkdirAll(repo, 0700))
			runCmd(t, repo, "git", "init")
			planRoot := filepath.Join(plans, "repos/org/repo/1/default/generated")
			Ok(t, os.MkdirAll(planRoot, 0700))
			for _, name := range []string{"atlantis.tfplan", "staging.tfplan", "default.tfplan", "named-default.tfplan"} {
				Ok(t, os.WriteFile(filepath.Join(planRoot, name), []byte("plan"), 0600))
			}
			finder := events.DefaultPendingPlanFinder{DataDir: data, LocalSharePlanDir: plans}
			found, err := finder.Find(pull)
			Ok(t, err)
			Equals(t, 2, len(found))
			Ok(t, finder.DeletePlans(pull))
			for _, name := range []string{"atlantis.tfplan", "staging.tfplan"} {
				_, err := os.Stat(filepath.Join(planRoot, name))
				Ok(t, err)
			}
			for _, name := range []string{"default.tfplan", "named-default.tfplan"} {
				_, err := os.Stat(filepath.Join(planRoot, name))
				Assert(t, os.IsNotExist(err), "managed plan must be removed")
			}
		})
	}
}
