// Copyright 2025 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package web_templates

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/jobs"
	. "github.com/runatlantis/atlantis/testing"
)

func TestIndexTemplate(t *testing.T) {
	err := IndexTemplate.Execute(io.Discard, IndexData{
		Locks: []LockIndexData{
			{
				LockPath:      "lock path",
				RepoFullName:  "repo full name",
				PullNum:       1,
				Path:          "path",
				Workspace:     "workspace",
				Time:          time.Now(),
				TimeFormatted: "2006-01-02 15:04:05",
			},
		},
		ApplyLock: ApplyLockData{
			Locked:        true,
			Time:          time.Now(),
			TimeFormatted: "2006-01-02 15:04:05",
		},
		AtlantisVersion: "v0.0.0",
		CleanedBasePath: "/path",
		PullToJobMapping: []jobs.PullInfoWithJobIDs{
			{
				Pull: jobs.PullInfo{
					PullNum:      1,
					Repo:         "repo",
					RepoFullName: "repo full name",
					ProjectName:  "project name",
					Path:         "path",
					Workspace:    "workspace",
				},
				JobIDInfos: []jobs.JobIDInfo{
					{JobID: "job id", JobIDUrl: "job id url", JobDescription: "job description", Time: time.Now(), TimeFormatted: "02-01-2006 15:04:05", JobStep: "job step"},
				},
			},
		},
	})
	Ok(t, err)
}

func TestLockTemplate(t *testing.T) {
	err := LockTemplate.Execute(io.Discard, LockDetailData{
		LockKeyEncoded:  "lock key encoded",
		LockKey:         "lock key",
		PullRequestLink: "https://example.com",
		LockedBy:        "locked by",
		Workspace:       "workspace",
		AtlantisVersion: "v0.0.0",
		CleanedBasePath: "/path",
		RepoOwner:       "repo owner",
		RepoName:        "repo name",
	})
	Ok(t, err)
}

func TestProjectJobsTemplate(t *testing.T) {
	err := ProjectJobsTemplate.Execute(io.Discard, ProjectJobData{
		AtlantisVersion: "v0.0.0",
		ProjectPath:     "project path",
		CleanedBasePath: "/path",
	})
	Ok(t, err)
}

func TestProjectJobsErrorTemplate(t *testing.T) {
	err := ProjectJobsTemplate.Execute(io.Discard, ProjectJobsError{
		AtlantisVersion: "v0.0.0",
		ProjectPath:     "project path",
		CleanedBasePath: "/path",
	})
	Ok(t, err)
}

func TestGithubAppSetupTemplate(t *testing.T) {
	err := GithubAppSetupTemplate.Execute(io.Discard, GithubSetupData{
		Target:          "target",
		Manifest:        "manifest",
		ID:              1,
		Key:             "key",
		WebhookSecret:   "webhook secret",
		URL:             "https://example.com",
		CleanedBasePath: "/path",
	})
	Ok(t, err)
}

func TestRunHistoryTemplates(t *testing.T) {
	run := RunHistoryRun{
		ID: "019c0000-0000-7000-8000-000000000000", Repository: "org/repo",
		PullNumber: 42, Command: "plan", Trigger: "comment", Actor: "operator",
		HeadSHA: "abc123", Status: "failed", CreatedAt: "2026-08-16 12:00:00 UTC",
		DetailPath: "/runs/019c0000-0000-7000-8000-000000000000",
	}
	project := RunHistoryProject{
		ID: "019c0000-0000-7000-8000-000000000001", ProjectName: "network",
		AttemptID: "019c0000-0000-7000-8000-000000000002",
		Directory: "terraform/network", Workspace: "production", Status: "failed",
		DetailPath: run.DetailPath + "/projects/019c0000-0000-7000-8000-000000000001",
	}
	attempt := RunHistoryAttempt{
		ID: "019c0000-0000-7000-8000-000000000002", Status: "unknown",
		InstanceID: "019c0000-0000-7000-8000-000000000003", ReplicaID: "atlantis-2",
		DeploymentID: "production", FailureReason: "unsafe <outcome>", CanReconcile: true,
		ReconcilePath: run.DetailPath + "/attempts/019c0000-0000-7000-8000-000000000002/reconcile",
		CSRFToken:     "token",
	}
	cases := []struct {
		name     string
		template TemplateWriter
		data     any
	}{
		{"list", RunHistoryListTemplate, RunHistoryListData{Title: "Run history", Runs: []RunHistoryRun{run}}},
		{"detail", RunHistoryDetailTemplate, RunHistoryDetailData{Run: run, Summary: runs.ProjectRunSummary{Total: 1, Failed: 1}, Projects: []RunHistoryProject{project}, Attempts: []RunHistoryAttempt{attempt}}},
		{"audit", RunAuditListTemplate, RunAuditListData{Events: []RunAuditEvent{{Repository: "org/repo", EventType: "plan.completed"}}}},
		{"drift list", DriftHistoryListTemplate, DriftHistoryListData{Projects: []DriftCurrentProject{{Repository: "org/repo", ProjectName: "network", Outcome: "drifted"}}}},
		{"drift detection", DriftDetectionTemplate, DriftDetectionDetailData{Detection: DriftDetectionRun{ID: run.ID, Repository: "org/repo", Status: "partial"}, Projects: []DriftDetectionProject{{ProjectName: "network", Error: "unsafe <value>"}}}},
		{"drift remediation", DriftRemediationTemplate, DriftRemediationDetailData{ID: run.ID, Repository: "org/repo", Status: "success", Projects: []DriftRemediationProject{{ProjectName: "network", Before: `{"summary":"unsafe <value>"}`}}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			Assert(t, test.template != nil, "history template should be parsed")
			Ok(t, test.template.Execute(io.Discard, test.data))
		})
	}

	var output bytes.Buffer
	err := RunProjectDetailTemplate.Execute(&output, RunProjectDetailData{
		Run: run, Project: project,
		Output: []RunHistoryOutputChunk{{Stream: "stdout", Content: "sensitive <value>\n"}},
	})
	Ok(t, err)
	Assert(t, strings.Contains(output.String(), "sensitive &lt;value&gt;"), "output must be HTML escaped")
	Assert(t, !strings.Contains(output.String(), "sensitive <value>"), "raw output must not be injected into HTML")

	output.Reset()
	err = RunHistoryDetailTemplate.Execute(&output, RunHistoryDetailData{Run: run, Attempts: []RunHistoryAttempt{attempt}})
	Ok(t, err)
	Assert(t, strings.Contains(output.String(), "unsafe &lt;outcome&gt;"), "attempt failures must be HTML escaped")
	Assert(t, !strings.Contains(output.String(), "unsafe <outcome>"), "raw attempt failures must not be injected into HTML")

	output.Reset()
	err = DriftDetectionTemplate.Execute(&output, DriftDetectionDetailData{
		Detection: DriftDetectionRun{ID: run.ID, Repository: "org/repo", Status: "failed"},
		Projects:  []DriftDetectionProject{{ProjectName: "network", Error: "sensitive <value>"}},
	})
	Ok(t, err)
	Assert(t, strings.Contains(output.String(), "sensitive &lt;value&gt;"), "drift errors must be HTML escaped")
	Assert(t, !strings.Contains(output.String(), "sensitive <value>"), "raw drift errors must not be injected into HTML")
}
