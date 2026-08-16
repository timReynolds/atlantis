// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"regexp"
	"testing"

	"github.com/runatlantis/atlantis/server/core/config/valid"
	"github.com/stretchr/testify/require"
)

func TestResolvedWorkflowIdentityIsStableAndConfigurationSensitive(t *testing.T) {
	cfg := valid.MergedProjectCfg{
		RepoCfgVersion: 3, Name: "network", RepoRelDir: "terraform/network", Workspace: "production",
		Workflow: valid.Workflow{Name: "terragrunt", Plan: valid.Stage{Steps: []valid.Step{{
			StepName: "run", RunCommand: "terragrunt plan", FilterRegexes: []*regexp.Regexp{regexp.MustCompile("token=.*")},
		}}}, Apply: valid.Stage{Steps: []valid.Step{{StepName: "apply"}}}},
		ApplyRequirements: []string{"approved"}, RepoLocks: valid.RepoLocks{Mode: valid.RepoLocksOnApplyMode},
	}
	first := resolvedWorkflowIdentity(cfg)
	require.Equal(t, first, resolvedWorkflowIdentity(cfg))
	require.Regexp(t, `^sha256:[0-9a-f]{64}$`, first)

	changed := cfg
	changed.Workflow.Apply.Steps = []valid.Step{{StepName: "run", RunCommand: "terragrunt apply"}}
	require.NotEqual(t, first, resolvedWorkflowIdentity(changed))
	changed = cfg
	changed.ApplyRequirements = []string{"approved", "mergeable"}
	require.NotEqual(t, first, resolvedWorkflowIdentity(changed))
}
