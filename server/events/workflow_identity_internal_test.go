// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"regexp"
	"testing"

	"github.com/hashicorp/go-version"
	"github.com/runatlantis/atlantis/server/core/config/valid"
	"github.com/runatlantis/atlantis/server/core/terraform"
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

func TestResolvedWorkflowIdentityIsSensitiveToPolicyCheckEnablement(t *testing.T) {
	cfg := valid.MergedProjectCfg{
		Name: "network", RepoRelDir: "network", Workspace: "default",
		Workflow: valid.Workflow{Name: "default"},
	}
	disabled := resolvedWorkflowIdentity(cfg)

	policyEnabled := cfg
	policyEnabled.PolicyCheck = true
	require.NotEqual(t, disabled, resolvedWorkflowIdentity(policyEnabled),
		"enabling policy_check with the same commit and workflow must change the identity")

	customPolicyEnabled := cfg
	customPolicyEnabled.CustomPolicyCheck = true
	require.NotEqual(t, disabled, resolvedWorkflowIdentity(customPolicyEnabled),
		"enabling custom_policy_check must change the identity")
	require.NotEqual(t, resolvedWorkflowIdentity(policyEnabled), resolvedWorkflowIdentity(customPolicyEnabled))
}

func TestResolvedWorkflowIdentityIncludesEffectiveTerraformDefaults(t *testing.T) {
	cfg := valid.MergedProjectCfg{
		Name: "network", RepoRelDir: "network", Workspace: "default",
		Workflow: valid.Workflow{Name: "default"},
	}
	terraformVersion, err := version.NewVersion("1.11.1")
	require.NoError(t, err)
	applyTerraformDefaults(&cfg, terraform.NewDistributionTerraform(), terraformVersion)
	terraformIdentity := resolvedWorkflowIdentity(cfg)

	opentofuVersion, err := version.NewVersion("1.9.0")
	require.NoError(t, err)
	cfg.TerraformDistribution = nil
	cfg.TerraformVersion = nil
	applyTerraformDefaults(&cfg, terraform.NewDistributionOpenTofu(), opentofuVersion)

	require.NotEqual(t, terraformIdentity, resolvedWorkflowIdentity(cfg))
	require.Equal(t, "opentofu", *cfg.TerraformDistribution)
	require.Equal(t, "1.9.0", cfg.TerraformVersion.String())
}
