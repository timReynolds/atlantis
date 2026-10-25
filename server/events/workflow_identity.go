// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/runatlantis/atlantis/server/core/config/valid"
)

type resolvedExecutionIdentity struct {
	RepoConfigVersion     int
	ProjectName           string
	Directory             string
	Workspace             string
	WorkflowName          string
	Plan                  []resolvedStepIdentity
	Apply                 []resolvedStepIdentity
	PolicyCheck           []resolvedStepIdentity
	Import                []resolvedStepIdentity
	StateRemove           []resolvedStepIdentity
	TerraformDistribution string
	TerraformVersion      string
	PlanRequirements      []string
	ApplyRequirements     []string
	ImportRequirements    []string
	DependsOn             []string
	RepoLocksMode         valid.RepoLocksMode
	PolicyVersion         string
	PolicySets            []valid.PolicySet
	PolicyOwners          valid.PolicyOwners
	PolicyApproveCount    int
	PolicyStickyApprovals bool
	PolicyItemRegex       string
	PolicyCheckEnabled    bool
	CustomPolicyCheck     bool
}

type resolvedStepIdentity struct {
	Name          string
	ExtraArgs     []string
	RunCommand    string
	Output        []valid.PostProcessRunOutputOption
	EnvVarName    string
	EnvVarValue   string
	Shell         string
	ShellArgs     []string
	FilterRegexes []string
}

func resolvedWorkflowIdentity(cfg valid.MergedProjectCfg) string {
	payload := resolvedExecutionIdentity{
		RepoConfigVersion: cfg.RepoCfgVersion, ProjectName: cfg.Name,
		Directory: cfg.RepoRelDir, Workspace: cfg.Workspace, WorkflowName: cfg.Workflow.Name,
		Plan: resolvedSteps(cfg.Workflow.Plan.Steps), Apply: resolvedSteps(cfg.Workflow.Apply.Steps),
		PolicyCheck: resolvedSteps(cfg.Workflow.PolicyCheck.Steps), Import: resolvedSteps(cfg.Workflow.Import.Steps),
		StateRemove:      resolvedSteps(cfg.Workflow.StateRm.Steps),
		PlanRequirements: cfg.PlanRequirements, ApplyRequirements: cfg.ApplyRequirements,
		ImportRequirements: cfg.ImportRequirements, DependsOn: cfg.DependsOn,
		RepoLocksMode: cfg.RepoLocks.Mode, PolicySets: cfg.PolicySets.PolicySets,
		PolicyOwners: cfg.PolicySets.Owners, PolicyApproveCount: cfg.PolicySets.ApproveCount,
		PolicyStickyApprovals: cfg.PolicySets.StickyApprovals, PolicyItemRegex: cfg.PolicySets.PolicyItemRegex,
		PolicyCheckEnabled: cfg.PolicyCheck, CustomPolicyCheck: cfg.CustomPolicyCheck,
	}
	if cfg.TerraformDistribution != nil {
		payload.TerraformDistribution = *cfg.TerraformDistribution
	}
	if cfg.TerraformVersion != nil {
		payload.TerraformVersion = cfg.TerraformVersion.String()
	}
	if cfg.PolicySets.Version != nil {
		payload.PolicyVersion = cfg.PolicySets.Version.String()
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		panic("resolved Atlantis execution identity contains an unsupported value: " + err.Error())
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func resolvedSteps(steps []valid.Step) []resolvedStepIdentity {
	result := make([]resolvedStepIdentity, 0, len(steps))
	for _, step := range steps {
		identity := resolvedStepIdentity{
			Name: step.StepName, ExtraArgs: step.ExtraArgs, RunCommand: step.RunCommand,
			Output: step.Output, EnvVarName: step.EnvVarName, EnvVarValue: step.EnvVarValue,
		}
		if step.RunShell != nil {
			identity.Shell = step.RunShell.Shell
			identity.ShellArgs = step.RunShell.ShellArgs
		}
		for _, expression := range step.FilterRegexes {
			if expression != nil {
				identity.FilterRegexes = append(identity.FilterRegexes, expression.String())
			}
		}
		result = append(result, identity)
	}
	return result
}
