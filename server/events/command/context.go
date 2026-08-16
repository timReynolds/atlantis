// Copyright 2025 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"

	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/logging"
	tally "github.com/uber-go/tally/v4"
)

// ExecutionLease fences the start of routed command work against its current
// distributed ownership claim.
type ExecutionLease interface {
	Admit(context.Context) error
}

// SideEffectMarker durably records that a command is crossing from retryable
// preparation into infrastructure mutation. Implementations must fail closed.
type SideEffectMarker interface {
	MarkSideEffectStarted(context.Context) error
}

// RoutingContext carries owner-local execution state through command runners.
type RoutingContext struct {
	Lease                ExecutionLease
	RecoverExternalPlans bool
	InstanceID           runs.ID
	DeploymentID         string
	ConcurrencyKey       string
	OwnershipClaimID     string
}

// Trigger represents the how the command was triggered
type Trigger int

const (
	// Commands that are automatically triggered (ie. automatic plans)
	AutoTrigger Trigger = iota

	// Commands that are triggered by comments (ie. atlantis plan)
	CommentTrigger
)

// Context represents the context of a command that should be executed
// for a pull request.
type Context struct {
	// RunID links this command to optional durable run history. It is empty
	// when durable history is disabled or the command is outside its scope.
	RunID runs.ID
	// AttemptID identifies this process attempt when durable HA is enabled.
	AttemptID runs.ID
	// HeadRepo is the repository that is getting merged into the BaseRepo.
	// If the pull request branch is from the same repository then HeadRepo will
	// be the same as BaseRepo.
	// See https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/incorporating-changes-from-a-pull-request/about-pull-request-merges
	HeadRepo models.Repo
	Pull     models.PullRequest
	Scope    tally.Scope
	// User is the user that triggered this command.
	User models.User
	Log  logging.SimpleLogging

	// Current PR state
	PullRequestStatus models.PullReqStatus

	PullStatus *models.PullStatus

	// PolicySet is the policy set to target (if specified) for the approve_policies command.
	PolicySet string

	// ClearPolicyApproval is true if approval should be cleared on specified policies.
	ClearPolicyApproval bool

	Trigger Trigger

	// API is true if plan/apply by API endpoints
	API bool

	// SkipPRRequirements allows explicitly opted-in API workflows that are not
	// tied to a pull request to skip PR-only requirements like approved and mergeable.
	SkipPRRequirements bool

	// SkipPRModifiedFiles allows synthetic non-PR API workflows to resolve
	// explicit selectors without querying pull request modified files.
	SkipPRModifiedFiles bool

	// SuppressVCSStatus prevents API workflows such as drift detection from
	// publishing normal PR lifecycle commit statuses.
	SuppressVCSStatus bool

	// SuppressJobOutput prevents API workflows such as drift detection from
	// publishing raw command output to the public job stream.
	SuppressJobOutput bool

	// SuppressApplyWebhooks prevents synthetic API workflows such as drift
	// remediation from sending legacy event: apply webhooks.
	SuppressApplyWebhooks bool

	// RunPolicyChecks allows API workflows that model the full plan lifecycle
	// to execute generated policy_check contexts after successful plan contexts.
	RunPolicyChecks bool

	// FailOnTeamAllowlistDenied makes project selection return an error when
	// any selected project is denied by team allowlist filtering.
	FailOnTeamAllowlistDenied bool

	// FailOnMissingDependencies makes apply dependency validation fail when a
	// dependency is absent from PullStatus. Drift remediation uses this to avoid
	// applying a selected subset without its configured dependencies.
	FailOnMissingDependencies bool

	// ExactProjectNameMatching treats API project selectors as exact project
	// identities even when regex command selection is enabled.
	ExactProjectNameMatching bool

	// SortByExecutionOrder sorts API-selected project commands by configured
	// execution order.
	SortByExecutionOrder bool

	// PreWorkflowHooksAlreadyRun is set when an API workflow has already run
	// pre-workflow hooks before project discovery.
	PreWorkflowHooksAlreadyRun bool

	// TeamAllowlistChecker is used to check authorization on a project-level
	TeamAllowlistChecker TeamAllowlistChecker

	// Set true if there were any errors during the command execution
	CommandHasErrors bool

	// Set true if the command was intentionally skipped without executing work.
	CommandSkipped bool

	// CommandOutcomeSkipped records a skipped durable outcome without changing
	// workflow-hook dispatch. Use CommandSkipped only when the command pipeline
	// itself must stop before post-workflow hooks.
	CommandOutcomeSkipped bool

	// Set true when distributed ownership changed while this command was
	// queued or executing. Superseded generations must not publish results.
	CommandSuperseded bool

	// PreferLocalRepoCfgForTargetedIgnore makes targeted ignore checks read a
	// cloned repo config before falling back to VCS content. This is used after
	// pre-workflow hooks may have generated or updated atlantis.yaml.
	PreferLocalRepoCfgForTargetedIgnore bool

	// ExecutionLease is present only for commands accepted through replica routing.
	ExecutionLease ExecutionLease

	// RecoverExternalPlans marks a newly prepared local ownership generation.
	RecoverExternalPlans bool

	// HA execution identity is populated only for owner-routed commands.
	ExecutionInstanceID   runs.ID
	ExecutionDeploymentID string
	ConcurrencyKey        string
	OwnershipClaimID      string
	SideEffectMarker      SideEffectMarker
}
