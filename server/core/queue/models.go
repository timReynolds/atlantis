// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

// Package queue schedules pull requests while existing Atlantis runs execute projects.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
)

type State string

const (
	Queued              State = "QUEUED"
	Preparing           State = "PREPARING"
	Planning            State = "PLANNING"
	Ready               State = "READY"
	Applying            State = "APPLYING"
	Merging             State = "MERGING"
	Completed           State = "COMPLETED"
	Replanning          State = "REPLANNING"
	AwaitingApproval    State = "AWAITING_APPROVAL"
	Blocked             State = "BLOCKED"
	ApplyOutcomeUnknown State = "APPLY_OUTCOME_UNKNOWN"
	PartiallyApplied    State = "PARTIALLY_APPLIED"
	AppliedUnmerged     State = "APPLIED_UNMERGED"
	RecoveryRequired    State = "RECOVERY_REQUIRED"
	Cancelled           State = "CANCELLED"
)

var (
	ErrConflict = errors.New("queue generation or claim changed")
	ErrNotFound = errors.New("queue entry not found")
	ErrReserved = errors.New("terraform state is reserved by another queue entry")
	ErrReplan   = errors.New("plan preconditions changed")
)

// Entry contains scheduling state, not Terraform credentials or raw plan JSON.
// Plans contain digests and summaries; potentially sensitive values stay in S3.
type Entry struct {
	ReportedVersion int64     `json:"reported_version,omitempty"`
	Position        int       `json:"position,omitempty"`
	ID              runs.ID   `json:"id"`
	Repository      string    `json:"repository"`
	PullNumber      int       `json:"pull_number"`
	TargetBranch    string    `json:"target_branch"`
	HeadSHA         string    `json:"head_sha"`
	Actor           string    `json:"actor"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	State           State     `json:"state"`
	Generation      int64     `json:"generation"`
	Version         int64     `json:"version"`
	Worker          string    `json:"worker,omitempty"`
	Claim           runs.ID   `json:"claim,omitempty"`
	HeartbeatAt     time.Time `json:"heartbeat_at,omitempty"`
	RetryAt         time.Time `json:"retry_at,omitempty"`
	RetryCount      int       `json:"retry_count"`
	Reason          string    `json:"reason,omitempty"`
	Checkpoint      string    `json:"checkpoint,omitempty"`
	CommentID       int64     `json:"comment_id,omitempty"`
	// Source is the credential-free repository reference needed to hydrate a restart.
	Source             json.RawMessage `json:"source"`
	Prepared           PreparedPlan    `json:"prepared"`
	Plans              []Plan          `json:"plans,omitempty"`
	ApprovedDigest     string          `json:"approved_digest,omitempty"`
	ApprovedGeneration int64           `json:"approved_generation,omitempty"`
	ApprovedBy         string          `json:"approved_by,omitempty"`
	ApplyStarted       bool            `json:"apply_started"`
	Merge              MergeAttempt    `json:"merge"`
}

type PreparedPlan struct {
	BaseSHA            string    `json:"base_sha"`
	HeadSHA            string    `json:"head_sha"`
	IntegrationSHA     string    `json:"integration_sha"`
	TreeSHA            string    `json:"tree_sha"`
	Configuration      string    `json:"configuration"`
	ArtifactGeneration runs.ID   `json:"artifact_generation"`
	Projects           []Project `json:"projects"`
}

type Project struct {
	Name      string   `json:"name"`
	Directory string   `json:"directory"`
	Workspace string   `json:"workspace"`
	BackendID string   `json:"backend_id"`
	StateID   string   `json:"state_id"`
	Workflow  string   `json:"workflow"`
	Inputs    string   `json:"inputs"`
	DependsOn []string `json:"depends_on,omitempty"`
}

func (p Project) Key() string { return p.Directory + "\x00" + p.Workspace + "\x00" + p.Name }

type StateVersion struct {
	Lineage  string `json:"lineage"`
	Serial   uint64 `json:"serial"`
	Checksum string `json:"checksum"`
}

type Plan struct {
	Project        Project                      `json:"project"`
	Artifact       runs.PlanArtifactExpectation `json:"artifact"`
	State          StateVersion                 `json:"state"`
	SemanticDigest string                       `json:"semantic_digest"`
	Summary        string                       `json:"summary"`
	Destructive    bool                         `json:"destructive"`
	ApplyIntent    bool                         `json:"apply_intent"`
	Applied        bool                         `json:"applied"`
	ApplyRunID     runs.ID                      `json:"apply_run_id,omitempty"`
	ApplyAttemptID runs.ID                      `json:"apply_attempt_id,omitempty"`
	ResultState    *StateVersion                `json:"result_state,omitempty"`
	Error          string                       `json:"error,omitempty"`
}

type MergeAttempt struct {
	ID              runs.ID    `json:"id,omitempty"`
	ExpectedHeadSHA string     `json:"expected_head_sha,omitempty"`
	IntegrationSHA  string     `json:"integration_sha,omitempty"`
	ResultSHA       string     `json:"result_sha,omitempty"`
	ObservedAt      *time.Time `json:"observed_at,omitempty"`
}

type Event struct {
	ID         runs.ID         `json:"id"`
	EntryID    runs.ID         `json:"entry_id"`
	Generation int64           `json:"generation"`
	AttemptID  runs.ID         `json:"attempt_id,omitempty"`
	Type       string          `json:"type"`
	Actor      string          `json:"actor"`
	CreatedAt  time.Time       `json:"created_at"`
	Payload    json.RawMessage `json:"payload"`
}

type Request struct {
	ApprovedDigest string
	ApprovedBy     string
	Repository     string
	PullNumber     int
	TargetBranch   string
	HeadSHA        string
	Actor          string
	DeliveryID     string
	Source         json.RawMessage
}

// Store operations atomically persist state and its audit event. Claims and
// transitions are fenced; state reservations are never released by lease expiry.
type Store interface {
	Enqueue(context.Context, Request) (Entry, error)
	Get(context.Context, runs.ID) (Entry, error)
	Find(context.Context, string, int) (Entry, error)
	List(context.Context, int) ([]Entry, error)
	Events(context.Context, runs.ID, int) ([]Event, error)
	ClaimNext(context.Context, string, time.Duration) (Entry, error)
	Heartbeat(context.Context, Entry) error
	CheckClaim(context.Context, Entry) error
	RecoverStale(context.Context, time.Duration) ([]Entry, error)
	Save(context.Context, Entry, Entry, string) (Entry, error)
	Reserve(context.Context, Entry, []string) error
	Action(context.Context, runs.ID, int64, string, string, string, string) (Entry, error)
	SetComment(context.Context, runs.ID, int64) error
	PendingReports(context.Context, int) ([]Entry, error)
	MarkReported(context.Context, runs.ID, int64) error
	LinkRun(context.Context, Entry, runs.ID, runs.ID) error
}

func (s State) Terminal() bool { return s == Completed || s == Cancelled }
func (s State) Recoverable() bool {
	return s == ApplyOutcomeUnknown || s == PartiallyApplied || s == AppliedUnmerged || s == RecoveryRequired
}
func (s State) Runnable() bool    { return s == Queued || s == Replanning || s == Ready || s == Merging }
func (e Entry) HoldsBranch() bool { return e.Worker != "" || (e.ApplyStarted && !e.State.Terminal()) }

func ValidateTransition(from, to State) error {
	allowed := map[State][]State{
		Queued:              {Preparing, Cancelled, Blocked},
		Preparing:           {Planning, Blocked, Queued, Cancelled, Replanning},
		Planning:            {Ready, AwaitingApproval, Blocked, Replanning, Queued, Cancelled},
		Ready:               {Applying, Replanning, Blocked, Cancelled, Queued},
		Applying:            {Applying, Merging, ApplyOutcomeUnknown, PartiallyApplied, RecoveryRequired},
		Merging:             {Completed, AppliedUnmerged, RecoveryRequired},
		Replanning:          {Preparing, Queued, Blocked, Cancelled},
		AwaitingApproval:    {Ready, Queued, Cancelled},
		Blocked:             {Queued, Cancelled},
		AppliedUnmerged:     {Merging, RecoveryRequired, Cancelled},
		ApplyOutcomeUnknown: {RecoveryRequired, Cancelled},
		PartiallyApplied:    {RecoveryRequired, Cancelled},
		RecoveryRequired:    {Cancelled},
	}
	if from == to {
		return nil
	}
	for _, state := range allowed[from] {
		if state == to {
			return nil
		}
	}
	return fmt.Errorf("invalid queue transition %s to %s", from, to)
}

func (r Request) Validate() error {
	if r.Repository == "" || r.PullNumber <= 0 || r.TargetBranch == "" || r.HeadSHA == "" || r.Actor == "" || !json.Valid(r.Source) {
		return errors.New("queue request requires repository, pull, branch, head, actor and source")
	}
	return nil
}

// MutationReservation covers ordinary commands that share queue-managed state.
type MutationReservation struct {
	StateID    string    `json:"state_id"`
	Claim      runs.ID   `json:"claim"`
	Actor      string    `json:"actor"`
	AcquiredAt time.Time `json:"acquired_at"`
}
type MutationRecovery interface {
	ListMutations(context.Context) ([]MutationReservation, error)
	ReconcileMutation(context.Context, runs.ID, string, string) error
}
