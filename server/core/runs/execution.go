// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package runs

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// AttemptStatus is the lifecycle state of one process attempt to execute a
// logical Run. It is intentionally distinct from the aggregate Run status.
type AttemptStatus string

const (
	AttemptClaimed     AttemptStatus = "claimed"
	AttemptRunning     AttemptStatus = "running"
	AttemptSucceeded   AttemptStatus = "succeeded"
	AttemptFailed      AttemptStatus = "failed"
	AttemptInterrupted AttemptStatus = "interrupted"
	AttemptUnknown     AttemptStatus = "unknown"
)

// ExecutionInstance identifies one Atlantis process for its lifetime. A
// replica ID may be reused after restart; an instance ID must not be.
type ExecutionInstance struct {
	ID           ID
	ReplicaID    string
	DeploymentID string
	AdvertiseURL string
	StartedAt    time.Time
	HeartbeatAt  time.Time
	StoppedAt    *time.Time
	Version      string
	Commit       string
	Metadata     Metadata
}

// Validate checks durable process identity invariants.
func (i ExecutionInstance) Validate() error {
	if _, err := ParseID(string(i.ID)); err != nil {
		return fmt.Errorf("validating execution instance ID: %w", err)
	}
	if strings.TrimSpace(i.ReplicaID) == "" {
		return fmt.Errorf("replica ID is required")
	}
	if strings.TrimSpace(i.DeploymentID) == "" {
		return fmt.Errorf("deployment ID is required")
	}
	if i.StartedAt.IsZero() {
		return fmt.Errorf("instance start time is required")
	}
	if i.HeartbeatAt.IsZero() || i.HeartbeatAt.Before(i.StartedAt) {
		return fmt.Errorf("instance heartbeat must not precede its start")
	}
	if i.StoppedAt != nil && (i.StoppedAt.IsZero() || i.StoppedAt.Before(i.StartedAt)) {
		return fmt.Errorf("instance stop time must not precede its start")
	}
	return validateMetadata(i.Metadata)
}

// RunAttempt records one Atlantis process's attempt to execute a logical Run.
// OwnershipClaimID is the Redis claim generation used for admission; it is not
// an externally meaningful run identity or a claim of exactly-once execution.
type RunAttempt struct {
	ID                    ID
	RunID                 ID
	InstanceID            ID
	DeploymentID          string
	ConcurrencyKey        string
	OwnershipClaimID      string
	Status                AttemptStatus
	ClaimedAt             time.Time
	StartedAt             *time.Time
	HeartbeatAt           time.Time
	SideEffectStartedAt   *time.Time
	CompletedAt           *time.Time
	FailureReason         string
	ReconciledAt          *time.Time
	ReconciledBy          string
	ReconciliationSummary string
	Metadata              Metadata
}

// Validate checks invariants shared by every execution-attempt adapter.
func (a RunAttempt) Validate() error {
	if _, err := ParseID(string(a.ID)); err != nil {
		return fmt.Errorf("validating run attempt ID: %w", err)
	}
	if _, err := ParseID(string(a.RunID)); err != nil {
		return fmt.Errorf("validating attempt run ID: %w", err)
	}
	if _, err := ParseID(string(a.InstanceID)); err != nil {
		return fmt.Errorf("validating attempt instance ID: %w", err)
	}
	if strings.TrimSpace(a.DeploymentID) == "" {
		return fmt.Errorf("attempt deployment ID is required")
	}
	if strings.TrimSpace(a.ConcurrencyKey) == "" {
		return fmt.Errorf("attempt concurrency key is required")
	}
	if strings.ContainsRune(a.ConcurrencyKey, '\x00') {
		return fmt.Errorf("attempt concurrency key cannot contain NUL bytes")
	}
	if !a.Status.valid() {
		return fmt.Errorf("invalid attempt status %q", a.Status)
	}
	if a.ClaimedAt.IsZero() {
		return fmt.Errorf("attempt claimed time is required")
	}
	if a.HeartbeatAt.IsZero() || a.HeartbeatAt.Before(a.ClaimedAt) {
		return fmt.Errorf("attempt heartbeat must not precede its claim")
	}
	if err := validateAttemptLifecycle(a); err != nil {
		return err
	}
	if err := validateAttemptFailureReason(a.Status, a.FailureReason); err != nil {
		return err
	}
	if err := validateAttemptReconciliation(a); err != nil {
		return err
	}
	return validateMetadata(a.Metadata)
}

// AttemptCompletion contains the fields written when an attempt ends.
type AttemptCompletion struct {
	ID            ID
	Status        AttemptStatus
	CompletedAt   time.Time
	FailureReason string
}

// Validate checks terminal attempt completion invariants.
func (c AttemptCompletion) Validate() error {
	if _, err := ParseID(string(c.ID)); err != nil {
		return fmt.Errorf("validating run attempt ID: %w", err)
	}
	if !c.Status.terminal() {
		return fmt.Errorf("attempt completion requires a terminal status")
	}
	if c.CompletedAt.IsZero() {
		return fmt.Errorf("attempt completion time is required")
	}
	return validateAttemptFailureReason(c.Status, c.FailureReason)
}

// AttemptReconciliation records an operator's resolution of an unknown apply.
// The attempt remains unknown history; reconciliation is an explicit overlay.
type AttemptReconciliation struct {
	ID           ID
	AuditEventID ID
	At           time.Time
	Actor        string
	Summary      string
}

// Validate checks reconciliation audit fields.
func (r AttemptReconciliation) Validate() error {
	if _, err := ParseID(string(r.ID)); err != nil {
		return fmt.Errorf("validating run attempt ID: %w", err)
	}
	if _, err := ParseID(string(r.AuditEventID)); err != nil {
		return fmt.Errorf("validating reconciliation audit event ID: %w", err)
	}
	if r.At.IsZero() {
		return fmt.Errorf("reconciliation time is required")
	}
	if strings.TrimSpace(r.Actor) == "" {
		return fmt.Errorf("reconciliation actor is required")
	}
	if strings.TrimSpace(r.Summary) == "" {
		return fmt.Errorf("reconciliation summary is required")
	}
	return nil
}

// AttemptPage is a bounded list of attempts ordered by claim time.
type AttemptPage struct {
	Attempts   []RunAttempt
	NextCursor string
}

// AttemptTakeoverRequest asks the durable store to classify any attempt left
// active under an older Redis ownership claim. Redis remains authoritative for
// live ownership; PostgreSQL supplies history and a second admission fence.
type AttemptTakeoverRequest struct {
	ConcurrencyKey   string
	OwnershipClaimID string
	HeartbeatBefore  time.Time
	RecoveredAt      time.Time
	Repository       string
	PullNumber       *int
	Command          Command
	Trigger          Trigger
	Actor            string
	BaseRef          string
	HeadRef          string
	HeadSHA          string
}

// Validate checks the fencing and logical-operation identity used for a
// takeover decision.
func (r AttemptTakeoverRequest) Validate() error {
	if strings.TrimSpace(r.ConcurrencyKey) == "" {
		return fmt.Errorf("takeover concurrency key is required")
	}
	if strings.ContainsRune(r.ConcurrencyKey, '\x00') {
		return fmt.Errorf("takeover concurrency key cannot contain NUL bytes")
	}
	if strings.TrimSpace(r.OwnershipClaimID) == "" {
		return fmt.Errorf("takeover ownership claim ID is required")
	}
	if r.HeartbeatBefore.IsZero() || r.RecoveredAt.IsZero() || r.RecoveredAt.Before(r.HeartbeatBefore) {
		return fmt.Errorf("takeover recovery window is invalid")
	}
	if strings.TrimSpace(r.Repository) == "" {
		return fmt.Errorf("takeover repository is required")
	}
	if r.PullNumber != nil && *r.PullNumber <= 0 {
		return fmt.Errorf("takeover pull number must be positive")
	}
	if r.PullNumber == nil && r.Trigger != TriggerAPI {
		return fmt.Errorf("takeover pull number is required outside API execution")
	}
	if !r.Command.valid() {
		return fmt.Errorf("invalid takeover command %q", r.Command)
	}
	if !r.Trigger.valid() {
		return fmt.Errorf("invalid takeover trigger %q", r.Trigger)
	}
	if strings.TrimSpace(r.HeadSHA) == "" {
		return fmt.Errorf("takeover head SHA is required")
	}
	return nil
}

// AttemptTakeoverResult reports a durable classification made before a new
// attempt is admitted. At most one ActiveAttempt or RetryRun is returned.
// UnreconciledUnknown is populated independently so mutating commands can be
// blocked until an operator resolves an earlier ambiguous apply.
type AttemptTakeoverResult struct {
	RecoveredAttempt    *RunAttempt
	ActiveAttempt       *RunAttempt
	RetryRun            *Run
	UnreconciledUnknown *RunAttempt
}

// ExecutionRecovery atomically classifies stale attempts before admission.
type ExecutionRecovery interface {
	PrepareAttemptTakeover(ctx context.Context, request AttemptTakeoverRequest) (AttemptTakeoverResult, error)
}

// ExecutionWriter is the execution-facing seam for instance and attempt state.
// Unlike Phase 1's observational Writer, HA callers may require these writes
// to succeed before starting side-effecting work.
type ExecutionWriter interface {
	RegisterInstance(ctx context.Context, instance ExecutionInstance) error
	HeartbeatInstance(ctx context.Context, id ID, heartbeatAt time.Time) error
	StopInstance(ctx context.Context, id ID, stoppedAt time.Time) error
	CreateAttempt(ctx context.Context, attempt RunAttempt) error
	StartAttempt(ctx context.Context, id ID, startedAt time.Time) error
	HeartbeatAttempt(ctx context.Context, id ID, heartbeatAt time.Time) error
	MarkAttemptSideEffectStarted(ctx context.Context, id ID, startedAt time.Time) error
	CompleteAttempt(ctx context.Context, completion AttemptCompletion) error
	ReconcileAttempt(ctx context.Context, reconciliation AttemptReconciliation) error
}

// ExecutionReader is the UI and recovery-facing seam for execution state.
type ExecutionReader interface {
	GetInstance(ctx context.Context, id ID) (ExecutionInstance, error)
	GetAttempt(ctx context.Context, id ID) (RunAttempt, error)
	ListRunAttempts(ctx context.Context, runID ID, page PageRequest) (AttemptPage, error)
}

// ExecutionStore is the complete execution identity and attempt seam.
type ExecutionStore interface {
	ExecutionWriter
	ExecutionReader
	ExecutionRecovery
}

func validateAttemptLifecycle(a RunAttempt) error {
	if a.StartedAt != nil && (a.StartedAt.IsZero() || a.StartedAt.Before(a.ClaimedAt)) {
		return fmt.Errorf("attempt start must not precede its claim")
	}
	if a.SideEffectStartedAt != nil {
		if a.StartedAt == nil || a.SideEffectStartedAt.IsZero() || a.SideEffectStartedAt.Before(*a.StartedAt) {
			return fmt.Errorf("attempt side effect must not precede its start")
		}
	}
	if a.CompletedAt != nil {
		if a.CompletedAt.IsZero() || a.CompletedAt.Before(a.ClaimedAt) || (a.StartedAt != nil && a.CompletedAt.Before(*a.StartedAt)) {
			return fmt.Errorf("attempt completion must not precede its lifecycle")
		}
	}
	switch a.Status {
	case AttemptClaimed:
		if a.StartedAt != nil || a.SideEffectStartedAt != nil || a.CompletedAt != nil {
			return fmt.Errorf("claimed attempt cannot have execution timestamps")
		}
	case AttemptRunning:
		if a.StartedAt == nil || a.CompletedAt != nil {
			return fmt.Errorf("running attempt requires a start and no completion")
		}
	case AttemptSucceeded, AttemptFailed:
		if a.StartedAt == nil || a.CompletedAt == nil {
			return fmt.Errorf("completed attempt requires start and completion times")
		}
	case AttemptInterrupted:
		if a.CompletedAt == nil || a.SideEffectStartedAt != nil {
			return fmt.Errorf("interrupted attempt requires completion without started side effects")
		}
	case AttemptUnknown:
		if a.StartedAt == nil || a.SideEffectStartedAt == nil || a.CompletedAt == nil {
			return fmt.Errorf("unknown attempt requires start, side effect, and completion times")
		}
	}
	return nil
}

func validateAttemptReconciliation(a RunAttempt) error {
	values := 0
	if a.ReconciledAt != nil {
		values++
	}
	if strings.TrimSpace(a.ReconciledBy) != "" {
		values++
	}
	if strings.TrimSpace(a.ReconciliationSummary) != "" {
		values++
	}
	if values == 0 {
		return nil
	}
	if values != 3 || a.Status != AttemptUnknown || a.ReconciledAt.IsZero() {
		return fmt.Errorf("only unknown attempts may have complete reconciliation fields")
	}
	if a.CompletedAt == nil || a.ReconciledAt.Before(*a.CompletedAt) {
		return fmt.Errorf("attempt reconciliation must not precede completion")
	}
	return nil
}

func validateAttemptFailureReason(status AttemptStatus, reason string) error {
	hasReason := strings.TrimSpace(reason) != ""
	switch status {
	case AttemptFailed, AttemptInterrupted, AttemptUnknown:
		if !hasReason {
			return fmt.Errorf("attempt status %q requires a failure reason", status)
		}
	case AttemptClaimed, AttemptRunning, AttemptSucceeded:
		if hasReason {
			return fmt.Errorf("attempt status %q cannot have a failure reason", status)
		}
	}
	return nil
}

func (s AttemptStatus) valid() bool {
	return s == AttemptClaimed || s == AttemptRunning || s.terminal()
}

func (s AttemptStatus) terminal() bool {
	switch s {
	case AttemptSucceeded, AttemptFailed, AttemptInterrupted, AttemptUnknown:
		return true
	default:
		return false
	}
}
