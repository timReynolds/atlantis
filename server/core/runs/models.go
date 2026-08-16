// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package runs

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ID is a globally unique identifier for a durable history record.
type ID string

// NewID returns a time-sortable UUIDv7 identifier.
func NewID() (ID, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("generating UUIDv7: %w", err)
	}
	return ID(id.String()), nil
}

// ParseID validates and returns a durable history identifier.
func ParseID(value string) (ID, error) {
	id, err := uuid.Parse(value)
	if err != nil {
		return "", fmt.Errorf("parsing history identifier: %w", err)
	}
	if id.Version() != uuid.Version(7) || id.Variant() != uuid.RFC4122 {
		return "", fmt.Errorf("history identifier must be UUIDv7")
	}
	return ID(id.String()), nil
}

// Command is the logical Atlantis operation represented by a Run.
type Command string

const (
	CommandPlan             Command = "plan"
	CommandApply            Command = "apply"
	CommandImport           Command = "import"
	CommandStateRemove      Command = "state_remove"
	CommandUnlock           Command = "unlock"
	CommandDriftDetection   Command = "drift_detection"
	CommandDriftRemediation Command = "drift_remediation"
)

// Trigger describes how a Run was requested.
type Trigger string

const (
	TriggerAutoplan Trigger = "autoplan"
	TriggerComment  Trigger = "comment"
	TriggerAPI      Trigger = "api"
	TriggerSchedule Trigger = "schedule"
)

// Status is the lifecycle state of a Run or ProjectRun.
type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusSucceeded Status = "succeeded"
	StatusUnchanged Status = "unchanged"
	StatusFailed    Status = "failed"
	StatusPartial   Status = "partial"
	StatusCancelled Status = "cancelled"
	StatusSkipped   Status = "skipped"
	// StatusUnknown means infrastructure side effects may have occurred but the
	// executing process disappeared before Atlantis observed a result.
	StatusUnknown Status = "unknown"
)

// Metadata is an optional JSON object for information that is useful to
// operators but is not part of the stable run query model.
type Metadata json.RawMessage

// Run records one accepted logical Atlantis operation. PullNumber is nil for
// operations that do not originate from a pull request.
type Run struct {
	ID          ID
	Repository  string
	PullNumber  *int
	Command     Command
	Trigger     Trigger
	Actor       string
	BaseRef     string
	HeadRef     string
	HeadSHA     string
	Status      Status
	CreatedAt   time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
	Metadata    Metadata
}

// Validate checks the stable invariants shared by every Store adapter.
func (r Run) Validate() error {
	if _, err := ParseID(string(r.ID)); err != nil {
		return fmt.Errorf("validating run ID: %w", err)
	}
	if r.Repository == "" {
		return fmt.Errorf("repository is required")
	}
	if err := validatePullNumber(r.PullNumber); err != nil {
		return err
	}
	if !r.Command.valid() {
		return fmt.Errorf("unknown run command %q", r.Command)
	}
	if !r.Trigger.valid() {
		return fmt.Errorf("unknown run trigger %q", r.Trigger)
	}
	if !r.Status.validForRun() {
		return fmt.Errorf("invalid run status %q", r.Status)
	}
	if r.CreatedAt.IsZero() {
		return fmt.Errorf("created time is required")
	}
	if err := validateLifecycle(r.Status, r.CreatedAt, r.StartedAt, r.CompletedAt, false); err != nil {
		return fmt.Errorf("validating run lifecycle: %w", err)
	}
	return validateMetadata(r.Metadata)
}

// ArtifactReference describes an opaque plan artifact stored outside the
// RunStore. Content is never stored in this model.
type ArtifactReference struct {
	Key       string
	Checksum  string
	CreatedAt time.Time
	ExpiresAt *time.Time
}

// ProjectRun records one project's participation in a Run.
type ProjectRun struct {
	ID    ID
	RunID ID
	// AttemptID is set for HA execution so project results and output remain
	// attributable when one logical Run spans multiple process attempts.
	AttemptID    *ID
	ProjectName  string
	Directory    string
	Workspace    string
	Status       Status
	Additions    int
	Changes      int
	Destructions int
	Imports      int
	Forgets      int
	StartedAt    *time.Time
	CompletedAt  *time.Time
	ErrorSummary string
	PlanArtifact *ArtifactReference
	Metadata     Metadata
}

// Validate checks the stable invariants shared by every Store adapter.
func (p ProjectRun) Validate() error {
	if _, err := ParseID(string(p.ID)); err != nil {
		return fmt.Errorf("validating project run ID: %w", err)
	}
	if _, err := ParseID(string(p.RunID)); err != nil {
		return fmt.Errorf("validating parent run ID: %w", err)
	}
	if p.AttemptID != nil {
		if _, err := ParseID(string(*p.AttemptID)); err != nil {
			return fmt.Errorf("validating project attempt ID: %w", err)
		}
	}
	if p.Directory == "" {
		return fmt.Errorf("directory is required")
	}
	if p.Workspace == "" {
		return fmt.Errorf("workspace is required")
	}
	if !p.Status.validForProject() {
		return fmt.Errorf("invalid project run status %q", p.Status)
	}
	if err := validateLifecycle(p.Status, time.Time{}, p.StartedAt, p.CompletedAt, true); err != nil {
		return fmt.Errorf("validating project run lifecycle: %w", err)
	}
	for label, count := range map[string]int{
		"additions": p.Additions, "changes": p.Changes,
		"destructions": p.Destructions, "imports": p.Imports,
		"forgets": p.Forgets,
	} {
		if count < 0 {
			return fmt.Errorf("%s cannot be negative", label)
		}
	}
	if p.PlanArtifact != nil {
		if p.PlanArtifact.Key == "" {
			return fmt.Errorf("plan artifact key is required")
		}
		if p.PlanArtifact.CreatedAt.IsZero() {
			return fmt.Errorf("plan artifact created time is required")
		}
		if p.PlanArtifact.ExpiresAt != nil && p.PlanArtifact.ExpiresAt.Before(p.PlanArtifact.CreatedAt) {
			return fmt.Errorf("plan artifact expiry precedes its created time")
		}
	}
	return validateMetadata(p.Metadata)
}

// OutputStream identifies the source of a persisted output chunk.
type OutputStream string

const (
	OutputStdout OutputStream = "stdout"
	OutputStderr OutputStream = "stderr"
	OutputSystem OutputStream = "system"
)

// OutputChunk is an ordered, moderately sized block of project output. Store
// callers batch terminal lines into chunks before appending them.
type OutputChunk struct {
	ProjectRunID ID
	Sequence     int64
	Stream       OutputStream
	Content      string
	CreatedAt    time.Time
}

// Validate checks the stable invariants shared by every Store adapter.
func (o OutputChunk) Validate() error {
	if _, err := ParseID(string(o.ProjectRunID)); err != nil {
		return fmt.Errorf("validating project run ID: %w", err)
	}
	if o.Sequence < 0 {
		return fmt.Errorf("output sequence cannot be negative")
	}
	if !o.Stream.valid() {
		return fmt.Errorf("unknown output stream %q", o.Stream)
	}
	if o.Content == "" {
		return fmt.Errorf("output content is required")
	}
	if o.CreatedAt.IsZero() {
		return fmt.Errorf("output created time is required")
	}
	return nil
}

// AuditEvent records a significant operational or security event. It is an
// audit trail, not the primary run query model.
type AuditEvent struct {
	ID         ID
	Repository string
	PullNumber *int
	RunID      *ID
	Actor      string
	EventType  string
	Metadata   Metadata
	CreatedAt  time.Time
}

// Validate checks the stable invariants shared by every Store adapter.
func (a AuditEvent) Validate() error {
	if _, err := ParseID(string(a.ID)); err != nil {
		return fmt.Errorf("validating audit event ID: %w", err)
	}
	if a.RunID != nil {
		if _, err := ParseID(string(*a.RunID)); err != nil {
			return fmt.Errorf("validating audit event run ID: %w", err)
		}
	}
	if a.Repository == "" {
		return fmt.Errorf("repository is required")
	}
	if err := validatePullNumber(a.PullNumber); err != nil {
		return err
	}
	if a.EventType == "" {
		return fmt.Errorf("event type is required")
	}
	if a.CreatedAt.IsZero() {
		return fmt.Errorf("created time is required")
	}
	return validateMetadata(a.Metadata)
}

func validatePullNumber(number *int) error {
	if number != nil && *number <= 0 {
		return fmt.Errorf("pull number must be positive")
	}
	return nil
}

func (c Command) valid() bool {
	switch c {
	case CommandPlan, CommandApply, CommandImport, CommandStateRemove, CommandUnlock,
		CommandDriftDetection, CommandDriftRemediation:
		return true
	default:
		return false
	}
}

func (t Trigger) valid() bool {
	switch t {
	case TriggerAutoplan, TriggerComment, TriggerAPI, TriggerSchedule:
		return true
	default:
		return false
	}
}

func (s Status) validForRun() bool {
	switch s {
	case StatusPending, StatusRunning, StatusSucceeded, StatusFailed, StatusPartial,
		StatusCancelled, StatusSkipped, StatusUnknown:
		return true
	default:
		return false
	}
}

func validateLifecycle(status Status, createdAt time.Time, startedAt, completedAt *time.Time, allowUnstartedTerminal bool) error {
	switch status {
	case StatusPending:
		if startedAt != nil || completedAt != nil {
			return fmt.Errorf("pending record cannot have started or completed times")
		}
		return nil
	case StatusRunning:
		if startedAt == nil || startedAt.IsZero() {
			return fmt.Errorf("running record requires a started time")
		}
		if completedAt != nil {
			return fmt.Errorf("running record cannot have a completed time")
		}
	default:
		if completedAt == nil || completedAt.IsZero() {
			return fmt.Errorf("terminal record requires a completed time")
		}
		if startedAt == nil && !(allowUnstartedTerminal && (status == StatusSkipped || status == StatusCancelled)) {
			return fmt.Errorf("terminal record requires a started time")
		}
	}
	if startedAt != nil {
		if !createdAt.IsZero() && startedAt.Before(createdAt) {
			return fmt.Errorf("started time precedes created time")
		}
		if completedAt != nil && completedAt.Before(*startedAt) {
			return fmt.Errorf("completed time precedes started time")
		}
	}
	return nil
}

func (s Status) validForProject() bool {
	switch s {
	case StatusPending, StatusRunning, StatusSucceeded, StatusUnchanged, StatusFailed,
		StatusPartial, StatusCancelled, StatusSkipped, StatusUnknown:
		return true
	default:
		return false
	}
}

func (s OutputStream) valid() bool {
	switch s {
	case OutputStdout, OutputStderr, OutputSystem:
		return true
	default:
		return false
	}
}

func validateMetadata(metadata Metadata) error {
	if len(metadata) == 0 {
		return nil
	}
	if !json.Valid(metadata) {
		return fmt.Errorf("metadata is not valid JSON")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(metadata, &object); err != nil {
		return fmt.Errorf("metadata must be a JSON object: %w", err)
	}
	if object == nil {
		return fmt.Errorf("metadata must be a JSON object")
	}
	return nil
}
