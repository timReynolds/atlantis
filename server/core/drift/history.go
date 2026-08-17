// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package drift

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/runatlantis/atlantis/server/events/models"
)

// ErrHistoryNotFound is returned for an unknown durable drift record.
var ErrHistoryNotFound = errors.New("drift history record not found")

// DetectionStatus is the aggregate outcome of one drift detection request.
type DetectionStatus string

const (
	DetectionStatusSucceeded DetectionStatus = "succeeded"
	DetectionStatusPartial   DetectionStatus = "partial"
	DetectionStatusFailed    DetectionStatus = "failed"
)

// DetectionOutcome is one project's outcome in append-only drift history.
type DetectionOutcome string

const (
	DetectionOutcomeClean   DetectionOutcome = "clean"
	DetectionOutcomeDrifted DetectionOutcome = "drifted"
	DetectionOutcomeFailed  DetectionOutcome = "failed"
	DetectionOutcomeLocked  DetectionOutcome = "locked"
	DetectionOutcomeSkipped DetectionOutcome = "skipped"
)

// DetectionRun records one immutable drift detection request.
type DetectionRun struct {
	ID                string
	RunID             string
	Repository        string
	DisplayRepository string
	Ref               string
	BaseBranch        string
	ResolvedCommit    string
	Status            DetectionStatus
	StartedAt         time.Time
	CompletedAt       time.Time
	TotalProjects     int
	ProjectsWithDrift int
	FailedProjects    int
	LockedProjects    int
	SkippedProjects   int
}

// DetectionProject records one immutable project result within a detection.
type DetectionProject struct {
	DetectionID      string
	Ordinal          int
	Project          models.ProjectDrift
	Outcome          DetectionOutcome
	ArtifactKey      string
	ArtifactChecksum string
}

// DetectionRecord is written atomically after a detection request completes.
type DetectionRecord struct {
	Run      DetectionRun
	Projects []DetectionProject
}

// CurrentStatusFilter selects the authoritative latest drift status rows.
type CurrentStatusFilter struct {
	Repository string
	Ref        string
	Outcome    DetectionOutcome
	Project    string
	Directory  string
	Workspace  string
}

// CurrentProjectStatus combines latest drift status with the most recent
// remediation attempt for the same project identity.
type CurrentProjectStatus struct {
	Repository            string
	Project               models.ProjectDrift
	Outcome               DetectionOutcome
	LastRemediationID     string
	LastRemediationStatus models.RemediationStatus
	LastRemediationAt     *time.Time
}

// HistoryPageRequest asks for a bounded offset page.
type HistoryPageRequest struct {
	Limit  int
	Offset int
}

// CurrentStatusPage is a bounded page of latest project status.
type CurrentStatusPage struct {
	Projects []CurrentProjectStatus
	HasMore  bool
}

// DetectionPage is a bounded page of detection runs or project results.
type DetectionPage struct {
	Runs     []DetectionRun
	Projects []DetectionProject
	HasMore  bool
}

// HistoryWriter persists append-only drift detection history.
type HistoryWriter interface {
	RecordDetection(ctx context.Context, record DetectionRecord) error
}

// AtomicDetectionWriter persists latest project status and immutable detection
// history in one transaction. Reconcile removes older latest-state rows for
// projects absent from a successful full detection of the same ref.
type AtomicDetectionWriter interface {
	RecordDetectionWithLatest(ctx context.Context, repository string, record DetectionRecord, reconcile bool) error
}

// HistoryReader serves read-only drift UI queries.
type HistoryReader interface {
	ListCurrentStatus(ctx context.Context, filter CurrentStatusFilter, page HistoryPageRequest) (CurrentStatusPage, error)
	ListDetections(ctx context.Context, repository string, page HistoryPageRequest) (DetectionPage, error)
	GetDetection(ctx context.Context, id string, page HistoryPageRequest) (DetectionRun, DetectionPage, error)
}

// HistoryStore is the complete durable drift history seam.
type HistoryStore interface {
	HistoryWriter
	HistoryReader
}

// OutcomeForProject classifies an existing drift result without changing the
// authoritative API model. Lock and skip states are retained explicitly in
// append-only history when the current execution surface reports them as text.
func OutcomeForProject(project models.ProjectDrift) DetectionOutcome {
	if project.Error != "" {
		errorText := strings.ToLower(project.Error)
		if strings.Contains(errorText, " lock") || strings.Contains(errorText, "locked") || strings.HasPrefix(errorText, "lock") {
			return DetectionOutcomeLocked
		}
		if strings.Contains(errorText, "skip") {
			return DetectionOutcomeSkipped
		}
		return DetectionOutcomeFailed
	}
	if project.Drift.HasDrift {
		return DetectionOutcomeDrifted
	}
	return DetectionOutcomeClean
}
