// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package runs_test

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/runatlantis/atlantis/server/core/runs"
	. "github.com/runatlantis/atlantis/testing"
)

func TestNewIDReturnsUUIDv7(t *testing.T) {
	id, err := runs.NewID()
	Ok(t, err)

	parsed, err := uuid.Parse(string(id))
	Ok(t, err)
	Equals(t, uuid.Version(7), parsed.Version())
}

func TestParseIDRequiresUUIDv7AndReturnsCanonicalForm(t *testing.T) {
	id, err := runs.NewID()
	Ok(t, err)

	parsed, err := runs.ParseID(strings.ToUpper(string(id)))
	Ok(t, err)
	Equals(t, id, parsed)

	_, err = runs.ParseID(uuid.Nil.String())
	Assert(t, err != nil, "nil UUID must be rejected")
	_, err = runs.ParseID(uuid.New().String())
	Assert(t, err != nil, "UUIDv4 must be rejected")

	nonRFC := uuid.MustParse(string(id))
	nonRFC[8] &= 0x3f
	Equals(t, uuid.Reserved, nonRFC.Variant())
	_, err = runs.ParseID(nonRFC.String())
	Assert(t, err != nil, "non-RFC UUIDv7 must be rejected")
}

func TestRunValidate(t *testing.T) {
	started := time.Date(2026, time.August, 16, 10, 0, 0, 0, time.UTC)
	id, err := runs.NewID()
	Ok(t, err)

	run := runs.Run{
		ID:         id,
		Repository: "runatlantis/atlantis",
		Command:    runs.CommandPlan,
		Trigger:    runs.TriggerComment,
		Actor:      "user",
		Status:     runs.StatusRunning,
		CreatedAt:  started,
		StartedAt:  &started,
		Metadata:   runs.Metadata(`{"source":"comment"}`),
	}
	Ok(t, run.Validate())
	invalidPull := 0
	run.PullNumber = &invalidPull
	Assert(t, run.Validate() != nil, "nonpositive run pull number must be rejected")
	run.PullNumber = nil

	run.Status = runs.StatusSucceeded
	Assert(t, run.Validate() != nil, "completed run without completed time must be rejected")

	completed := started.Add(time.Minute)
	run.CompletedAt = &completed
	Ok(t, run.Validate())
	run.Status = runs.StatusUnknown
	Ok(t, run.Validate())

	run.Metadata = runs.Metadata(`[]`)
	Assert(t, run.Validate() != nil, "metadata arrays must be rejected")
}

func TestAuditEventValidateRejectsNonpositivePullNumber(t *testing.T) {
	id, err := runs.NewID()
	Ok(t, err)
	invalidPull := -1
	event := runs.AuditEvent{
		ID: id, Repository: "runatlantis/atlantis", PullNumber: &invalidPull,
		EventType: "plan.requested", CreatedAt: time.Now(),
	}

	Assert(t, event.Validate() != nil, "nonpositive audit pull number must be rejected")
	event.PullNumber = nil
	Ok(t, event.Validate())
}

func TestProjectRunValidatePreservesAllPlanCounts(t *testing.T) {
	started := time.Date(2026, time.August, 16, 10, 0, 0, 0, time.UTC)
	completed := started.Add(time.Minute)
	runID, err := runs.NewID()
	Ok(t, err)
	projectRunID, err := runs.NewID()
	Ok(t, err)

	projectRun := runs.ProjectRun{
		ID:           projectRunID,
		RunID:        runID,
		Directory:    "terraform/network",
		Workspace:    "production",
		Status:       runs.StatusSucceeded,
		Additions:    1,
		Changes:      2,
		Destructions: 3,
		Imports:      4,
		Forgets:      5,
		StartedAt:    &started,
		CompletedAt:  &completed,
	}
	Ok(t, projectRun.Validate())

	projectRun.Forgets = -1
	Assert(t, projectRun.Validate() != nil, "negative plan counts must be rejected")
	projectRun.Forgets = 0
	projectRun.Status = runs.StatusUnknown
	Ok(t, projectRun.Validate())
}

func TestPendingProjectRunCanBeRecordedBeforeFanOut(t *testing.T) {
	runID, err := runs.NewID()
	Ok(t, err)
	projectRunID, err := runs.NewID()
	Ok(t, err)

	projectRun := runs.ProjectRun{
		ID:        projectRunID,
		RunID:     runID,
		Directory: ".",
		Workspace: "default",
		Status:    runs.StatusPending,
	}
	Ok(t, projectRun.Validate())

	completed := time.Now()
	projectRun.Status = runs.StatusSkipped
	projectRun.CompletedAt = &completed
	Ok(t, projectRun.Validate())
}

func TestOutputChunkValidate(t *testing.T) {
	projectRunID, err := runs.NewID()
	Ok(t, err)
	chunk := runs.OutputChunk{
		ProjectRunID: projectRunID,
		Sequence:     0,
		Stream:       runs.OutputStdout,
		Content:      "terraform output\n",
		CreatedAt:    time.Now(),
	}
	Ok(t, chunk.Validate())

	chunk.Sequence = -1
	Assert(t, chunk.Validate() != nil, "negative output sequence must be rejected")
}

func TestCompletionValidationRequiresTerminalState(t *testing.T) {
	id, err := runs.NewID()
	Ok(t, err)

	runCompletion := runs.RunCompletion{
		ID:          id,
		Status:      runs.StatusSucceeded,
		CompletedAt: time.Now(),
	}
	Ok(t, runCompletion.Validate())
	runCompletion.Status = runs.StatusRunning
	Assert(t, runCompletion.Validate() != nil, "running is not a completion state")
	runCompletion.Status = runs.StatusPending
	Assert(t, runCompletion.Validate() != nil, "pending is not a completion state")

	projectCompletion := runs.ProjectRunCompletion{
		ID:          id,
		Status:      runs.StatusUnchanged,
		CompletedAt: time.Now(),
	}
	Ok(t, projectCompletion.Validate())
	projectCompletion.Destructions = -1
	Assert(t, projectCompletion.Validate() != nil, "negative completion counts must be rejected")
}
