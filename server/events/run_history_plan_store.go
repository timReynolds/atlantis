// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/core/runtime"
	"github.com/runatlantis/atlantis/server/events/command"
	"github.com/runatlantis/atlantis/server/logging"
)

type artifactKeyProvider interface {
	ArtifactKey(ctx command.ProjectContext, planPath string) string
}

// RunHistoryPlanStore records metadata for externally stored plans while
// delegating all artifact operations to the upstream PlanStore.
type RunHistoryPlanStore struct {
	runtime.PlanStore
	history *RunHistory
	now     func() time.Time
}

// NewRunHistoryPlanStore decorates only PlanStores that expose opaque artifact
// keys. Local plan behavior remains untouched.
func NewRunHistoryPlanStore(store runtime.PlanStore, history *RunHistory, _ logging.SimpleLogging) runtime.PlanStore {
	if history == nil {
		return store
	}
	if _, ok := store.(artifactKeyProvider); !ok {
		return store
	}
	return &RunHistoryPlanStore{PlanStore: store, history: history, now: time.Now}
}

func (s *RunHistoryPlanStore) Save(ctx command.ProjectContext, planPath string) error {
	if ctx.RunID == "" || ctx.ProjectRunID == "" {
		return errors.New("external plan persistence requires durable Run and ProjectRun identity")
	}
	checksum, err := hashFile(filepath.Dir(planPath), planPath)
	if err != nil {
		return fmt.Errorf("hashing plan artifact metadata: %w", err)
	}
	key := s.PlanStore.(artifactKeyProvider).ArtifactKey(ctx, planPath)
	if err := s.history.RecordPlanArtifact(ctx, runs.ArtifactReference{
		Key: key, Checksum: "sha256:" + checksum, CreatedAt: s.now().UTC(),
	}); err != nil {
		return fmt.Errorf("recording plan artifact metadata before upload: %w", err)
	}
	if err := s.PlanStore.Save(ctx, planPath); err != nil {
		return err
	}
	return nil
}

func (s *RunHistoryPlanStore) Load(ctx command.ProjectContext, planPath string) error {
	reject := func(err error) error {
		if removeErr := os.Remove(planPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return fmt.Errorf("%w; removing rejected plan: %v", err, removeErr)
		}
		return err
	}
	if err := s.PlanStore.Load(ctx, planPath); err != nil {
		return reject(err)
	}
	if s.history.planArtifacts == nil {
		return reject(errors.New("restoring an external plan requires durable artifact history"))
	}
	expected, err := s.history.planArtifacts.FindPlanArtifact(context.Background(), runs.PlanArtifactLookup{
		Repository: ctx.BaseRepo.ID(), PullNumber: ctx.Pull.Num, HeadSHA: ctx.Pull.HeadCommit,
		ProjectName: ctx.ProjectName, Directory: ctx.RepoRelDir, Workspace: ctx.Workspace,
		ProjectRunID: ctx.ProjectRunID,
	})
	if err != nil {
		return reject(fmt.Errorf("finding durable plan artifact expectation: %w", err))
	}
	key := s.PlanStore.(artifactKeyProvider).ArtifactKey(ctx, planPath)
	if expected.Artifact.Key != key {
		return reject(fmt.Errorf("restored plan object key does not match durable history; run `atlantis plan`"))
	}
	if expected.Artifact.ExpiresAt != nil && !s.now().UTC().Before(*expected.Artifact.ExpiresAt) {
		return reject(fmt.Errorf("restored plan artifact has expired; run `atlantis plan`"))
	}
	if expected.Identity.RepoConfigVersion != ctx.RepoConfigVersion || expected.Identity.WorkflowChecksum != ctx.WorkflowIdentity {
		return reject(fmt.Errorf("resolved Atlantis workflow changed since plan; run `atlantis plan`"))
	}
	actual, err := hashFile(filepath.Dir(planPath), planPath)
	if err != nil {
		return reject(fmt.Errorf("hashing restored plan artifact: %w", err))
	}
	if expected.Artifact.Checksum != "sha256:"+actual {
		return reject(fmt.Errorf("restored plan checksum does not match durable history; run `atlantis plan`"))
	}
	return nil
}

var _ runtime.PlanStore = (*RunHistoryPlanStore)(nil)
