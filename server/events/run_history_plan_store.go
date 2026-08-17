// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
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
	logger  logging.SimpleLogging
	now     func() time.Time
}

// NewRunHistoryPlanStore decorates only PlanStores that expose opaque artifact
// keys. Local plan behavior remains untouched.
func NewRunHistoryPlanStore(store runtime.PlanStore, history *RunHistory, logger logging.SimpleLogging) runtime.PlanStore {
	if history == nil {
		return store
	}
	if _, ok := store.(artifactKeyProvider); !ok {
		return store
	}
	return &RunHistoryPlanStore{PlanStore: store, history: history, logger: logger, now: time.Now}
}

func (s *RunHistoryPlanStore) Save(ctx command.ProjectContext, planPath string) error {
	if err := s.PlanStore.Save(ctx, planPath); err != nil {
		return err
	}
	if ctx.RunID == "" || ctx.ProjectRunID == "" {
		return nil
	}
	checksum, err := hashFile(filepath.Dir(planPath), planPath)
	if err != nil {
		s.logger.Err("hashing plan artifact metadata %v", err)
		return nil
	}
	key := s.PlanStore.(artifactKeyProvider).ArtifactKey(ctx, planPath)
	s.history.RecordPlanArtifact(ctx, runs.ArtifactReference{
		Key: key, Checksum: "sha256:" + checksum, CreatedAt: s.now().UTC(),
	})
	return nil
}

var _ runtime.PlanStore = (*RunHistoryPlanStore)(nil)
