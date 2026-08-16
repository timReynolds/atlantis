// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package drift

import (
	"fmt"
	"slices"
	"sync"

	"github.com/runatlantis/atlantis/server/events/models"
)

// RemediationResultStore owns remediation result persistence independently of
// latest-state drift storage.
type RemediationResultStore interface {
	Put(result *models.RemediationResult) error
	GetResult(id string) (*models.RemediationResult, error)
	ListResults(repository string, limit int) ([]*models.RemediationResult, error)
}

// InMemoryRemediationResultStore preserves the upstream default behavior.
type InMemoryRemediationResultStore struct {
	mu          sync.RWMutex
	results     map[string]*models.RemediationResult
	repoResults map[string][]string
}

// NewInMemoryRemediationResultStore creates process-local remediation history.
func NewInMemoryRemediationResultStore() *InMemoryRemediationResultStore {
	return &InMemoryRemediationResultStore{
		results: make(map[string]*models.RemediationResult), repoResults: make(map[string][]string),
	}
}

// Put stores a defensive copy of the latest remediation result state.
func (s *InMemoryRemediationResultStore) Put(result *models.RemediationResult) error {
	if result == nil {
		return fmt.Errorf("remediation result is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results[result.ID] = cloneRemediationResult(result)
	repositoryKey := remediationResultRepositoryKey(result)
	if !slices.Contains(s.repoResults[repositoryKey], result.ID) {
		s.repoResults[repositoryKey] = append(s.repoResults[repositoryKey], result.ID)
	}
	return nil
}

// GetResult retrieves a defensive copy by ID.
func (s *InMemoryRemediationResultStore) GetResult(id string) (*models.RemediationResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result, ok := s.results[id]
	if !ok {
		return nil, fmt.Errorf("remediation result not found: %s", id)
	}
	return cloneRemediationResult(result), nil
}

// ListResults returns newest-first defensive copies for one repository key.
func (s *InMemoryRemediationResultStore) ListResults(repository string, limit int) ([]*models.RemediationResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids, ok := s.repoResults[repository]
	if !ok {
		return []*models.RemediationResult{}, nil
	}
	results := make([]*models.RemediationResult, 0, len(ids))
	for i := len(ids) - 1; i >= 0 && (limit <= 0 || len(results) < limit); i-- {
		if result, ok := s.results[ids[i]]; ok {
			results = append(results, cloneRemediationResult(result))
		}
	}
	return results, nil
}

var _ RemediationResultStore = (*InMemoryRemediationResultStore)(nil)
