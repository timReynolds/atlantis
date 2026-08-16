// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/require"
)

func TestExecutionInstanceServiceRegistersHeartbeatsAndStops(t *testing.T) {
	store := &recordingExecutionInstanceStore{}
	startedAt := time.Date(2026, 8, 16, 10, 0, 0, 0, time.UTC)
	instance := runs.ExecutionInstance{
		ID: "0198a0df-85f1-7d83-a60b-2e57b725c62d", ReplicaID: "atlantis-0",
		DeploymentID: "prod-eu", StartedAt: startedAt, HeartbeatAt: startedAt,
	}
	service := newExecutionInstanceService(store, instance, time.Millisecond, logging.NewNoopLogger(t))
	service.now = func() time.Time { return startedAt.Add(time.Second) }

	require.NoError(t, service.Start(context.Background()))
	require.Eventually(t, func() bool { return store.heartbeatCount() > 0 }, time.Second, time.Millisecond)
	require.NoError(t, service.Ready(context.Background()))
	require.NoError(t, service.Stop(context.Background()))

	require.Equal(t, instance, store.registered)
	require.Equal(t, instance.ID, store.stoppedID)
	require.Equal(t, startedAt.Add(time.Second), store.stoppedAt)
	require.ErrorContains(t, service.Ready(context.Background()), "stopped")
}

func TestExecutionInstanceServiceReadinessTracksHeartbeatFailureAndRecovery(t *testing.T) {
	store := &recordingExecutionInstanceStore{heartbeatErr: errors.New("database unavailable")}
	startedAt := time.Date(2026, 8, 16, 10, 0, 0, 0, time.UTC)
	service := newExecutionInstanceService(store, runs.ExecutionInstance{
		ID: "0198a0df-85f1-7d83-a60b-2e57b725c62d", ReplicaID: "atlantis-0",
		DeploymentID: "prod-eu", StartedAt: startedAt, HeartbeatAt: startedAt,
	}, time.Millisecond, logging.NewNoopLogger(t))
	require.NoError(t, service.Start(context.Background()))
	t.Cleanup(func() { _ = service.Stop(context.Background()) })

	require.Eventually(t, func() bool {
		return service.Ready(context.Background()) != nil
	}, time.Second, time.Millisecond)
	store.setHeartbeatError(nil)
	require.Eventually(t, func() bool {
		return service.Ready(context.Background()) == nil
	}, time.Second, time.Millisecond)
}

type recordingExecutionInstanceStore struct {
	mu           sync.Mutex
	registered   runs.ExecutionInstance
	heartbeats   int
	heartbeatErr error
	stoppedID    runs.ID
	stoppedAt    time.Time
}

func (s *recordingExecutionInstanceStore) RegisterInstance(_ context.Context, instance runs.ExecutionInstance) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registered = instance
	return nil
}

func (s *recordingExecutionInstanceStore) HeartbeatInstance(context.Context, runs.ID, time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.heartbeats++
	return s.heartbeatErr
}

func (s *recordingExecutionInstanceStore) StopInstance(_ context.Context, id runs.ID, stoppedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stoppedID = id
	s.stoppedAt = stoppedAt
	return nil
}

func (s *recordingExecutionInstanceStore) heartbeatCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.heartbeats
}

func (s *recordingExecutionInstanceStore) setHeartbeatError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.heartbeatErr = err
}
