// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/logging"
)

const defaultExecutionInstanceHeartbeatInterval = 10 * time.Second

type executionInstanceStore interface {
	RegisterInstance(context.Context, runs.ExecutionInstance) error
	HeartbeatInstance(context.Context, runs.ID, time.Time) error
	StopInstance(context.Context, runs.ID, time.Time) error
}

type executionInstanceLifecycle interface {
	Start(context.Context) error
	Ready(context.Context) error
	Stop(context.Context) error
}

// executionInstanceService keeps the durable process record current. Redis
// still controls ownership leases; this service makes process responsibility
// and crash detection inspectable in PostgreSQL.
type executionInstanceService struct {
	store    executionInstanceStore
	instance runs.ExecutionInstance
	interval time.Duration
	logger   logging.SimpleLogging
	now      func() time.Time

	mu      sync.RWMutex
	started bool
	stopped bool
	lastErr error
	cancel  context.CancelFunc
	done    chan struct{}
}

func newExecutionInstanceService(
	store executionInstanceStore,
	instance runs.ExecutionInstance,
	interval time.Duration,
	logger logging.SimpleLogging,
) *executionInstanceService {
	if interval <= 0 {
		interval = defaultExecutionInstanceHeartbeatInterval
	}
	return &executionInstanceService{
		store: store, instance: instance, interval: interval, logger: logger,
		now: func() time.Time { return time.Now().UTC() },
	}
}

func (s *executionInstanceService) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return errors.New("execution instance service is stopped")
	}
	if s.started {
		return nil
	}
	if err := s.store.RegisterInstance(ctx, s.instance); err != nil {
		return fmt.Errorf("registering execution instance: %w", err)
	}
	loopCtx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan struct{})
	s.started = true
	go s.run(loopCtx, s.done)
	return nil
}

func (s *executionInstanceService) Ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.started {
		return errors.New("execution instance is not registered")
	}
	if s.stopped {
		return errors.New("execution instance is stopped")
	}
	if s.lastErr != nil {
		return fmt.Errorf("execution instance heartbeat unhealthy: %w", s.lastErr)
	}
	return nil
}

func (s *executionInstanceService) Stop(ctx context.Context) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	if !s.started {
		s.stopped = true
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	cancel := s.cancel
	done := s.done
	s.mu.Unlock()

	cancel()
	select {
	case <-done:
	case <-ctx.Done():
		return fmt.Errorf("waiting for execution instance heartbeat: %w", ctx.Err())
	}
	if err := s.store.StopInstance(ctx, s.instance.ID, s.now()); err != nil {
		return fmt.Errorf("stopping execution instance: %w", err)
	}
	return nil
}

func (s *executionInstanceService) run(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.heartbeat(ctx)
		}
	}
}

func (s *executionInstanceService) heartbeat(ctx context.Context) {
	err := s.store.HeartbeatInstance(ctx, s.instance.ID, s.now())
	s.mu.Lock()
	s.lastErr = err
	s.mu.Unlock()
	if err != nil {
		s.logger.Warn("execution instance heartbeat unavailable %v", err)
	}
}
