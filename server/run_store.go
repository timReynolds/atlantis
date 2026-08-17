// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
	runspostgres "github.com/runatlantis/atlantis/server/core/runs/postgres"
	"github.com/runatlantis/atlantis/server/logging"
)

const runStoreRetentionInterval = 24 * time.Hour

func validateRunStoreConfig(userConfig UserConfig) error {
	storeType := userConfig.RunStoreType
	if storeType == "" {
		storeType = RunStoreNoop
	}
	switch storeType {
	case RunStoreNoop:
		if strings.TrimSpace(userConfig.RunStorePostgresURL) != "" {
			return errors.New("run store PostgreSQL URL requires run store type postgres")
		}
	case RunStorePostgres:
		if strings.TrimSpace(userConfig.RunStorePostgresURL) == "" {
			return errors.New("run store PostgreSQL URL is required")
		}
		if userConfig.WebBasicAuth &&
			(strings.TrimSpace(userConfig.WebUsername) == "" || strings.TrimSpace(userConfig.WebPassword) == "" ||
				(userConfig.WebUsername == DefaultWebUsername && userConfig.WebPassword == DefaultWebPassword)) {
			return errors.New("postgres run history requires non-default web basic-auth credentials")
		}
	default:
		return fmt.Errorf("unknown run store type %q", userConfig.RunStoreType)
	}
	if userConfig.RunStoreMaxOpenConns < 0 {
		return errors.New("run store maximum open connections cannot be negative")
	}
	if userConfig.RunStoreMaxIdleConns < 0 {
		return errors.New("run store maximum idle connections cannot be negative")
	}
	if userConfig.RunStoreMaxOpenConns > 0 && userConfig.RunStoreMaxIdleConns > userConfig.RunStoreMaxOpenConns {
		return errors.New("run store maximum idle connections exceed maximum open connections")
	}
	if userConfig.RunStoreRetentionDays < 0 || userConfig.RunStoreOutputRetentionDays < 0 || userConfig.RunStoreAuditRetentionDays < 0 {
		return errors.New("run store retention days cannot be negative")
	}
	return nil
}

func initializeRunStore(userConfig UserConfig, logger logging.SimpleLogging) (runs.Store, runStorePinger, io.Closer, error) {
	storeType := userConfig.RunStoreType
	if storeType == "" || storeType == RunStoreNoop {
		return runs.NoopStore{}, nil, nil, nil
	}
	logger.Info("utilizing PostgreSQL run history store")
	store, err := runspostgres.New(context.Background(), runspostgres.Config{
		URL:          userConfig.RunStorePostgresURL,
		MaxOpenConns: userConfig.RunStoreMaxOpenConns,
		MaxIdleConns: userConfig.RunStoreMaxIdleConns,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("initializing PostgreSQL run store: %w", err)
	}
	return store, store, store, nil
}

type runStoreRetentionService struct {
	store        runs.Retainer
	logger       logging.SimpleLogging
	metadataDays int
	outputDays   int
	auditDays    int
	interval     time.Duration
	now          func() time.Time
}

func newRunStoreRetentionService(store runs.Store, userConfig UserConfig, logger logging.SimpleLogging) *runStoreRetentionService {
	if userConfig.RunStoreType != RunStorePostgres ||
		(userConfig.RunStoreRetentionDays == 0 && userConfig.RunStoreOutputRetentionDays == 0 && userConfig.RunStoreAuditRetentionDays == 0) {
		return nil
	}
	return &runStoreRetentionService{
		store: store, logger: logger,
		metadataDays: userConfig.RunStoreRetentionDays,
		outputDays:   userConfig.RunStoreOutputRetentionDays,
		auditDays:    userConfig.RunStoreAuditRetentionDays,
		interval:     runStoreRetentionInterval,
		now:          time.Now,
	}
}

func (s *runStoreRetentionService) Run(ctx context.Context) {
	s.apply(ctx)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.apply(ctx)
		}
	}
}

func (s *runStoreRetentionService) apply(ctx context.Context) {
	result, err := s.store.ApplyRetention(ctx, s.policy(s.now()))
	if err != nil {
		s.logger.Err("applying run history retention %v", err)
		return
	}
	s.logger.Info(
		"applied run history retention runs %d, project runs %d, output chunks %d, audit events %d",
		result.RunsDeleted, result.ProjectRunsDeleted, result.OutputChunksDeleted, result.AuditEventsDeleted,
	)
}

func (s *runStoreRetentionService) policy(now time.Time) runs.RetentionPolicy {
	now = now.UTC()
	return runs.RetentionPolicy{
		RunMetadataBefore: retentionCutoff(now, s.metadataDays),
		OutputBefore:      retentionCutoff(now, s.outputDays),
		AuditEventsBefore: retentionCutoff(now, s.auditDays),
	}
}

func retentionCutoff(now time.Time, days int) *time.Time {
	if days == 0 {
		return nil
	}
	cutoff := now.AddDate(0, 0, -days)
	return &cutoff
}
