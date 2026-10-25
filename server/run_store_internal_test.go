// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/require"
)

func TestInitializeRunStoreDefaultsToNoop(t *testing.T) {
	store, health, closer, err := initializeRunStore(UserConfig{}, logging.NewNoopLogger(t))
	require.NoError(t, err)
	require.IsType(t, runs.NoopStore{}, store)
	require.Nil(t, health)
	require.Nil(t, closer)
}

func TestValidateRunStoreConfig(t *testing.T) {
	tests := []struct {
		name   string
		config UserConfig
		error  string
	}{
		{name: "empty configuration uses no-op"},
		{
			name:   "PostgreSQL URL needs explicit type",
			config: UserConfig{RunStorePostgresURL: "postgres://database/atlantis"},
			error:  "run store PostgreSQL URL requires run store type postgres",
		},
		{
			name:   "PostgreSQL type needs URL",
			config: UserConfig{RunStoreType: RunStorePostgres},
			error:  "run store PostgreSQL URL is required",
		},
		{
			name: "PostgreSQL history rejects default web credentials",
			config: UserConfig{
				RunStoreType: RunStorePostgres, RunStorePostgresURL: "postgres://database/atlantis",
				WebBasicAuth: true, WebUsername: DefaultWebUsername, WebPassword: DefaultWebPassword,
			},
			error: "postgres run history requires non-default web basic-auth credentials",
		},
		{
			name: "PostgreSQL history accepts custom web credentials",
			config: UserConfig{
				RunStoreType: RunStorePostgres, RunStorePostgresURL: "postgres://database/atlantis",
				WebBasicAuth: true, WebUsername: "operator", WebPassword: "secret",
			},
		},
		{
			name: "PostgreSQL history accepts a custom password with the default username",
			config: UserConfig{
				RunStoreType: RunStorePostgres, RunStorePostgresURL: "postgres://database/atlantis",
				WebBasicAuth: true, WebUsername: DefaultWebUsername, WebPassword: "secret",
			},
		},
		{
			name: "PostgreSQL history accepts a custom username with the default password",
			config: UserConfig{
				RunStoreType: RunStorePostgres, RunStorePostgresURL: "postgres://database/atlantis",
				WebBasicAuth: true, WebUsername: "operator", WebPassword: DefaultWebPassword,
			},
		},
		{
			name:   "unknown type",
			config: UserConfig{RunStoreType: "filesystem"},
			error:  `unknown run store type "filesystem"`,
		},
		{
			name: "idle connections cannot exceed open connections",
			config: UserConfig{
				RunStoreType: RunStoreNoop, RunStoreMaxOpenConns: 2, RunStoreMaxIdleConns: 3,
			},
			error: "run store maximum idle connections exceed maximum open connections",
		},
		{
			name:   "retention cannot be negative",
			config: UserConfig{RunStoreOutputRetentionDays: -1},
			error:  "run store retention days cannot be negative",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateRunStoreConfig(test.config)
			if test.error == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, test.error)
		})
	}
}

func TestRunStoreRetentionServiceBuildsIndependentCutoffs(t *testing.T) {
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.FixedZone("test", 2*60*60))
	service := runStoreRetentionService{metadataDays: 365, outputDays: 90, now: func() time.Time { return now }}

	policy := service.policy(now)

	require.Equal(t, now.UTC().AddDate(0, 0, -365), *policy.RunMetadataBefore)
	require.Equal(t, now.UTC().AddDate(0, 0, -90), *policy.OutputBefore)
	require.Nil(t, policy.AuditEventsBefore)
}

func TestRunStoreRetentionServiceStopsWithContext(t *testing.T) {
	retainer := &recordingRetainer{called: make(chan runs.RetentionPolicy, 1)}
	service := runStoreRetentionService{
		store: retainer, logger: logging.NewNoopLogger(t), outputDays: 90,
		interval: time.Hour, now: func() time.Time { return time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC) },
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		service.Run(ctx)
	}()

	select {
	case policy := <-retainer.called:
		require.NotNil(t, policy.OutputBefore)
	case <-time.After(time.Second):
		t.Fatal("retention service did not apply its startup policy")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("retention service did not stop after cancellation")
	}
}

func TestReadyzChecksRunStore(t *testing.T) {
	s := &Server{runStoreHealth: failingRunStorePinger{err: errors.New("database unavailable")}}
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	response := httptest.NewRecorder()

	s.Readyz(response, request)

	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.JSONEq(t, `{"status":"error","error":"database unavailable"}`, response.Body.String())
}

type failingRunStorePinger struct{ err error }

func (p failingRunStorePinger) Ping(context.Context) error { return p.err }

type recordingRetainer struct {
	called chan runs.RetentionPolicy
}

func (r *recordingRetainer) ApplyRetention(_ context.Context, policy runs.RetentionPolicy) (runs.RetentionResult, error) {
	r.called <- policy
	return runs.RetentionResult{}, nil
}
