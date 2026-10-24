// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package controllers_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
	"github.com/runatlantis/atlantis/server/controllers"
	"github.com/runatlantis/atlantis/server/core/queue"
	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/require"
)

type queueHTTPStore struct {
	queue.Store
	called     bool
	actor      string
	generation int64
}

func (s *queueHTTPStore) Action(_ context.Context, _ runs.ID, generation int64, action, actor, delivery, summary string) (queue.Entry, error) {
	s.called = true
	s.actor = actor
	s.generation = generation
	return queue.Entry{State: queue.Cancelled, Reason: summary}, nil
}
func (s *queueHTTPStore) Get(context.Context, runs.ID) (queue.Entry, error) {
	return queue.Entry{Repository: "<script>alert(1)</script>", State: queue.Blocked, Reason: "<img src=x onerror=alert(1)>"}, nil
}
func (s *queueHTTPStore) Events(context.Context, runs.ID, int) ([]queue.Event, error) {
	return nil, nil
}
func TestQueueRecoveryAuthorizationAndConfirmation(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		authenticated, confirmed bool
		body                     string
		status                   int
	}{{"unauthenticated", false, true, `{"generation":2,"action":"release","summary":"verified state"}`, 401}, {"missing confirmation", true, false, `{"generation":2,"action":"release","summary":"verified state"}`, 403}, {"missing decision", true, true, `{"generation":2,"action":"release","summary":""}`, 400}, {"unknown field", true, true, `{"generation":2,"action":"release","summary":"verified state","force":true}`, 400}, {"trailing JSON", true, true, `{"generation":2,"action":"release","summary":"verified state"}{}`, 400}, {"explicit recovery", true, true, `{"generation":2,"action":"release","summary":"stopped previous worker and verified infrastructure"}`, 200}} {
		t.Run(tc.name, func(t *testing.T) {
			store := &queueHTTPStore{}
			controller := &controllers.QueueController{Store: store, History: &controllers.RunHistoryController{WebAuthentication: true, WebUsername: "operator", WebPassword: "secret", Logger: logging.NewNoopLogger(t)}}
			request := httptest.NewRequest(http.MethodPost, "/queue/id/reconcile", strings.NewReader(tc.body))
			request = mux.SetURLVars(request, map[string]string{"queue-id": "01922000-0000-7000-8000-000000000001"})
			request.Header.Set("Content-Type", "application/json")
			if tc.authenticated {
				request.SetBasicAuth("operator", "secret")
			}
			if tc.confirmed {
				request.Header.Set("X-Atlantis-Reconcile-Unknown", "true")
			}
			response := httptest.NewRecorder()
			controller.Reconcile(response, request)
			require.Equal(t, tc.status, response.Code)
			require.Equal(t, tc.status == 200, store.called)
			if store.called {
				require.Equal(t, "operator", store.actor)
				require.EqualValues(t, 2, store.generation)
			}
		})
	}
}
func TestQueueDetailEscapesRepositoryAndReasons(t *testing.T) {
	controller := &controllers.QueueController{Store: &queueHTTPStore{}, History: &controllers.RunHistoryController{WebAuthentication: true, WebUsername: "operator", WebPassword: "secret", Logger: logging.NewNoopLogger(t)}}
	request := httptest.NewRequest(http.MethodGet, "/queue/id", nil)
	request = mux.SetURLVars(request, map[string]string{"queue-id": "01922000-0000-7000-8000-000000000001"})
	request.SetBasicAuth("operator", "secret")
	response := httptest.NewRecorder()
	controller.Detail(response, request)
	require.Equal(t, 200, response.Code)
	require.NotContains(t, response.Body.String(), "<script>")
	require.Contains(t, response.Body.String(), "&lt;script&gt;")
	require.NotContains(t, response.Body.String(), "<img src=x")
	require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
}
