// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package github_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/events/vcs/github"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/require"
)

func TestQueueCommentRecoversDeletedPersistedComment(t *testing.T) {
	created := false
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPatch && r.URL.Path == "/api/v3/repos/o/r/issues/comments/10":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"Not Found"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/repos/o/r/issues/1/comments":
			_, _ = w.Write([]byte(`[]`))
		case r.Method == http.MethodPost && r.URL.Path == "/api/v3/repos/o/r/issues/1/comments":
			created = true
			_, _ = w.Write([]byte(`{"id":11}`))
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	host, err := url.Parse(server.URL)
	require.NoError(t, err)
	client, err := github.New(host.Host, transportCredentials{client: server.Client()}, github.Config{}, 0, logging.NewNoopLogger(t))
	require.NoError(t, err)
	id, err := client.QueueComment(context.Background(), models.Repo{FullName: "o/r", Owner: "o", Name: "r"}, 1, 10, "<!-- atlantis-queue:test -->", "awaiting approval")
	require.NoError(t, err, "a deleted status comment must not permanently prevent durable queue reporting")
	require.True(t, created)
	require.Equal(t, int64(11), id)
}

func TestQueueCommentRecoveryIsBounded(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusNotFound} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			edits, searches := 0, 0
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.Method {
				case http.MethodPatch:
					edits++
					w.WriteHeader(status)
					_, _ = w.Write([]byte(`{"message":"unavailable"}`))
				case http.MethodGet:
					searches++
					// A stale list result must not cause recursive recovery forever.
					_, _ = w.Write([]byte(`[{"id":10,"body":"<!-- atlantis-queue:test -->\nold body","user":{"login":"test-user"}}]`))
				default:
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					http.Error(w, "unexpected request", http.StatusBadRequest)
				}
			}))
			defer server.Close()
			host, err := url.Parse(server.URL)
			require.NoError(t, err)
			client, err := github.New(host.Host, transportCredentials{client: server.Client()}, github.Config{}, 0, logging.NewNoopLogger(t))
			require.NoError(t, err)
			_, err = client.QueueComment(context.Background(), models.Repo{FullName: "o/r", Owner: "o", Name: "r"}, 1, 10, "<!-- atlantis-queue:test -->", "awaiting approval")
			require.Error(t, err)
			if status == http.StatusNotFound {
				require.Equal(t, 2, edits)
				require.Equal(t, 1, searches)
			} else {
				require.Equal(t, 1, edits)
				require.Zero(t, searches, "permission errors must not recreate comments")
			}
		})
	}
}
