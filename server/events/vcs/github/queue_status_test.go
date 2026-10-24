// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package github_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/events/vcs/github"
	"github.com/runatlantis/atlantis/server/logging"
	. "github.com/runatlantis/atlantis/testing"
)

func TestQueuePullIsMergeableExcludesOnlyDeferredChecks(t *testing.T) {
	for _, tc := range []struct {
		name             string
		state            string
		draft            bool
		failedCheck      string
		projectApply     bool
		review           string
		requiredReviewer bool
		requiredWorkflow bool
		mergeable        bool
	}{
		{name: "pending queue and apply", state: "blocked", review: "APPROVED", mergeable: true},
		{name: "pending project apply", state: "blocked", review: "APPROVED", projectApply: true, mergeable: true},
		{name: "unrelated apply prefix failure", state: "blocked", review: "APPROVED", failedCheck: "atlantis/apply-other"},
		{name: "external failure", state: "blocked", review: "APPROVED", failedCheck: "ci/build"},
		{name: "plan failure", state: "blocked", review: "APPROVED", failedCheck: "atlantis/plan"},
		{name: "other queue failure", state: "blocked", review: "APPROVED", failedCheck: "atlantis/queue-other"},
		{name: "review required", state: "blocked", review: "REVIEW_REQUIRED"},
		{name: "changes requested", state: "blocked", review: "CHANGES_REQUESTED"},
		{name: "ruleset reviewer required", state: "blocked", requiredReviewer: true},
		{name: "required workflow missing", state: "blocked", review: "APPROVED", requiredWorkflow: true},
		{name: "conflict", state: "dirty", review: "APPROVED"},
		{name: "behind", state: "behind", review: "APPROVED"},
		{name: "unknown", state: "unknown", review: "APPROVED"},
		{name: "draft", state: "clean", draft: true, review: "APPROVED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var required, statuses []map[string]any
			contexts := []string{"atlantis/queue", "atlantis/apply", "atlantis/apply-other", "atlantis/plan", "atlantis/queue-other", "ci/build"}
			if tc.projectApply {
				contexts = append(contexts, "atlantis/apply: production")
			}
			for _, context := range contexts {
				required = append(required, map[string]any{"context": context})
				state := "SUCCESS"
				if context == "atlantis/queue" || context == "atlantis/apply" || context == "atlantis/apply: production" {
					state = "PENDING"
				}
				if context == tc.failedCheck {
					state = "FAILURE"
				}
				statuses = append(statuses, map[string]any{"__typename": "StatusContext", "context": context, "state": state, "isRequired": true})
			}
			rules := []map[string]any{}
			if tc.requiredReviewer {
				rules = append(rules, map[string]any{"type": "PULL_REQUEST", "repositoryRuleset": map[string]any{"enforcement": "ACTIVE"}, "parameters": map[string]any{"requiredApprovingReviewCount": 1}})
			}
			if tc.requiredWorkflow {
				rules = append(rules, map[string]any{"type": "WORKFLOWS", "repositoryRuleset": map[string]any{"enforcement": "ACTIVE"}, "parameters": map[string]any{"workflows": []map[string]any{{"path": ".github/workflows/required.yml", "repositoryId": 1}}}})
			}
			pullStatus := map[string]any{
				"reviewDecision": tc.review,
				"baseRef": map[string]any{
					"branchProtectionRule": map[string]any{"requiredStatusChecks": required},
					"rules":                map[string]any{"nodes": rules},
				},
				"commits": map[string]any{"nodes": []map[string]any{{"commit": map[string]any{"statusCheckRollup": map[string]any{"contexts": map[string]any{"nodes": statuses}}}}}},
			}
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/api/v3/repos/o/r/pulls/1":
					if err := json.NewEncoder(w).Encode(map[string]any{"number": 1, "mergeable_state": tc.state, "draft": tc.draft}); err != nil {
						t.Error(err)
					}
				case "/api/graphql":
					if err := json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"repository": map[string]any{"pullRequest": pullStatus}}}); err != nil {
						t.Error(err)
					}
				default:
					t.Errorf("unexpected request %s", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(server.Close)
			host, err := url.Parse(server.URL)
			Ok(t, err)
			log := logging.NewNoopLogger(t)
			// The queue path must work without the ordinary apply bypass flag.
			client, err := github.New(host.Host, transportCredentials{client: server.Client()}, github.Config{}, 0, log)
			Ok(t, err)
			repo := models.Repo{FullName: "o/r", Owner: "o", Name: "r"}
			pull := models.PullRequest{Num: 1, BaseRepo: repo}
			status, err := client.QueuePullIsMergeable(log, repo, pull, "atlantis", "atlantis/queue")
			Ok(t, err)
			Equals(t, tc.mergeable, status.IsMergeable)
			ordinary, err := client.PullIsMergeable(log, repo, pull, "atlantis", []string{"atlantis/queue"})
			Ok(t, err)
			Assert(t, !ordinary.IsMergeable, "ordinary commands must retain configured mergeability behavior")
		})
	}
}
