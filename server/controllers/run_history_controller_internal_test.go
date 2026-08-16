// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package controllers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/runatlantis/atlantis/server/controllers/web_templates"
	"github.com/runatlantis/atlantis/server/core/runs"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/require"
)

func TestRunHistoryControllerRequiresExplicitWebAuthentication(t *testing.T) {
	controller := testRunHistoryController(t, &recordingRunReader{}, &recordingHistoryTemplate{})

	recorder := httptest.NewRecorder()
	controller.ListRuns(recorder, httptest.NewRequest(http.MethodGet, "/runs", nil))
	require.Equal(t, http.StatusNotFound, recorder.Code)

	controller.WebAuthentication = true
	recorder = httptest.NewRecorder()
	controller.ListRuns(recorder, httptest.NewRequest(http.MethodGet, "/runs", nil))
	require.Equal(t, http.StatusUnauthorized, recorder.Code)
	require.NotEmpty(t, recorder.Header().Get("WWW-Authenticate"))

	recorder = httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/runs", nil)
	request.SetBasicAuth("operator", "wrong")
	controller.ListRuns(recorder, request)
	require.Equal(t, http.StatusUnauthorized, recorder.Code)
}

func TestRunHistoryControllerListsFilteredRuns(t *testing.T) {
	runID := mustRunHistoryID(t)
	pull := 42
	createdAt := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	reader := &recordingRunReader{
		listRunsResult: runs.RunPage{Runs: []runs.Run{{
			ID: runID, Repository: "org/repo", PullNumber: &pull,
			Command: runs.CommandPlan, Trigger: runs.TriggerComment, Actor: "operator",
			HeadSHA: "abc123", Status: runs.StatusFailed, CreatedAt: createdAt,
			Metadata: runs.Metadata(`{"pull_url":"https://example.test/org/repo/pull/42"}`),
		}}, NextCursor: "next-page"},
	}
	template := &recordingHistoryTemplate{}
	controller := testRunHistoryController(t, reader, template)
	controller.WebAuthentication = true
	request := httptest.NewRequest(http.MethodGet, "/runs?repository=org%2Frepo&pull=42&commit=abc123&actor=operator&command=plan%2Capply&status=failed%2Cpartial&cursor=old", nil)
	request.SetBasicAuth("operator", "password")
	recorder := httptest.NewRecorder()

	controller.ListRuns(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, runs.RunFilter{
		Repository: "org/repo", PullNumber: &pull, HeadSHA: "abc123", Actor: "operator",
		Commands: []runs.Command{runs.CommandPlan, runs.CommandApply},
		Statuses: []runs.Status{runs.StatusFailed, runs.StatusPartial},
	}, reader.listRunsFilter)
	require.Equal(t, runs.PageRequest{Limit: runHistoryPageLimit, Cursor: "old"}, reader.listRunsPage)
	data, ok := template.data.(web_templates.RunHistoryListData)
	require.True(t, ok)
	require.Len(t, data.Runs, 1)
	require.Equal(t, string(runID), data.Runs[0].ID)
	require.Equal(t, "/atlantis/runs/"+string(runID), data.CleanedBasePath+data.Runs[0].DetailPath)
	require.Contains(t, data.NextPath, "cursor=next-page")
	require.NotContains(t, data.NextPath, "cursor=old")
}

func TestRunHistoryControllerPreservesNestedRepositoryNamespaces(t *testing.T) {
	pull := 42
	run := runs.Run{Repository: "group/subgroup/repo", PullNumber: &pull}
	presented := presentRun(run)
	require.Equal(t, "/repos/group/subgroup/repo", presented.RepositoryPath)
	require.Equal(t, "/repos/group/subgroup/repo?pull=42", presented.PullHistoryPath)

	request := mux.SetURLVars(httptest.NewRequest(http.MethodGet, presented.PullHistoryPath, nil), map[string]string{
		"repository": "group/subgroup/repo",
	})
	filter, _, _, err := parseRunHistoryFilter(request)
	require.NoError(t, err)
	require.Equal(t, run.Repository, filter.Repository)
	require.Equal(t, pull, *filter.PullNumber)
}

func TestRunHistoryControllerDoesNotTreatRepositorySuffixAsPullRoute(t *testing.T) {
	request := mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/repos/group/pulls/42", nil), map[string]string{
		"repository": "group/pulls/42",
	})

	filter, _, _, err := parseRunHistoryFilter(request)

	require.NoError(t, err)
	require.Equal(t, "group/pulls/42", filter.Repository)
	require.Nil(t, filter.PullNumber)
}

func TestRunHistoryControllerDoesNotExposeProjectOutputWithoutAuthentication(t *testing.T) {
	reader := &recordingRunReader{failOnRead: true}
	controller := testRunHistoryController(t, reader, &recordingHistoryTemplate{})
	controller.WebAuthentication = true
	request := mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/runs/run/projects/project", nil), map[string]string{
		"run-id": "run", "project-id": "project",
	})
	recorder := httptest.NewRecorder()

	controller.GetProject(recorder, request)

	require.Equal(t, http.StatusUnauthorized, recorder.Code)
	require.False(t, reader.readCalled)
}

func TestRunHistoryControllerReconcilesUnknownAttemptThroughConfirmedAPI(t *testing.T) {
	runID := mustRunHistoryID(t)
	attemptID := mustRunHistoryID(t)
	reader := &recordingRunReader{attemptResult: runs.RunAttempt{
		ID: attemptID, RunID: runID, Status: runs.AttemptUnknown,
	}}
	controller := testRunHistoryController(t, reader, &recordingHistoryTemplate{})
	controller.WebAuthentication = true
	request := mux.SetURLVars(httptest.NewRequest(http.MethodPost,
		"/runs/x/attempts/y/reconcile", strings.NewReader(`{"summary":"state inspected; fresh plan required"}`)),
		map[string]string{"run-id": string(runID), "attempt-id": string(attemptID)})
	request.SetBasicAuth("operator", "password")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Atlantis-Reconcile-Unknown", "true")
	recorder := httptest.NewRecorder()

	controller.ReconcileAttempt(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, attemptID, reader.reconciliation.ID)
	require.Equal(t, "operator", reader.reconciliation.Actor)
	require.Equal(t, "state inspected; fresh plan required", reader.reconciliation.Summary)
	require.NotEmpty(t, reader.reconciliation.AuditEventID)
	_, err := runs.ParseID(string(reader.reconciliation.AuditEventID))
	require.NoError(t, err)
	require.Contains(t, recorder.Body.String(), `"status":"reconciled"`)
}

func TestRunHistoryControllerAllowsMaximumDecodedSummaryAfterTransportEncoding(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		summary     string
		contentType string
		body        func(*RunHistoryController, runs.ID, runs.ID, string) string
		wantStatus  int
	}{
		{
			name: "JSON escapes", summary: strings.Repeat("\"", maxReconciliationBytes),
			contentType: "application/json", wantStatus: http.StatusOK,
			body: func(_ *RunHistoryController, _, _ runs.ID, summary string) string {
				encoded, err := json.Marshal(map[string]string{"summary": summary})
				require.NoError(t, err)
				return string(encoded)
			},
		},
		{
			name: "form percent encoding", summary: strings.Repeat("é", maxReconciliationBytes/2),
			contentType: "application/x-www-form-urlencoded", wantStatus: http.StatusSeeOther,
			body: func(controller *RunHistoryController, runID, attemptID runs.ID, summary string) string {
				return url.Values{
					"csrf_token": {controller.reconciliationToken(runID, attemptID)},
					"summary":    {summary},
				}.Encode()
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runID := mustRunHistoryID(t)
			attemptID := mustRunHistoryID(t)
			reader := &recordingRunReader{attemptResult: runs.RunAttempt{
				ID: attemptID, RunID: runID, Status: runs.AttemptUnknown,
			}}
			controller := testRunHistoryController(t, reader, &recordingHistoryTemplate{})
			controller.WebAuthentication = true
			request := mux.SetURLVars(httptest.NewRequest(http.MethodPost,
				"/runs/x/attempts/y/reconcile",
				strings.NewReader(testCase.body(controller, runID, attemptID, testCase.summary))),
				map[string]string{"run-id": string(runID), "attempt-id": string(attemptID)})
			request.SetBasicAuth("operator", "password")
			request.Header.Set("Content-Type", testCase.contentType)
			request.Header.Set("X-Atlantis-Reconcile-Unknown", "true")
			recorder := httptest.NewRecorder()

			controller.ReconcileAttempt(recorder, request)

			require.Equal(t, testCase.wantStatus, recorder.Code)
			require.Equal(t, maxReconciliationBytes, len([]byte(reader.reconciliation.Summary)))
			require.Equal(t, testCase.summary, reader.reconciliation.Summary)
		})
	}
}

func TestRunHistoryControllerRejectsSummaryOverDecodedLimit(t *testing.T) {
	runID := mustRunHistoryID(t)
	attemptID := mustRunHistoryID(t)
	reader := &recordingRunReader{attemptResult: runs.RunAttempt{
		ID: attemptID, RunID: runID, Status: runs.AttemptUnknown,
	}}
	controller := testRunHistoryController(t, reader, &recordingHistoryTemplate{})
	controller.WebAuthentication = true
	encoded, err := json.Marshal(map[string]string{"summary": strings.Repeat("a", maxReconciliationBytes+1)})
	require.NoError(t, err)
	request := mux.SetURLVars(httptest.NewRequest(http.MethodPost,
		"/runs/x/attempts/y/reconcile", strings.NewReader(string(encoded))),
		map[string]string{"run-id": string(runID), "attempt-id": string(attemptID)})
	request.SetBasicAuth("operator", "password")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Atlantis-Reconcile-Unknown", "true")
	recorder := httptest.NewRecorder()

	controller.ReconcileAttempt(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Empty(t, reader.reconciliation.ID)
}

func TestRunHistoryControllerReconciliationRequiresConfirmationHeader(t *testing.T) {
	reader := &recordingRunReader{}
	controller := testRunHistoryController(t, reader, &recordingHistoryTemplate{})
	controller.WebAuthentication = true
	request := httptest.NewRequest(http.MethodPost, "/runs/x/attempts/y/reconcile", strings.NewReader(`{"summary":"checked"}`))
	request.SetBasicAuth("operator", "password")
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	controller.ReconcileAttempt(recorder, request)

	require.Equal(t, http.StatusForbidden, recorder.Code)
	require.False(t, reader.readCalled)
}

func TestRunHistoryControllerReturnsTypedProjectOutputPage(t *testing.T) {
	runID := mustRunHistoryID(t)
	projectID := mustRunHistoryID(t)
	next := int64(8)
	reader := &recordingRunReader{
		getRunResult: runs.Run{ID: runID, Repository: "org/repo", Command: runs.CommandApply,
			Trigger: runs.TriggerComment, Status: runs.StatusSucceeded, CreatedAt: time.Now().UTC()},
		getProjectResult: runs.ProjectRun{ID: projectID, RunID: runID, Directory: "terraform/network",
			Workspace: "default", Status: runs.StatusSucceeded},
		getOutputResult: runs.OutputPage{Chunks: []runs.OutputChunk{
			{ProjectRunID: projectID, Sequence: 7, Stream: runs.OutputStdout, Content: "sensitive value\n"},
			{ProjectRunID: projectID, Sequence: 8, Stream: runs.OutputStderr, Content: "warning\n"},
		}, NextSequence: &next},
	}
	template := &recordingHistoryTemplate{}
	controller := testRunHistoryController(t, reader, template)
	controller.WebAuthentication = true
	request := mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/runs/x/projects/y?after=6", nil), map[string]string{
		"run-id": string(runID), "project-id": string(projectID),
	})
	request.SetBasicAuth("operator", "password")
	recorder := httptest.NewRecorder()

	controller.GetProject(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, int64(6), reader.outputAfter)
	require.Equal(t, runHistoryOutputLimit, reader.outputLimit)
	data, ok := template.data.(web_templates.RunProjectDetailData)
	require.True(t, ok)
	require.Equal(t, []web_templates.RunHistoryOutputChunk{
		{Stream: "stdout", Content: "sensitive value\n"},
		{Stream: "stderr", Content: "warning\n"},
	}, data.Output)
	require.Contains(t, data.NextPath, "after=8")
}

func TestRunHistoryControllerParsesAuditDateRange(t *testing.T) {
	reader := &recordingRunReader{}
	template := &recordingHistoryTemplate{}
	controller := testRunHistoryController(t, reader, template)
	controller.WebAuthentication = true
	request := httptest.NewRequest(http.MethodGet, "/audit?repository=org%2Frepo&pull=42&actor=operator&event=apply.completed%2Cunlock.completed&from=2026-08-01&to=2026-08-16", nil)
	request.SetBasicAuth("operator", "password")
	recorder := httptest.NewRecorder()

	controller.ListAudit(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "org/repo", reader.auditFilter.Repository)
	require.Equal(t, []string{"apply.completed", "unlock.completed"}, reader.auditFilter.EventTypes)
	require.Equal(t, time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), *reader.auditFilter.CreatedFrom)
	require.Equal(t, time.Date(2026, 8, 17, 0, 0, 0, 0, time.UTC), *reader.auditFilter.CreatedTo)
}

func TestRunHistoryControllerPresentsAttemptReplicaIdentity(t *testing.T) {
	runID := mustRunHistoryID(t)
	attemptID := mustRunHistoryID(t)
	instanceID := mustRunHistoryID(t)
	startedAt := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	reader := &recordingRunReader{
		getRunResult: runs.Run{ID: runID, Repository: "org/repo", Command: runs.CommandPlan,
			Trigger: runs.TriggerComment, Status: runs.StatusRunning, CreatedAt: startedAt, StartedAt: &startedAt},
		attemptPage: runs.AttemptPage{Attempts: []runs.RunAttempt{
			{
				ID: attemptID, RunID: runID, InstanceID: instanceID,
				ConcurrencyKey: "sha256:key", OwnershipClaimID: "claim-2",
				Status: runs.AttemptRunning, ClaimedAt: startedAt, StartedAt: &startedAt, HeartbeatAt: startedAt,
			},
			{
				ID: mustRunHistoryID(t), RunID: runID, InstanceID: instanceID,
				ConcurrencyKey: "sha256:key", OwnershipClaimID: "claim-1",
				Status: runs.AttemptInterrupted, ClaimedAt: startedAt, HeartbeatAt: startedAt,
			},
		}, NextCursor: "next-attempt"},
		instanceResult: runs.ExecutionInstance{
			ID: instanceID, ReplicaID: "atlantis-2", DeploymentID: "production",
			StartedAt: startedAt, HeartbeatAt: startedAt, Version: "1.2.3", Commit: "abc123",
		},
	}
	template := &recordingHistoryTemplate{}
	controller := testRunHistoryController(t, reader, template)
	controller.WebAuthentication = true
	request := mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/runs/"+string(runID)+"?attempt_cursor=current-attempt", nil), map[string]string{
		"run-id": string(runID),
	})
	request.SetBasicAuth("operator", "password")
	recorder := httptest.NewRecorder()

	controller.GetRun(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code)
	data, ok := template.data.(web_templates.RunHistoryDetailData)
	require.True(t, ok)
	require.Len(t, data.Attempts, 2)
	require.Equal(t, "atlantis-2", data.Attempts[0].ReplicaID)
	require.Equal(t, "production", data.Attempts[0].DeploymentID)
	require.Equal(t, "abc123", data.Attempts[0].Commit)
	require.Equal(t, 1, reader.instanceReads)
	require.Equal(t, "current-attempt", reader.attemptPageRequest.Cursor)
	require.Contains(t, data.AttemptNextPath, "attempt_cursor=next-attempt")
}

func TestRunHistoryControllerReconcilesUnknownAttemptWithAuditIdentity(t *testing.T) {
	runID := mustRunHistoryID(t)
	attemptID := mustRunHistoryID(t)
	reader := &recordingRunReader{attemptResult: runs.RunAttempt{
		ID: attemptID, RunID: runID, Status: runs.AttemptUnknown,
	}}
	controller := testRunHistoryController(t, reader, &recordingHistoryTemplate{})
	controller.WebAuthentication = true
	form := url.Values{
		"csrf_token": {controller.reconciliationToken(runID, attemptID)},
		"summary":    {"State inspected; fresh plan confirms no pending changes."},
	}
	request := mux.SetURLVars(httptest.NewRequest(http.MethodPost,
		"/runs/"+string(runID)+"/attempts/"+string(attemptID)+"/reconcile",
		strings.NewReader(form.Encode())), map[string]string{
		"run-id": string(runID), "attempt-id": string(attemptID),
	})
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.SetBasicAuth("operator", "password")
	recorder := httptest.NewRecorder()

	controller.ReconcileAttempt(recorder, request)

	require.Equal(t, http.StatusSeeOther, recorder.Code)
	require.Equal(t, attemptID, reader.reconciliation.ID)
	require.Equal(t, "operator", reader.reconciliation.Actor)
	require.Equal(t, form.Get("summary"), reader.reconciliation.Summary)
	_, err := runs.ParseID(string(reader.reconciliation.AuditEventID))
	require.NoError(t, err)
	require.Equal(t, "/atlantis/runs/"+string(runID)+"#attempts", recorder.Header().Get("Location"))
}

func TestRunHistoryControllerRejectsReconciliationWithoutCSRFToken(t *testing.T) {
	runID := mustRunHistoryID(t)
	attemptID := mustRunHistoryID(t)
	reader := &recordingRunReader{attemptResult: runs.RunAttempt{
		ID: attemptID, RunID: runID, Status: runs.AttemptUnknown,
	}}
	controller := testRunHistoryController(t, reader, &recordingHistoryTemplate{})
	controller.WebAuthentication = true
	request := mux.SetURLVars(httptest.NewRequest(http.MethodPost,
		"/runs/"+string(runID)+"/attempts/"+string(attemptID)+"/reconcile",
		strings.NewReader(url.Values{"summary": {"inspected"}}.Encode())), map[string]string{
		"run-id": string(runID), "attempt-id": string(attemptID),
	})
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.SetBasicAuth("operator", "password")
	recorder := httptest.NewRecorder()

	controller.ReconcileAttempt(recorder, request)

	require.Equal(t, http.StatusForbidden, recorder.Code)
	require.Empty(t, reader.reconciliation.ID)
}

func testRunHistoryController(t *testing.T, reader runHistoryStore, template web_templates.TemplateWriter) *RunHistoryController {
	t.Helper()
	return &RunHistoryController{
		AtlantisVersion: "test", AtlantisURL: &url.URL{Path: "/atlantis"},
		Logger: logging.NewNoopLogger(t), Store: reader,
		RunListTemplate: template, RunDetailTemplate: template,
		ProjectDetailTemplate: template, AuditListTemplate: template,
		WebUsername: "operator", WebPassword: "password",
	}
}

func mustRunHistoryID(t *testing.T) runs.ID {
	t.Helper()
	id, err := runs.NewID()
	require.NoError(t, err)
	return id
}

type recordingHistoryTemplate struct {
	data any
}

func (t *recordingHistoryTemplate) Execute(_ io.Writer, data any) error {
	t.data = data
	return nil
}

type recordingRunReader struct {
	failOnRead         bool
	readCalled         bool
	listRunsFilter     runs.RunFilter
	listRunsPage       runs.PageRequest
	listRunsResult     runs.RunPage
	getRunResult       runs.Run
	getProjectResult   runs.ProjectRun
	getOutputResult    runs.OutputPage
	outputAfter        int64
	outputLimit        int
	auditFilter        runs.AuditFilter
	attemptPage        runs.AttemptPage
	attemptPageRequest runs.PageRequest
	attemptResult      runs.RunAttempt
	instanceResult     runs.ExecutionInstance
	instanceReads      int
	reconciliation     runs.AttemptReconciliation
}

func (r *recordingRunReader) markRead() {
	r.readCalled = true
	if r.failOnRead {
		panic("history store must not be read")
	}
}

func (r *recordingRunReader) GetRun(context.Context, runs.ID) (runs.Run, error) {
	r.markRead()
	return r.getRunResult, nil
}

func (r *recordingRunReader) ListRuns(_ context.Context, filter runs.RunFilter, page runs.PageRequest) (runs.RunPage, error) {
	r.markRead()
	r.listRunsFilter = filter
	r.listRunsPage = page
	return r.listRunsResult, nil
}

func (r *recordingRunReader) GetProjectRun(context.Context, runs.ID) (runs.ProjectRun, error) {
	r.markRead()
	return r.getProjectResult, nil
}

func (r *recordingRunReader) ListProjectRuns(context.Context, runs.ID, runs.ProjectRunFilter, runs.PageRequest) (runs.ProjectRunPage, error) {
	r.markRead()
	return runs.ProjectRunPage{}, nil
}

func (r *recordingRunReader) SummarizeProjectRuns(context.Context, runs.ID) (runs.ProjectRunSummary, error) {
	r.markRead()
	return runs.ProjectRunSummary{}, nil
}

func (r *recordingRunReader) GetOutput(_ context.Context, _ runs.ID, after int64, limit int) (runs.OutputPage, error) {
	r.markRead()
	r.outputAfter = after
	r.outputLimit = limit
	return r.getOutputResult, nil
}

func (r *recordingRunReader) ListAuditEvents(_ context.Context, filter runs.AuditFilter, _ runs.PageRequest) (runs.AuditPage, error) {
	r.markRead()
	r.auditFilter = filter
	return runs.AuditPage{}, nil
}

func (r *recordingRunReader) GetInstance(context.Context, runs.ID) (runs.ExecutionInstance, error) {
	r.markRead()
	r.instanceReads++
	return r.instanceResult, nil
}

func (r *recordingRunReader) GetAttempt(context.Context, runs.ID) (runs.RunAttempt, error) {
	r.markRead()
	return r.attemptResult, nil
}

func (r *recordingRunReader) ListRunAttempts(_ context.Context, _ runs.ID, page runs.PageRequest) (runs.AttemptPage, error) {
	r.markRead()
	r.attemptPageRequest = page
	return r.attemptPage, nil
}

func (r *recordingRunReader) ReconcileAttempt(_ context.Context, reconciliation runs.AttemptReconciliation) error {
	r.reconciliation = reconciliation
	return nil
}

var _ runs.Reader = (*recordingRunReader)(nil)
var _ runHistoryStore = (*recordingRunReader)(nil)
