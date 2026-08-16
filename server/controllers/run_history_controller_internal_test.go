// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package controllers

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func testRunHistoryController(t *testing.T, reader runs.Reader, template web_templates.TemplateWriter) *RunHistoryController {
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
	failOnRead       bool
	readCalled       bool
	listRunsFilter   runs.RunFilter
	listRunsPage     runs.PageRequest
	listRunsResult   runs.RunPage
	getRunResult     runs.Run
	getProjectResult runs.ProjectRun
	getOutputResult  runs.OutputPage
	outputAfter      int64
	outputLimit      int
	auditFilter      runs.AuditFilter
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

var _ runs.Reader = (*recordingRunReader)(nil)
