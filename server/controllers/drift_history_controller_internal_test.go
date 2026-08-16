// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package controllers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/runatlantis/atlantis/server/controllers/web_templates"
	"github.com/runatlantis/atlantis/server/core/drift"
	"github.com/runatlantis/atlantis/server/events/models"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/require"
)

func TestDriftHistoryControllerRequiresExplicitWebAuthentication(t *testing.T) {
	reader := &recordingDriftHistoryReader{failOnRead: true}
	controller := testDriftHistoryController(t, reader, drift.NewInMemoryRemediationResultStore())
	recorder := httptest.NewRecorder()
	controller.List(recorder, httptest.NewRequest(http.MethodGet, "/drift", nil))
	require.Equal(t, http.StatusNotFound, recorder.Code)
	require.False(t, reader.readCalled)

	controller.WebAuthentication = true
	recorder = httptest.NewRecorder()
	controller.List(recorder, httptest.NewRequest(http.MethodGet, "/drift", nil))
	require.Equal(t, http.StatusUnauthorized, recorder.Code)
	require.False(t, reader.readCalled)
}

func TestDriftHistoryControllerListsStaleStatusAndRemediation(t *testing.T) {
	now := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	detectionID := mustRunHistoryID(t)
	remediationID := mustRunHistoryID(t)
	remediationAt := now.Add(-time.Hour)
	reader := &recordingDriftHistoryReader{
		current: drift.CurrentStatusPage{Projects: []drift.CurrentProjectStatus{{
			Repository: "github.com/org/repo",
			Project: models.ProjectDrift{
				ProjectName: "network", Path: "terraform/network", Workspace: "production",
				Ref: "main", ResolvedCommit: "deadbeef", DetectionID: string(detectionID),
				Drift: models.DriftSummary{HasDrift: true, ToAdd: 1}, LastChecked: now.Add(-25 * time.Hour),
			},
			Outcome: drift.DetectionOutcomeDrifted, LastRemediationID: string(remediationID),
			LastRemediationStatus: models.RemediationStatusSuccess, LastRemediationAt: &remediationAt,
		}}, HasMore: true},
		detections: drift.DetectionPage{Runs: []drift.DetectionRun{{
			ID: string(detectionID), RunID: string(detectionID), DisplayRepository: "org/repo",
			Ref: "main", Status: drift.DetectionStatusSucceeded, CompletedAt: now,
		}}, HasMore: true},
	}
	controller := testDriftHistoryController(t, reader, drift.NewInMemoryRemediationResultStore())
	controller.WebAuthentication = true
	controller.now = func() time.Time { return now }
	request := httptest.NewRequest(http.MethodGet, "/drift?repository=org%2Frepo&ref=main&outcome=drifted&project=network&offset=100&detection_offset=40", nil)
	request.SetBasicAuth("operator", "password")
	recorder := httptest.NewRecorder()

	controller.List(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, drift.CurrentStatusFilter{
		Repository: "org/repo", Ref: "main", Outcome: drift.DetectionOutcomeDrifted, Project: "network",
	}, reader.filter)
	require.Equal(t, drift.HistoryPageRequest{Limit: driftHistoryPageLimit, Offset: 100}, reader.page)
	require.Equal(t, drift.HistoryPageRequest{Limit: driftDetectionPageLimit, Offset: 40}, reader.detectionPage)
	data := controller.ListTemplate.(*recordingHistoryTemplate).data.(web_templates.DriftHistoryListData)
	require.True(t, data.Projects[0].Stale)
	require.Contains(t, data.Projects[0].DetectionPath, string(detectionID))
	require.Contains(t, data.Projects[0].LastRemediationPath, string(remediationID))
	require.Contains(t, data.NextPath, "offset=200")
	previousURL, err := url.Parse(data.PreviousPath)
	require.NoError(t, err)
	require.Empty(t, previousURL.Query().Get("offset"))
	require.Contains(t, data.DetectionNextPath, "detection_offset=60")
	require.Contains(t, data.DetectionPreviousPath, "detection_offset=20")
}

func TestDriftHistoryControllerRemediationOmitsRawOutput(t *testing.T) {
	id := mustRunHistoryID(t)
	store := drift.NewInMemoryRemediationResultStore()
	result := models.NewRemediationResult(string(id), "org/repo", "main", models.RemediationPlanOnly)
	result.RunID = string(id)
	result.Status = models.RemediationStatusSuccess
	result.Projects = []models.ProjectRemediationResult{{
		ProjectName: "network", Path: "terraform/network", Workspace: "default",
		Status: models.RemediationStatusSuccess, PlanOutput: "sensitive plan output",
	}}
	result.Complete()
	require.NoError(t, store.Put(result))
	controller := testDriftHistoryController(t, &recordingDriftHistoryReader{}, store)
	controller.WebAuthentication = true
	request := mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/drift/remediations/id", nil), map[string]string{
		"remediation-id": string(id),
	})
	request.SetBasicAuth("operator", "password")
	recorder := httptest.NewRecorder()

	controller.GetRemediation(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code)
	data := controller.RemediationTemplate.(*recordingHistoryTemplate).data.(web_templates.DriftRemediationDetailData)
	require.Len(t, data.Projects, 1)
	require.NotContains(t, data.Projects[0].Before, "sensitive")
	require.Equal(t, "/runs/"+string(id), data.RunPath)
}

func testDriftHistoryController(t *testing.T, reader drift.HistoryReader, remediationStore drift.RemediationResultStore) *DriftHistoryController {
	t.Helper()
	list := &recordingHistoryTemplate{}
	return &DriftHistoryController{
		AtlantisVersion: "test", AtlantisURL: &url.URL{Path: "/atlantis"},
		Logger: logging.NewNoopLogger(t), History: reader, Remediations: remediationStore,
		ListTemplate: list, DetectionTemplate: &recordingHistoryTemplate{},
		RemediationTemplate: &recordingHistoryTemplate{},
		WebUsername:         "operator", WebPassword: "password",
	}
}

type recordingDriftHistoryReader struct {
	failOnRead    bool
	readCalled    bool
	filter        drift.CurrentStatusFilter
	page          drift.HistoryPageRequest
	detectionPage drift.HistoryPageRequest
	current       drift.CurrentStatusPage
	detections    drift.DetectionPage
	detection     drift.DetectionRun
	projectPage   drift.DetectionPage
}

func (r *recordingDriftHistoryReader) markRead() {
	r.readCalled = true
	if r.failOnRead {
		panic("drift history must not be read")
	}
}

func (r *recordingDriftHistoryReader) ListCurrentStatus(_ context.Context, filter drift.CurrentStatusFilter, page drift.HistoryPageRequest) (drift.CurrentStatusPage, error) {
	r.markRead()
	r.filter = filter
	r.page = page
	return r.current, nil
}

func (r *recordingDriftHistoryReader) ListDetections(_ context.Context, _ string, page drift.HistoryPageRequest) (drift.DetectionPage, error) {
	r.markRead()
	r.detectionPage = page
	return r.detections, nil
}

func (r *recordingDriftHistoryReader) GetDetection(context.Context, string, drift.HistoryPageRequest) (drift.DetectionRun, drift.DetectionPage, error) {
	r.markRead()
	return r.detection, r.projectPage, nil
}

var _ drift.HistoryReader = (*recordingDriftHistoryReader)(nil)
